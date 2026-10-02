import pathlib
from dataclasses import dataclass
from datetime import datetime
from time import perf_counter
from typing import Any, Callable, Iterator, Sequence

import sqlalchemy as sa
from multipledispatch import dispatch
from sqlalchemy.engine import Connection

from spinta import commands
from spinta.backends import Backend
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.cli.helpers.message import cli_message
from spinta.cli.helpers.script.helpers import ensure_store_is_loaded
from spinta.components import Context, Model
from spinta.core.enums import Action

UPDATE_OPS = (Action.PATCH.value, Action.UPDATE.value, Action.UPSERT.value)
TERMINAL_OPS = (Action.DELETE.value, Action.MOVE.value)
ANCHOR_OPS = (Action.INSERT.value, Action.UPSERT.value)

COMPRESSION_BATCH_SIZE = 10000


@dataclass
class CompressionRow:
    cid: int
    op: str
    data: dict[str, Any]
    compressed: bool


@dataclass
class CompressionGroup:
    id: Any
    lifecycle: int
    target_cid: int
    rows: list[CompressionRow]


MergeChanges = Callable[
    [Sequence[CompressionRow]],
    tuple[str, dict[str, Any]],
]


def check_if_need_to_compress_changelogs(context: Context, **kwargs) -> bool:
    ensure_store_is_loaded(context)
    store = context.get("store")
    manifest = store.manifest
    for model in commands.get_models(context, manifest).values():
        if _requires_changelog_compression(model, model.backend):
            return True
    return False


def compress_changelogs(context: Context, output_path: pathlib.Path | None = None, **kwargs):
    ensure_store_is_loaded(context)
    store = context.get("store")
    manifest = store.manifest

    for model in commands.get_models(context, manifest).values():
        if not _requires_changelog_compression(model, model.backend):
            continue

        _compress_model_changelog(model, model.backend)


@dataclass
class CompressionStats:
    lifecycles: int = 0
    rows_read: int = 0
    rows_deleted: int = 0
    max_rows_per_lifecycle: int = 0

    fetch_seconds: float = 0.0
    merge_seconds: float = 0.0
    update_seconds: float = 0.0
    delete_seconds: float = 0.0


@dispatch(Model, PostgreSQL)
def _compress_model_changelog(model: Model, backend: PostgreSQL):
    with backend.begin() as conn:
        # Remove unneeded data
        # TODO this should come from model
        cutoff = datetime(year=2024, month=5, day=17)
        compressed_at = datetime(year=2035, month=1, day=1, hour=12)
        changelog_table = backend.get_table(model, TableType.CHANGELOG)

        total_start = perf_counter()
        start = perf_counter()
        removed_count = prepare_changelog_for_compression(conn, changelog_table, cutoff=cutoff)
        preprocessing_time = perf_counter() - start
        cli_message(model.model_type())
        cli_message(f"PREPROCESSING: Removed {removed_count} rows from changelog. {preprocessing_time}s")

        stats = compress_lifecycles(
            conn,
            changelog_table,
            cutoff=cutoff,
            compressed_at=compressed_at,
        )
        cli_message(f"COMPRESSED: Compressed {stats.lifecycles} lifecycles in changelog.")
        cli_message(
            f"\tRead rows:{stats.rows_read}\n"
            f"\tDeleted rows:{stats.rows_deleted}\n"
            f"\tMax rows per lifecycle:{stats.max_rows_per_lifecycle}\n"
            f"\tUpdate time:{stats.update_seconds}s\n"
            f"\tMerge time:{stats.merge_seconds}s\n"
            f"\tDelete time:{stats.delete_seconds}s\n"
            f"\tFetch time:{stats.fetch_seconds}s\n"
        )
        total_time = perf_counter() - total_start
        cli_message(f"TOTAL: Total time: {total_time}s")


def prepare_changelog_for_compression(
    conn: Connection,
    table: sa.Table,
    *,
    cutoff: datetime,
) -> int:
    return delete_expired_lifecycles(
        conn,
        table,
        cutoff=cutoff,
    )


def delete_expired_lifecycles(
    conn: Connection,
    table: sa.Table,
    *,
    cutoff: datetime,
) -> int:
    """
    Delete complete lifecycles that:

      1. are not the latest lifecycle for their _rid;
      2. have no rows inside the retention period.

    The latest lifecycle is always preserved, regardless of age.
    """
    rows = lifecycle_rows(
        table,
        name="expired_lifecycle_rows",
    )

    protected_lifecycle = sa.func.coalesce(
        sa.func.min(rows.c.lifecycle)
        .filter(rows.c.datetime > cutoff)
        .over(
            partition_by=rows.c._rid,
        ),
        sa.func.max(rows.c.lifecycle).over(
            partition_by=rows.c._rid,
        ),
    ).label("protected_lifecycle")

    retention_rows = (
        sa.select(
            rows.c._id,
            rows.c._rid,
            rows.c.lifecycle,
            protected_lifecycle,
        )
        .where(rows.c.lifecycle > 0)
        .cte("retention_rows")
    )

    removable = sa.select(retention_rows.c._id).where(retention_rows.c.lifecycle < retention_rows.c.protected_lifecycle)

    stmt = sa.delete(table).where(table.c._id.in_(removable))

    result = conn.execute(stmt)
    return result.rowcount


def compress_lifecycles(
    conn: Connection,
    table: sa.Table,
    *,
    cutoff: datetime,
    compressed_at: datetime | None = None,
    batch_size: int = COMPRESSION_BATCH_SIZE,
) -> CompressionStats:
    stats = CompressionStats()

    if compressed_at is None:
        compressed_at = datetime.now()

    batch: list[CompressionGroup] = []

    for group in iter_compression_groups(
        conn,
        table,
        cutoff=cutoff,
        stats=stats,
    ):
        batch.append(group)

        if len(batch) >= batch_size:
            compress_groups(
                conn,
                table,
                batch,
                compressed_at=compressed_at,
                stats=stats,
            )
            batch.clear()

    if batch:
        compress_groups(
            conn,
            table,
            batch,
            compressed_at=compressed_at,
            stats=stats,
        )

    return stats


def compress_groups(
    conn: Connection,
    table: sa.Table,
    groups: list[CompressionGroup],
    *,
    compressed_at: datetime,
    stats: CompressionStats | None = None,
) -> None:
    mark_updates = []
    data_updates = []
    delete_cids = []

    for group in groups:
        rows = group.rows

        if not rows:
            continue

        target = rows[-1]

        if target.cid != group.target_cid:
            raise RuntimeError(f"Expected target {group.target_cid}, got {target.cid}.")

        if stats is not None:
            stats.rows_read += len(rows)
            stats.max_rows_per_lifecycle = max(
                stats.max_rows_per_lifecycle,
                len(rows),
            )
            stats.lifecycles += 1

        if len(rows) == 1:
            mark_updates.append(
                {
                    "cid": target.cid,
                    "compressed_at": compressed_at,
                }
            )
            continue

        start = perf_counter()
        result_data = merge_compression_rows(rows)

        if stats is not None:
            stats.merge_seconds += perf_counter() - start

        data_updates.append(
            {
                "cid": target.cid,
                "data": result_data,
                "compressed_at": compressed_at,
            }
        )

        delete_cids.extend(row.cid for row in rows[:-1])

    # Single-row lifecycle:
    # only mark target as compressed.
    if mark_updates:
        stmt = (
            sa.update(table)
            .where(table.c._id == sa.bindparam("cid"))
            .values(
                _compressed=sa.bindparam("compressed_at"),
            )
        )

        start = perf_counter()

        conn.execute(
            stmt,
            mark_updates,
        )

        if stats is not None:
            stats.update_seconds += perf_counter() - start

    # Multi-row lifecycle:
    # replace target data + mark compressed.
    if data_updates:
        stmt = (
            sa.update(table)
            .where(table.c._id == sa.bindparam("cid"))
            .values(
                data=sa.bindparam("data"),
                _compressed=sa.bindparam("compressed_at"),
            )
        )

        start = perf_counter()

        conn.execute(
            stmt,
            data_updates,
        )

        if stats is not None:
            stats.update_seconds += perf_counter() - start

    # All obsolete rows from this batch can be deleted together.
    if delete_cids:
        stmt = sa.delete(table).where(table.c._id.in_(delete_cids))

        start = perf_counter()

        conn.execute(stmt)

        if stats is not None:
            stats.delete_seconds += perf_counter() - start
            stats.rows_deleted += len(delete_cids)


def merge_compression_rows(
    rows: list[CompressionRow],
) -> dict[str, Any]:
    if not rows:
        raise ValueError("Cannot compress an empty group.")

    data: dict[str, Any] = {}
    for row in rows:
        if row.op == Action.UPDATE.value:
            data = dict(row.data)
        elif row.op in (Action.PATCH.value, Action.UPSERT.value):
            data.update(row.data)
        else:
            raise ValueError(f"Unexpected compression operation: {row.op!r}")

    return data


def iter_compression_groups(
    conn: Connection,
    table: sa.Table,
    *,
    cutoff: datetime,
    stats: CompressionStats | None = None,
) -> Iterator[CompressionGroup]:
    stmt = compression_query(
        table,
        cutoff=cutoff,
    )

    start = perf_counter()

    result = conn.execution_options(
        stream_results=True,
        yield_per=5000,
    ).execute(stmt)

    if stats is not None:
        stats.fetch_seconds += perf_counter() - start

    current_key = None
    current_rows = []
    target_cid = None

    start = perf_counter()
    for row in result:
        key = (row._rid, row.lifecycle)

        if current_key is not None and key != current_key:
            if stats is not None:
                stats.max_rows_per_lifecycle = max(
                    stats.max_rows_per_lifecycle,
                    len(current_rows),
                )

            yield CompressionGroup(
                id=current_key[0],
                lifecycle=current_key[1],
                target_cid=target_cid,
                rows=current_rows,
            )

            current_rows = []

        current_key = key
        target_cid = row.target_cid

        current_rows.append(
            CompressionRow(
                cid=row._id,
                op=row.action,
                data=row.data,
                compressed=row._compressed is not None,
            )
        )

    if current_key is not None:
        if stats is not None:
            stats.max_rows_per_lifecycle = max(
                stats.max_rows_per_lifecycle,
                len(current_rows),
            )

        yield CompressionGroup(
            id=current_key[0],
            lifecycle=current_key[1],
            target_cid=target_cid,
            rows=current_rows,
        )
    if stats is not None:
        stats.fetch_seconds += perf_counter() - start


def compression_query(
    table: sa.Table,
    *,
    cutoff: datetime,
):
    rows = lifecycle_rows(
        table,
        name="compression_lifecycle_rows",
    )

    target_cid = (
        sa.func.max(rows.c._id)
        .filter(
            sa.and_(
                rows.c.lifecycle > 0,
                rows.c.is_anchor == 0,
                rows.c.action.in_(UPDATE_OPS),
                rows.c.datetime <= cutoff,
            )
        )
        .over(
            partition_by=(
                rows.c._rid,
                rows.c.lifecycle,
            )
        )
        .label("target_cid")
    )

    compression_rows = sa.select(
        rows.c._id,
        rows.c._rid,
        rows.c.lifecycle,
        rows.c.is_anchor,
        rows.c.action,
        target_cid,
    ).cte("compression_rows")

    target_row = table.alias("compression_target_row")
    data_row = table.alias("compression_data_row")

    return (
        sa.select(
            compression_rows.c._id,
            compression_rows.c.lifecycle,
            compression_rows.c.target_cid,
            compression_rows.c._rid,
            compression_rows.c.action,
            data_row.c.data,
            data_row.c._compressed,
        )
        .join(
            target_row,
            target_row.c._id == compression_rows.c.target_cid,
        )
        .join(
            data_row,
            data_row.c._id == compression_rows.c._id,
        )
        .where(
            compression_rows.c.target_cid.is_not(None),
            target_row.c._compressed.is_(None),
            compression_rows.c.is_anchor == 0,
            compression_rows.c.action.in_(UPDATE_OPS),
            compression_rows.c._id <= compression_rows.c.target_cid,
        )
        .order_by(
            compression_rows.c._rid,
            compression_rows.c.lifecycle,
            compression_rows.c._id,
        )
    )


def lifecycle_rows(
    table: sa.Table,
    *,
    name: str = "lifecycle_rows",
):
    window = {
        "partition_by": table.c._rid,
        "order_by": table.c._id,
        "rows": (None, -1),
    }

    previous_terminal_cid = (
        sa.func.max(
            sa.case(
                (table.c.action.in_(TERMINAL_OPS), table.c._id),
                else_=None,
            )
        )
        .over(**window)
        .label("previous_terminal_cid")
    )

    previous_creator_cid = (
        sa.func.max(
            sa.case(
                (table.c.action.in_(ANCHOR_OPS), table.c._id),
                else_=None,
            )
        )
        .over(**window)
        .label("previous_creator_cid")
    )

    state = sa.select(
        table.c._id,
        table.c._rid,
        table.c.action,
        table.c.datetime,
        previous_terminal_cid,
        previous_creator_cid,
    ).cte(f"{name}_state")

    # insert -> always an anchor
    #
    # upsert -> anchor when:
    #   * there has never been a creator before it, OR
    #   * the most recent terminal is newer than the most recent creator
    is_anchor = sa.case(
        (
            state.c.action == Action.INSERT.value,
            1,
        ),
        (
            sa.and_(
                state.c.action == Action.UPSERT.value,
                sa.or_(
                    state.c.previous_creator_cid.is_(None),
                    sa.and_(
                        state.c.previous_terminal_cid.is_not(None),
                        state.c.previous_terminal_cid > state.c.previous_creator_cid,
                    ),
                ),
            ),
            1,
        ),
        else_=0,
    ).label("is_anchor")

    anchors = sa.select(
        state,
        is_anchor,
    ).cte(f"{name}_anchors")

    lifecycle = (
        sa.func.sum(anchors.c.is_anchor)
        .over(
            partition_by=anchors.c._rid,
            order_by=anchors.c._id,
            rows=(None, 0),
        )
        .label("lifecycle")
    )

    return sa.select(
        anchors,
        lifecycle,
    ).cte(name)


@dispatch(Model, Backend)
def _compress_model_changelog(model: Model, backend: Backend):
    raise NotImplementedError()


@dispatch(Model, PostgreSQL)
def _requires_changelog_compression(model: Model, backend: PostgreSQL) -> bool:
    if model.name.startswith("_"):
        return False

    return True


@dispatch(Model, Backend)
def _requires_changelog_compression(model: Model, backend: Backend) -> bool:
    return False


def explain_analyze(
    conn: Connection,
    stmt,
) -> None:
    compiled = stmt.compile(
        dialect=conn.dialect,
        compile_kwargs={
            "render_postcompile": True,
        },
    )

    sql = "EXPLAIN (ANALYZE, BUFFERS) " + str(compiled)

    result = conn.exec_driver_sql(
        sql,
        compiled.params,
    )

    for row in result:
        print(row[0])
