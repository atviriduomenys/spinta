from typing import Dict, Any
from typing import Iterable
from typing import List

import sqlalchemy as sa
import tqdm

from spinta import commands
from spinta.cli.helpers.push.components import PushRow
from spinta.cli.helpers.push.utils import extract_state_page_keys, construct_where_condition_from_page
from spinta.cli.helpers.push.write import prepare_rows_for_deletion
from spinta.commands.read import PaginationMetaData, get_paginated_values
from spinta.components import Context, pagination_enabled
from spinta.components import Model
from spinta.components import get_page_size


def get_deleted_rows(
    models: List[Model],
    context: Context,
    metadata: sa.MetaData,
    no_progress_bar: bool = False,
):
    counts = (
        _get_deleted_row_counts(
            models,
            context,
            metadata
        )
    )
    total_count = sum(counts.values())
    if total_count > 0:
        rows = _iter_deleted_rows(
            models,
            context,
            metadata,
            counts,
            no_progress_bar,
        )
        if not no_progress_bar:
            rows = tqdm.tqdm(rows, 'PUSH DELETED', ascii=True, total=total_count)
        for row in rows:
            yield row


def _get_deleted_row_counts(
    models: List[Model],
    context: Context,
    metadata: sa.MetaData,
) -> dict:
    counts = {}
    conn = context.get('push.state.conn')
    for model in models:
        table = metadata.tables[model.name]

        row_count = conn.execute(
            sa.select(sa.func.count(table.c.id)).
            where(
                sa.and_(
                    table.c.pushed.is_(None),
                    table.c.error.is_(False)
                )
            )
        )
        counts[model.name] = row_count.scalar()
    return counts


def _iter_deleted_rows(
    models: List[Model],
    context: Context,
    metadata: sa.MetaData,
    counts: Dict[str, int],
    no_progress_bar: bool = False,
) -> Iterable[PushRow]:
    models = reversed(models)
    config = context.get('config')
    conn = context.get('push.state.conn')
    for model in models:
        size = get_page_size(config, model)
        table = metadata.tables[model.name]
        total = counts.get(model.name)

        if pagination_enabled(model):
            rows = _get_deleted_rows_with_page(
                context,
                model,
                table,
                size,
            )
        else:
            rows = conn.execute(
                sa.select([table.c.id]).
                where(
                    table.c.pushed.is_(None) &
                    table.c.error.is_(False)
                )
            )
        if not no_progress_bar:
            rows = tqdm.tqdm(
                rows,
                model.name,
                ascii=True,
                total=total,
                leave=False
            )

        for row in rows:
            yield prepare_rows_for_deletion(model, row[table.c.id])


def _get_deleted_rows_with_page(
    context: Context,
    model: Model,
    table: sa.Table,
    size: int,
) -> sa.engine.LegacyCursorResult:
    conn = context.get('push.state.conn')

    order_by = []
    page = commands.create_page(model.page)
    page.size += 1

    for page_by in page.by.values():
        order_by.append(sa.asc(table.c[f"page.{page_by.prop.name}"]))

    required_where_cond = sa.and_(
        table.c.pushed.is_(None),
        table.c.error.is_(False)
    )

    page_meta = PaginationMetaData(
        page_size=size
    )

    while not page_meta.is_finished:
        page_meta.is_finished = True

        cond = construct_where_condition_from_page(page, table)
        where_cond = None

        if cond is not None:
            where_cond = cond

        if where_cond is not None:
            where_cond = sa.and_(
                required_where_cond,
                where_cond
            )
        else:
            where_cond = required_where_cond

        rows = conn.execute(
            sa.select([table]).
            where(
                where_cond
            ).order_by(*order_by).limit(size)
        )

        yield from get_paginated_values(page, page_meta, rows, extract_state_page_keys)





