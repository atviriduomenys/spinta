import asyncio
import dataclasses
import datetime
import pathlib
import sys
from collections.abc import Generator
from typing import List, Iterator, AsyncIterator

import tqdm
from multipledispatch import dispatch
from sqlalchemy.engine import Engine
from typer import echo

from spinta import commands
from spinta.backends import Backend
from spinta.backends.constants import TableType
from spinta.backends.helpers import validate_and_return_transaction
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.migrate import get_prop_names
from spinta.backends.postgresql.helpers.name import get_pg_table_name, get_pg_column_name
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.script.helpers import ensure_store_is_loaded
from spinta.commands.write import dataitem_from_payload, prepare_data, prepare_patch, prepare_data_for_write
from spinta.components import Context, Model, DataItem
from spinta.core.enums import Action


@dataclasses.dataclass
class _DuplicateChangelogData:
    id_: str
    last_action: Action
    last_revision: str
    # Before the delete action, the last datetime
    last_datetime: datetime.datetime


@dataclasses.dataclass
class _DuplicateChangelogMoveMapping:
    primary: _DuplicateChangelogData
    targets: tuple[_DuplicateChangelogData]


class _DuplicateChangelogEntry:
    affected_data: list[_DuplicateChangelogData]

    def __init__(self):
        self.affected_data = []

    def add_data(self, data: _DuplicateChangelogData):
        self.affected_data.append(data)

    def to_move_mapping(self) -> _DuplicateChangelogMoveMapping:
        sorted_data = sorted(self.affected_data, key=lambda x: x.last_datetime, reverse=True)
        primary = sorted_data[0]
        targets = tuple(sorted_data[1:])
        return _DuplicateChangelogMoveMapping(primary, targets)


class _DuplicateChangelogModel:
    model: Model
    duplicate_entries: Iterator[_DuplicateChangelogEntry]

    def __init__(self, model: Model, duplicate_entries: Iterator[_DuplicateChangelogEntry]):
        self.model = model
        self.duplicate_entries = duplicate_entries

    def get_move_mappings(self) -> Iterator[_DuplicateChangelogMoveMapping]:
        for entry in self.duplicate_entries:
            yield entry.to_move_mapping()


def check_if_corrupted_changelog_exists(engine: Engine, table_name: str, data_keys: List[str]) -> bool:
    keys_list = ", ".join(f"('{key}')" for key in data_keys)

    contains_deleted_stmt = f"""
    SELECT EXISTS (
        SELECT 1
        FROM "{table_name}"
        WHERE action = 'delete'
    )
    """

    filter_stmt = f"""
        WITH kv AS (
          -- 1) Latest value per (_rid, key)
          SELECT DISTINCT ON (c._rid, t.key)
            c._rid,
            t.key,
            c.data::jsonb ->> t.key AS value
          FROM "{table_name}" AS c
          CROSS JOIN LATERAL (VALUES {keys_list}) AS t(key)
          WHERE c.action NOT IN ('delete','move')
            AND c.data::jsonb ? t.key
          ORDER BY c._rid, t.key, c.datetime DESC
        ),
        entity_data AS (
          -- 2) Re-aggregate back into JSON per _rid
          SELECT
            _rid,
            jsonb_object_agg(key, value) AS final_data
          FROM kv
          GROUP BY _rid
        ),
        last_data AS (
          -- 3) Grab each _rid’s very last action
          SELECT DISTINCT ON (_rid)
            _rid,
            action AS last_action
          FROM "{table_name}"
          ORDER BY _rid, datetime DESC
        ),
        filtered AS (
          -- 4) Keep only those whose last action is NOT 'move'
          SELECT e._rid, e.final_data
          FROM entity_data e
          JOIN last_data l USING (_rid)
          WHERE l.last_action != 'move'
        )
        -- 5) Existence check for any JSON blob appearing more than once
        SELECT EXISTS (
          SELECT 1
          FROM filtered
          GROUP BY final_data
          HAVING COUNT(*) > 1
        ) AS duplicates_exist;
    """

    with engine.connect() as conn:
        # Early return all tables that do not have any deleted entities
        contains_deleted = conn.execute(contains_deleted_stmt).scalar()
        if not contains_deleted:
            return False

        result = conn.execute(filter_stmt).scalar()
        return result


def fetch_corrupted_changelog_entities(
    engine: Engine, table_name: str, data_keys: List[str]
) -> Generator[_DuplicateChangelogEntry]:
    # Safely quote the data_keys for the SQL IN clause
    keys_list = ", ".join(f"('{key}')" for key in data_keys)

    sql = f"""
        WITH
        -- 1) Latest value per (_rid, key)
        kv as (
            SELECT DISTINCT ON (c._rid, t.key)
                c._rid,
                c.datetime,
                t.key,
                c.data::jsonb ->> t.key AS value
            FROM "{table_name}" AS c
            CROSS JOIN LATERAL (VALUES {keys_list}) AS t(key)
            WHERE c.action NOT IN ('delete', 'move')
              -- emit only the changelog rows that actually mention each key:
              AND c.data::jsonb ? t.key
            ORDER BY c._rid, t.key, c.datetime DESC
        ),
    
        -- 2) Re-aggregate those key/value pairs back into one JSON per _rid, also calculate latest datetime (that was not deleted or moved)
        entity_data AS (
            SELECT
                _rid,
                jsonb_object_agg(key, value) AS final_data,
                MAX(datetime) AS last_non_delete_datetime
            FROM kv
            GROUP BY _rid
        ),
    
        -- 3) Find each _rid’s very last action
        last_data AS (
            SELECT DISTINCT ON (_rid)
                _rid,
                action AS last_action,
                _revision as last_revision
            FROM "{table_name}"
            ORDER BY _rid, datetime DESC
        ),
    
        -- 4) Base set of all deleted entities with their reconstructed state
        base AS (
            SELECT
                e._rid,
                e.final_data,
                ld.last_action,
                ld.last_revision,
                e.last_non_delete_datetime
            FROM entity_data e
            JOIN last_data ld USING (_rid)
            WHERE ld.last_action != 'move'
        ),
        frequent_final_data AS (
            SELECT final_data
            FROM base
            GROUP BY final_data
            HAVING COUNT(*) > 1
        )
    
    -- 5) Only return those JSON states that occur >= 2×
    SELECT
        _rid,
        final_data,
        last_action,
        last_revision,
        last_non_delete_datetime
    FROM base b
    JOIN frequent_final_data fd USING (final_data)
    ORDER BY final_data, last_non_delete_datetime;
    """

    contains_deleted_stmt = f"""
    SELECT EXISTS (
        SELECT 1
        FROM "{table_name}"
        WHERE action = 'delete'
    )
    """

    # Execute the query and fetch results
    with engine.connect() as conn:
        # Early return all tables that do not have any deleted entities
        contains_deleted = conn.execute(contains_deleted_stmt).scalar()
        if not contains_deleted:
            return

        result = conn.execute(sql)
        current_key = None
        current_entry = None
        for row in result:
            key = row["final_data"]
            if key != current_key:
                if current_entry:
                    yield current_entry
                current_key = key
                current_entry = _DuplicateChangelogEntry()

            current_entry.add_data(
                _DuplicateChangelogData(
                    id_=str(row["_rid"]),
                    last_action=row["last_action"],
                    last_revision=row["last_revision"],
                    last_datetime=row["last_non_delete_datetime"],
                )
            )

    if current_entry and current_entry.affected_data:
        yield current_entry


@dispatch(Context, Backend, Model)
def _gather_corrupted_changelog_entities(
    context: Context, backend: Backend, model: Model
) -> Generator[_DuplicateChangelogEntry]:
    raise Exception("Not implemented")


@dispatch(Context, PostgreSQL, Model)
def _gather_corrupted_changelog_entities(
    context: Context, backend: PostgreSQL, model: Model
) -> Generator[_DuplicateChangelogEntry]:
    pkey_columns = list()
    for pkey in model.external.pkeys:
        for name in get_prop_names(pkey):
            pkey_columns.append(get_pg_column_name(name))

    yield from fetch_corrupted_changelog_entities(
        backend.engine, get_pg_table_name(model, TableType.CHANGELOG), pkey_columns
    )


def _gather_corrupted_changelog_model(context: Context, model: Model) -> _DuplicateChangelogModel:
    entities = _gather_corrupted_changelog_entities(context, model.backend, model)
    return _DuplicateChangelogModel(model, entities)


@dispatch(Context, Model)
def _changelog_contains_corrupted_data(context: Context, model: Model) -> bool:
    backend = model.backend
    return _changelog_contains_corrupted_data(context, backend, model)


@dispatch(Context, Backend, Model)
def _changelog_contains_corrupted_data(context: Context, backend: Backend, model: Model) -> bool:
    return False


@dispatch(Context, PostgreSQL, Model)
def _changelog_contains_corrupted_data(context: Context, backend: PostgreSQL, model: Model) -> bool:
    pkey_columns = list()
    for pkey in model.external.pkeys:
        for name in get_prop_names(pkey):
            pkey_columns.append(get_pg_column_name(name))

    table_name = get_pg_table_name(model, TableType.CHANGELOG)
    if check_if_corrupted_changelog_exists(backend.engine, table_name, pkey_columns):
        return True
    return False


async def _duplicate_mapping_to_dataitem(
    context: Context, model: Model, mapping: _DuplicateChangelogMoveMapping
) -> AsyncIterator[DataItem]:
    for target in mapping.targets:
        payload = {
            "_where": f'eq(_id, "{target.id_}")',
            "_op": Action.MOVE.value,
            "_revision": target.last_revision,
            "_id": mapping.primary.id_,
        }
        item = dataitem_from_payload(context, model, payload)
        yield item


def models_with_pkey(context: Context, whitelist: list[str]) -> Generator[Model]:
    store = ensure_store_is_loaded(context)

    for model_name in whitelist:
        model = commands.get_model(context, store.manifest, model_name)
        if model.model_type().startswith("_") or model.external.unknown_primary_key:
            continue

        yield model


def cli_requires_changelog_migrations(
    context: Context,
    input_path: pathlib.Path = None,
    **kwargs,
) -> bool:
    models_list = parse_input_path(context, input_path)

    models = list(models_with_pkey(context, models_list))
    for model in models:
        if _changelog_contains_corrupted_data(context, model):
            echo(f'Corrupted changelog entry found for "{model.model_type()}"')
            return True

    return False


async def _saved_data(context: Context, dstream: AsyncIterator[DataItem]):
    async for data in dstream:
        data.saved = {"_id": data.given["_where"]["args"][1], "_revision": data.given["_revision"]}
        yield data


async def _migrate_duplicates(
    context: Context,
    dmodel: _DuplicateChangelogModel,
):
    model = dmodel.model
    for mapping in dmodel.get_move_mappings():
        stream = _duplicate_mapping_to_dataitem(context, model, mapping)
        commands.authorize(context, Action.MOVE, model)
        dstream = prepare_data(context, stream, False)
        dstream = _saved_data(context, dstream)
        dstream = prepare_patch(context, dstream)
        dstream = prepare_data_for_write(context, dstream, None)
        dstream = commands.create_redirect_entry(context, model, model.backend, dstream=dstream)
        dstream = commands.create_changelog_entry(
            context,
            model,
            model.backend,
            dstream=dstream,
        )
        async for _ in dstream:
            pass


def migrate_changelog_duplicates(
    context: Context,
    input_path: pathlib.Path = None,
    **kwargs,
):
    models_list = parse_input_path(context, input_path)

    models = list(models_with_pkey(context, models_list))
    counter = tqdm.tqdm(desc="MIGRATING CHANGELOGS WITH DUPLICATES", ascii=True, total=len(models))
    store = context.get("store")
    try:
        with context:
            require_auth(context)
            context.attach("transaction", validate_and_return_transaction, context, store.manifest.backend, write=True)

            for model in models:
                dup_model = _gather_corrupted_changelog_model(context, model)
                asyncio.run(_migrate_duplicates(context, dup_model))
                counter.update(1)
    finally:
        counter.close()


def parse_input_path(
    context: Context,
    input_path: pathlib.Path = None,
    **kwargs,
):
    if input_path is None:
        # Reads stdin direct for data
        if not sys.stdin.isatty():
            data = sys.stdin.read()
            data = data.splitlines()
            return data

        echo("Script requires model list file path (can also add it through `--input <file_path>` argument).", err=True)
        input_path = input("Enter model list file path: ")

    if not input_path.exists():
        echo(f'File "{input_path}" does not exist.', err=True)
        sys.exit(1)

    with input_path.open("r") as f:
        return f.read().splitlines()
