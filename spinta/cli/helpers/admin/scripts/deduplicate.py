import asyncio
import dataclasses
import datetime
from collections.abc import Iterator
from typing import AsyncIterator

import sqlalchemy as sa
import tqdm
from multipledispatch import dispatch
from typer import echo

from spinta import commands
from spinta.backends import Backend
from spinta.backends.helpers import validate_and_return_transaction
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.migrate import extract_sqlalchemy_columns, get_prop_names
from spinta.backends.postgresql.helpers.name import get_pg_table_name, get_pg_column_name, get_pg_constraint_name
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.script.helpers import ensure_store_is_loaded
from spinta.commands.write import dataitem_from_payload, move_stream
from spinta.components import Context, Model, DataItem
from spinta.core.enums import Action
from spinta.utils.itertools import ensure_list


@dataclasses.dataclass
class _DuplicateData:
    id_: str
    rev_: str
    created: datetime.datetime


@dataclasses.dataclass
class _DuplicateMoveMapping:
    primary: _DuplicateData
    targets: tuple[_DuplicateData]


class _DuplicateEntry:
    affected_data: list[_DuplicateData]

    def __init__(self):
        self.affected_data = []

    def add_data(self, data: _DuplicateData):
        self.affected_data.append(data)

    def to_move_mapping(self) -> _DuplicateMoveMapping:
        sorted_data = sorted(self.affected_data, key=lambda x: x.created, reverse=True)
        primary = sorted_data[0]
        targets = tuple(sorted_data[1:])
        return _DuplicateMoveMapping(primary, targets)


class _DuplicateModel:
    model: Model
    missing_unique: bool
    duplicate_entries: dict[tuple, _DuplicateEntry]

    def __init__(self, model: Model, missing_unique: bool = False):
        self.model = model
        self.missing_unique = missing_unique
        self.duplicate_entries = {}

    def get_entry(self, pkey: tuple) -> _DuplicateEntry:
        if pkey in self.duplicate_entries:
            return self.duplicate_entries[pkey]

        new_entry = _DuplicateEntry()
        self.duplicate_entries[pkey] = new_entry
        return new_entry

    def get_move_mappings(self) -> Iterator[_DuplicateMoveMapping]:
        for entry in self.duplicate_entries.values():
            yield entry.to_move_mapping()


@dispatch(Context, Model)
def _fulfills_unique_constraint(context: Context, model: Model) -> bool:
    return _fulfills_unique_constraint(context, model.backend, model)


@dispatch(Context, Backend, Model)
def _fulfills_unique_constraint(context: Context, backend: Backend, model: Model) -> bool:
    # Skips unique constraint checks
    return True


@dispatch(Context, PostgreSQL, Model)
def _fulfills_unique_constraint(context: Context, backend: PostgreSQL, model: Model) -> bool:
    table_name = get_pg_table_name(model)
    insp = sa.inspect(backend.engine)
    constraints = insp.get_unique_constraints(table_name)
    pkey_columns = list()
    for pkey in model.external.pkeys:
        for name in get_prop_names(pkey):
            pkey_columns.append(get_pg_column_name(name))

    return any(True for constraint in constraints if set(constraint["column_names"]) == set(pkey_columns))


@dispatch(Context, _DuplicateModel)
def _create_duplicate_entries(context: Context, dmodel: _DuplicateModel):
    _create_duplicate_entries(context, dmodel.model.backend, dmodel)


@dispatch(Context, Backend, _DuplicateModel)
def _create_duplicate_entries(context: Context, backend: Backend, dmodel: _DuplicateModel):
    return


@dispatch(Context, PostgreSQL, _DuplicateModel)
def _create_duplicate_entries(context: Context, backend: PostgreSQL, dmodel: _DuplicateModel):
    transaction = context.get("transaction")
    connection = transaction.connection

    table = backend.get_table(dmodel.model)
    pkey_columns = list()
    pkey_column_names = list()
    for pkey in dmodel.model.external.pkeys:
        for name in get_prop_names(pkey):
            pkey_column_names.append(get_pg_column_name(name))

        columns = commands.prepare(context, backend, pkey)
        columns = ensure_list(columns)
        columns = extract_sqlalchemy_columns(columns)
        columns = [_ensure_table_set(col, table) for col in columns]
        pkey_columns.extend(columns)

    subquery = sa.select(*pkey_columns).group_by(*pkey_columns).having(sa.func.count() > 1).subquery()
    query = (
        sa.select(table)
        .where(sa.and_(*(table.c[col.name].is_not_distinct_from(subquery.c[col.name]) for col in pkey_columns)))
        .order_by(*pkey_columns, sa.desc(table.c._created))
    )

    results = connection.execute(query)
    for row in results:
        pkey = _extract_pkey_values(row, pkey_column_names)
        entry = dmodel.get_entry(pkey)
        data = _DuplicateData(row["_id"], row["_revision"], row["_created"])
        entry.add_data(data)


@dispatch(Context, Model)
def _add_uniqueness(context: Context, model: Model):
    _add_uniqueness(context, model.backend, model)


@dispatch(Context, Backend, Model)
def _add_uniqueness(context: Context, backend: Backend, model: Model):
    echo(f'Skipped adding uniqueness to "{model.model_type()}"')


@dispatch(Context, PostgreSQL, Model)
def _add_uniqueness(context: Context, backend: PostgreSQL, model: Model):
    table_name = get_pg_table_name(model)
    column_name_list = list()
    for prop in model.external.pkeys:
        column_name_list.append(get_pg_column_name(prop.place))

    constraint_name = get_pg_constraint_name(table_name, column_name_list)
    query = sa.text(f'''
        ALTER TABLE "{table_name}"
        ADD CONSTRAINT "{constraint_name}" UNIQUE ({",".join(f'"{col}"' for col in column_name_list)});
    ''')
    transaction = context.get("transaction")
    conn = transaction.connection
    conn.execute(query)
    echo(f'Added unique constraint to "{model.model_type()}"')


def _extract_pkey_values(row: dict, keys: list) -> tuple:
    return tuple(row[key] for key in keys)


def _ensure_table_set(column: sa.Column, table: sa.Table) -> sa.Column:
    column.table = table
    return column


def _models_without_unique_constraint(context: Context) -> Iterator[Model]:
    store = ensure_store_is_loaded(context)
    for model in commands.get_models(context, store.manifest).values():
        if model.model_type().startswith("_") or not model.external or model.external.unknown_primary_key:
            continue

        check = _fulfills_unique_constraint(context, model)
        if not check:
            yield model


def _generate_duplicate_model_data(context: Context, model: Model):
    dmodel = _DuplicateModel(model, True)
    _create_duplicate_entries(context, dmodel)
    return dmodel


async def _duplicate_mapping_to_dataitem(
    context: Context, model: Model, mapping: _DuplicateMoveMapping
) -> AsyncIterator[DataItem]:
    for target in mapping.targets:
        payload = {
            "_where": f'eq(_id, "{target.id_}")',
            "_op": Action.MOVE.value,
            "_revision": target.rev_,
            "_id": mapping.primary.id_,
        }
        item = dataitem_from_payload(context, model, payload)
        yield item


async def _migrate_duplicates(
    context: Context,
    dmodel: _DuplicateModel,
    destructive: bool,
) -> bool:
    model = dmodel.model
    if len(dmodel.duplicate_entries) > 0:
        if not destructive:
            echo(f'"{model.model_type()}" contains duplicate values, use --destructive to migrate them')
            return False

        duplicate_counter = tqdm.tqdm(desc=model.model_type(), ascii=True)
        try:
            for mapping in dmodel.get_move_mappings():
                stream = _duplicate_mapping_to_dataitem(context, model, mapping)
                dstream = move_stream(context, model, stream)
                async for _ in dstream:
                    duplicate_counter.update(1)
        finally:
            duplicate_counter.close()
    else:
        echo(f'"{model.model_type()}" does not contain duplicate values')

    return True


def migrate_duplicates(context: Context, destructive: bool, **kwargs):
    models = list(_models_without_unique_constraint(context))
    counter = tqdm.tqdm(desc="MIGRATING MODELS WITH DUPLICATES", ascii=True, total=len(models))
    store = context.get("store")
    try:
        with context:
            require_auth(context)
            context.attach("transaction", validate_and_return_transaction, context, store.manifest.backend, write=True)
            for model in models:
                dmodel = _generate_duplicate_model_data(context, model)
                result = asyncio.run(_migrate_duplicates(context, dmodel, destructive))
                if dmodel.missing_unique and result:
                    _add_uniqueness(context, model)
                    pass
                counter.update(1)
    finally:
        counter.close()


def cli_requires_deduplicate_migrations(context: Context, **kwargs):
    models = _models_without_unique_constraint(context)
    for _ in models:
        return True
    return False
