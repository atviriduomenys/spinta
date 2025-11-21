from typing import Generator

from multipledispatch import dispatch
from tqdm import tqdm

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_name
from spinta.cli.helpers.script.helpers import ensure_store_is_loaded
from spinta.components import Context, Property, Model

from sqlalchemy.dialects.postgresql.base import PGInspector

import sqlalchemy as sa

from spinta.types.datatype import DataType, Array, File


def cli_requires_comments_migration(context: Context, **kwargs) -> bool:
    ensure_store_is_loaded(context)
    store = context.get("store")

    inspectors = {}
    for name, backend in store.backends.items():
        if not isinstance(backend, PostgreSQL):
            continue

        if name not in inspectors:
            inspectors[name] = sa.inspect(backend.engine)

    if not inspectors:
        return False

    models = collect_models_without_comments(context, inspectors)
    if next(models, None):
        return True

    return False


def collect_models_without_comments(context: Context, inspectors: dict) -> Generator:
    store = context.get("store")
    models = commands.get_models(context, store.manifest).values()

    for model in models:
        if model_missing_comment(model, inspectors):
            yield model


def model_missing_comment(model: Model, inspectors: dict) -> bool:
    if not isinstance(model.backend, PostgreSQL):
        return False

    backend = model.backend

    table_name = get_table_name(model)
    changes_name = get_table_name(model, TableType.CHANGELOG)
    redirect_name = get_table_name(model, TableType.REDIRECT)
    inspector = inspectors[backend.name]

    if not table_contains_all_comments(backend, inspector, table_name):
        return True

    for prop in model.flatprops.values():
        if not custom_property_table_contains_comments(backend, inspector, prop):
            return True

    if table_name.startswith("_"):
        return False

    # Specialized tables for normal models
    if not table_contains_all_comments(backend, inspector, changes_name):
        return True
    if not table_contains_all_comments(backend, inspector, redirect_name):
        return True

    return False


@dispatch(PostgreSQL, PGInspector, Property)
def custom_property_table_contains_comments(backend: PostgreSQL, inspector: PGInspector, prop: Property) -> bool:
    return custom_property_table_contains_comments(backend, inspector, prop.dtype)


@dispatch(PostgreSQL, PGInspector, DataType)
def custom_property_table_contains_comments(backend: PostgreSQL, inspector: PGInspector, dtype: DataType) -> bool:
    return True


@dispatch(PostgreSQL, PGInspector, Array)
def custom_property_table_contains_comments(backend: PostgreSQL, inspector: PGInspector, dtype: Array) -> bool:
    name = get_table_name(dtype.prop, TableType.LIST)
    return table_contains_all_comments(backend, inspector, name)


@dispatch(PostgreSQL, PGInspector, File)
def custom_property_table_contains_comments(backend: PostgreSQL, inspector: PGInspector, dtype: File) -> bool:
    name = get_table_name(dtype.prop, TableType.FILE)
    return table_contains_all_comments(backend, inspector, name)


def table_contains_comment(inspector: PGInspector, table_name: str, table: sa.Table) -> bool:
    if table is None:
        return True

    if not inspector.has_table(table_name):
        return True

    comment = table.comment
    result = inspector.get_table_comment(table_name).get("text")
    if not result and comment:
        return False

    return result == comment


def table_contains_all_comments(backend: PostgreSQL, inspector: PGInspector, table: str) -> bool:
    bootstrap_table = backend.tables.get(table)

    if bootstrap_table is None:
        return True

    if not table_contains_comment(inspector, get_pg_name(table), bootstrap_table):
        return False

    table_columns = {column["name"]: column for column in inspector.get_columns(bootstrap_table.name)}
    for bootstrap_column in backend.tables.get(table).columns:
        if not bootstrap_column.comment:
            continue

        if not (column := table_columns.get(bootstrap_column.name)):
            continue

        if column["comment"] != bootstrap_column.comment:
            return False

    return True


def migrate_comments(context: Context, **kwargs):
    ensure_store_is_loaded(context)
    store = context.get("store")

    inspectors = {}
    for name, backend in store.backends.items():
        if not isinstance(backend, PostgreSQL):
            continue

        if name not in inspectors:
            inspectors[name] = sa.inspect(backend.engine)

    if not inspectors:
        return

    affected_models = list(collect_models_without_comments(context, inspectors))

    progress_bar = tqdm(affected_models)
    for model in progress_bar:
        backend = model.backend
        with backend.begin() as conn:
            table_name = get_table_name(model)
            changes_name = get_table_name(model, TableType.CHANGELOG)
            redirect_name = get_table_name(model, TableType.REDIRECT)
            inspector = inspectors[backend.name]

            progress_bar.display(model.model_type())

            apply_missing_table_comments(conn, backend, inspector, table_name, progress_bar=progress_bar)

            for prop in model.flatprops.values():
                apply_custom_property_table_comments(conn, backend, inspector, prop, progress_bar)

            if table_name.startswith("_"):
                continue

            # Specialized tables for normal models
            apply_missing_table_comments(conn, backend, inspector, changes_name, progress_bar=progress_bar)
            apply_missing_table_comments(conn, backend, inspector, redirect_name, progress_bar=progress_bar)


def cleanup_comment(comment: str) -> str:
    if not comment:
        return "NULL"

    return f"'{comment}'"


def apply_missing_table_comments(
    conn: sa.engine.Connection, backend: PostgreSQL, inspector: PGInspector, table: str, progress_bar: tqdm = None
):
    bootstrap_table = backend.tables.get(table)
    if bootstrap_table is None:
        if progress_bar:
            progress_bar.write(f"COULD NOT FIND '{table}' TABLE")
        return

    if not table_contains_comment(inspector, get_pg_name(table), bootstrap_table):
        if progress_bar:
            progress_bar.write(f"'{get_pg_name(table)}' <- {cleanup_comment(bootstrap_table.comment)}")

        conn.execute(f'COMMENT ON TABLE "{get_pg_name(table)}" IS {cleanup_comment(bootstrap_table.comment)}')

    table_columns = {column["name"]: column for column in inspector.get_columns(bootstrap_table.name)}
    for bootstrap_column in backend.tables.get(table).columns:
        if not bootstrap_column.comment:
            continue

        if not (column := table_columns.get(bootstrap_column.name)):
            continue

        if column["comment"] != bootstrap_column.comment:
            if progress_bar:
                progress_bar.write(
                    f"'{get_pg_name(table)}'.'{column['name']}' <- {cleanup_comment(bootstrap_column.comment)}"
                )
            conn.execute(
                f'COMMENT ON COLUMN "{get_pg_name(table)}"."{column["name"]}" IS {cleanup_comment(bootstrap_column.comment)}'
            )


@dispatch(sa.engine.Connection, PostgreSQL, PGInspector, Property, object)
def apply_custom_property_table_comments(
    conn: sa.engine.Connection, backend: PostgreSQL, inspector: PGInspector, prop: Property, progress_bar: tqdm
):
    return apply_custom_property_table_comments(conn, backend, inspector, prop.dtype, progress_bar)


@dispatch(sa.engine.Connection, PostgreSQL, PGInspector, DataType, object)
def apply_custom_property_table_comments(
    conn: sa.engine.Connection, backend: PostgreSQL, inspector: PGInspector, dtype: DataType, progress_bar: tqdm
):
    return


@dispatch(sa.engine.Connection, PostgreSQL, PGInspector, Array, object)
def apply_custom_property_table_comments(
    conn: sa.engine.Connection, backend: PostgreSQL, inspector: PGInspector, dtype: Array, progress_bar: tqdm
):
    name = get_table_name(dtype.prop, TableType.LIST)
    return apply_missing_table_comments(conn, backend, inspector, name, progress_bar=progress_bar)


@dispatch(sa.engine.Connection, PostgreSQL, PGInspector, File, object)
def apply_custom_property_table_comments(
    conn: sa.engine.Connection, backend: PostgreSQL, inspector: PGInspector, dtype: File, progress_bar: tqdm
):
    name = get_table_name(dtype.prop, TableType.FILE)
    return apply_missing_table_comments(conn, backend, inspector, name, progress_bar=progress_bar)
