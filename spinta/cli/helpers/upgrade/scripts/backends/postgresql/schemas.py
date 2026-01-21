from contextlib import ExitStack
from typing import Generator

from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from click import echo
from sqlalchemy.engine import Inspector
from tqdm import tqdm


from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_identifier, TableIdentifier
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_sequence_name, get_pg_name
from spinta.backends.postgresql.helpers.migrate.actions import (
    CreateSchemaMigrationAction,
    RenameTableMigrationAction,
    RenameConstraintMigrationAction,
    RenameIndexMigrationAction,
    RenameSequenceMigrationAction,
    MigrationHandler,
)
from spinta.backends.postgresql.helpers.migrate.migrate import extract_sequence_name
from spinta.backends.postgresql.helpers.name import (
    get_pg_constraint_name,
    get_pg_foreign_key_name,
    get_pg_index_name,
    PG_NAMING_CONVENTION,
)
from spinta.cli.helpers.script.helpers import ensure_store_is_loaded
from spinta.components import Context, Model

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from spinta.types.namespace import sort_models_by_ref_and_base
from spinta.utils.sqlalchemy import Convention

pg_identifier_preparer = postgresql.dialect().identifier_preparer


def cli_requires_schema_migration(context: Context, **kwargs) -> bool:
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

    models = collect_models_without_schemas(context, inspectors)
    if next(models, None):
        return True

    return False


def collect_models_without_schemas(context: Context, inspectors: dict) -> Generator:
    store = context.get("store")
    models = commands.get_models(context, store.manifest)
    sorted_models = sort_models_by_ref_and_base(list(models.values()))

    for model in sorted_models:
        if model_missing_schema(model, inspectors):
            yield model


def model_missing_schema(model: Model, inspectors: dict) -> bool:
    if not isinstance(model.backend, PostgreSQL):
        return False

    backend = model.backend

    table_identifier = get_table_identifier(model)
    inspector = inspectors[backend.name]
    schemas = inspector.get_schema_names()

    if table_identifier.pg_schema_name and table_identifier.pg_schema_name not in schemas:
        return True

    associated_tables = gather_associated_tables(backend, table_identifier)
    for table in associated_tables:
        if not inspector.has_table(table.name, schema=table.schema):
            return True

    return False


def gather_associated_tables(backend: PostgreSQL, table_identifier: TableIdentifier) -> list[sa.Table]:
    return [
        table
        for key, table in backend.tables.items()
        if key == table_identifier.logical_qualified_name
        or key.startswith(f"{table_identifier.logical_qualified_name}/:")
    ]


def migrate_schemas(context: Context, **kwargs):
    ensure_store_is_loaded(context)
    store = context.get("store")

    inspectors = {}
    metadata = {}
    echo("Reflecting databases schemas")
    for name, backend in store.backends.items():
        if not isinstance(backend, PostgreSQL):
            continue

        if name not in inspectors:
            inspectors[name] = sa.inspect(backend.engine)

        if name not in metadata:
            meta = sa.MetaData(backend.engine)
            meta.reflect(extend_existing=True)

            metadata[name] = meta

    if not inspectors or not metadata:
        return

    affected_models = list(collect_models_without_schemas(context, inspectors))

    echo("Generating schema migrations")
    progress_bar = tqdm(affected_models)

    with ExitStack() as stack:
        conns = {}
        ops = {}
        handlers = {}

        def get_conn(backend_: PostgreSQL):
            if backend_.name not in conns:
                conns[backend_.name] = stack.enter_context(backend_.begin())
            return conns[backend_.name]

        def cache_op(backend_: PostgreSQL):
            if backend_.name not in ops:
                connection = get_conn(backend_)
                ctx = MigrationContext.configure(
                    connection,
                    # If uncommented, it will only generate SQL and output it to stdout
                    # opts={
                    #     "as_sql": True,
                    #     "literal_binds": True,
                    # },
                )
                operations = Operations(ctx)
                ops[backend_.name] = operations

        def get_handler(backend_: PostgreSQL):
            # Preload operations
            cache_op(backend_)

            if backend_.name not in handlers:
                handlers[backend_.name] = MigrationHandler()
            return handlers[backend_.name]

        for model in progress_bar:
            backend = model.backend
            handler = get_handler(backend)
            table_identifier = get_table_identifier(model)
            inspector = inspectors[backend.name]

            if table_identifier.pg_schema_name:
                handler.add_action(CreateSchemaMigrationAction(schema_name=table_identifier.pg_schema_name))

            associated_tables = gather_associated_tables(backend, table_identifier)
            progress_bar.display(model.model_type())
            for table in associated_tables:
                if inspector.has_table(table.name, schema=table.schema):
                    continue

                meta = metadata[backend.name]
                old_table_name = get_pg_name(table.comment or table.name)

                if not inspector.has_table(old_table_name, schema=inspector.default_schema_name):
                    continue

                table = meta.tables.get(old_table_name, None)
                if table is None:
                    continue

                _update_table_migration(handler, meta, model, table, inspector)

        echo("Running schema migrations")
        for key, op in ops.items():
            handler = handlers[key]
            handler.run_migrations(op)


def _update_table_migration(
    handler: MigrationHandler, meta: sa.MetaData, model: Model, table: sa.Table, inspector: Inspector
):
    old_table_identifier = get_table_identifier(table, default_pg_schema="public")
    new_table_identifier = get_table_identifier(
        model,
        old_table_identifier.table_type,
        table_arg=old_table_identifier.table_arg,
        default_pg_schema="public",
    )
    handler.add_action(
        RenameTableMigrationAction(old_table_identifier=old_table_identifier, new_table_identifier=new_table_identifier)
    )

    # Rename unique, index and foreign key constraints
    updated_constraints = []

    pk_constraint = inspector.get_pk_constraint(
        old_table_identifier.pg_table_name, schema=old_table_identifier.pg_schema_name
    )
    if pk_constraint and pk_constraint["name"]:
        new_constraint_name = PG_NAMING_CONVENTION[Convention.PK] % {"table_name": new_table_identifier.pg_table_name}

        if new_constraint_name != pk_constraint["name"]:
            handler.add_action(
                RenameConstraintMigrationAction(
                    table_identifier=new_table_identifier,
                    old_constraint_name=pk_constraint["name"],
                    new_constraint_name=new_constraint_name,
                )
            )
        updated_constraints.append(pk_constraint["name"])

    unique_constraints = {
        constraint["name"]: constraint
        for constraint in inspector.get_unique_constraints(
            old_table_identifier.pg_table_name, schema=old_table_identifier.pg_schema_name
        )
    }
    for constraint_name, constraint in unique_constraints.items():
        if constraint_name in updated_constraints:
            continue

        new_constraint_name = get_pg_constraint_name(new_table_identifier.pg_table_name, constraint["column_names"])
        if constraint_name != new_constraint_name:
            handler.add_action(
                RenameConstraintMigrationAction(
                    table_identifier=new_table_identifier,
                    old_constraint_name=constraint_name,
                    new_constraint_name=new_constraint_name,
                )
            )
        updated_constraints.append(constraint_name)

    foreign_constraints = {
        constraint["name"]: constraint
        for constraint in inspector.get_foreign_keys(
            old_table_identifier.pg_table_name, schema=old_table_identifier.pg_schema_name
        )
    }
    for constraint_name, constraint in foreign_constraints.items():
        if constraint_name in updated_constraints:
            continue

        ref_table_name = constraint["referred_table"]
        ref_table_schema = constraint["referred_schema"]
        ref_table_metadata_name = ".".join([ref_table_schema, ref_table_name] if ref_table_schema else [ref_table_name])
        if (ref_table := meta.tables.get(ref_table_metadata_name, None)) is None:
            meta.reflect(schema=ref_table_schema, only=[ref_table_name], extend_existing=True)
            ref_table = meta.tables[ref_table_metadata_name]

        ref_table_identifier = get_table_identifier(ref_table)
        new_constraint_name = get_pg_foreign_key_name(
            table_identifier=new_table_identifier,
            referred_table_identifier=ref_table_identifier,
            column_name=constraint["constrained_columns"][0],
        )
        if constraint_name != new_constraint_name:
            handler.add_action(
                RenameConstraintMigrationAction(
                    table_identifier=new_table_identifier,
                    old_constraint_name=constraint_name,
                    new_constraint_name=new_constraint_name,
                )
            )
        updated_constraints.append(constraint_name)

    indexes = {
        index["name"]: index
        for index in inspector.get_indexes(
            old_table_identifier.pg_table_name, schema=old_table_identifier.pg_schema_name
        )
    }
    for index_name, index in indexes.items():
        if index_name in updated_constraints:
            continue

        new_index_name = get_pg_index_name(new_table_identifier.pg_table_name, index["column_names"])
        if index_name != new_index_name:
            handler.add_action(
                RenameIndexMigrationAction(
                    old_index_name=index_name,
                    new_index_name=new_index_name,
                    table_identifier=new_table_identifier,
                    old_table_identifier=old_table_identifier,
                )
            )
        updated_constraints.append(index_name)

    # Currently, only changelog has sequences
    if old_table_identifier.table_type == TableType.CHANGELOG:
        old_sequence_name = extract_sequence_name(table)
        new_sequence_name = get_pg_sequence_name(new_table_identifier.pg_table_name)
        if new_sequence_name != extract_sequence_name(table):
            handler.add_action(
                RenameSequenceMigrationAction(
                    old_name=old_sequence_name,
                    new_name=new_sequence_name,
                    table_identifier=new_table_identifier,
                    old_table_identifier=old_table_identifier,
                )
            )
