from typing import List, Dict, Tuple

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.engine.reflection import Inspector

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.commands.migrate.constants import EXCLUDED_MODELS
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_column_name
from spinta.backends.postgresql.helpers.migrate.actions import MigrationHandler
from spinta.backends.postgresql.helpers.migrate.migrate import drop_all_indexes_and_constraints, model_name_key, \
    MigratePostgresMeta
from spinta.backends.postgresql.helpers.name import get_pg_table_name, get_pg_column_name, \
    get_pg_foreign_key_name, get_pg_file_name, PG_NAMING_CONVENTION
from spinta.utils.sqlalchemy import get_metadata_naming_convention
from spinta.cli.helpers.migrate import MigrateRename, MigrateMeta
from spinta.commands import create_exception
from spinta.components import Context, Model
from spinta.datasets.inspect.helpers import zipitems
from spinta.manifests.components import Manifest
from spinta.types.datatype import Ref, File
from spinta.types.namespace import sort_models_by_ref_and_base
from spinta.utils.schema import NA


@commands.migrate.register(Context, Manifest, PostgreSQL, MigrateMeta)
def migrate(context: Context, manifest: Manifest, backend: PostgreSQL, migrate_meta: MigrateMeta):
    conn = context.get(f'transaction.{backend.name}')
    ctx = MigrationContext.configure(conn, opts={
        "as_sql": migrate_meta.plan,
        "literal_binds": migrate_meta.plan,
        "target_metadata": backend.schema
    })
    op = Operations(ctx)
    inspector = sa.inspect(conn)
    metadata = sa.MetaData(bind=conn, naming_convention=get_metadata_naming_convention(PG_NAMING_CONVENTION))
    metadata.reflect(
        only=_filter_reflect_datasets(
            inspector,
            migrate_meta.datasets
        )
    )

    handler = MigrationHandler()
    meta = MigratePostgresMeta(
        inspector=inspector,
        handler=handler,
        rename=migrate_meta.rename
    )

    models = commands.get_models(context, manifest)
    models, tables = _filter_models_and_tables(
        models=models,
        existing_tables=inspector.get_table_names(),
        filtered_datasets=migrate_meta.datasets,
        rename=migrate_meta.rename
    )

    sorted_models = sort_models_by_ref_and_base(list(models.values()))
    sorted_model_names = list([model.name for model in sorted_models])
    # Do reverse zip, to ensure that sorted models get selected first
    zipped_names = zipitems(
        sorted_model_names,
        tables,
        model_name_key
    )

    for zipped_name in zipped_names:
        for new_model_name, old_table_name in zipped_name:
            # Skip Changelog and File table migrations, because this is done in DataType migration section
            if old_table_name and any(
                value in old_table_name for value in (TableType.CHANGELOG.value, TableType.FILE.value)
            ):
                continue

            # Skip excluded tables
            if old_table_name and old_table_name in EXCLUDED_MODELS:
                continue

            old = NA
            if old_table_name:
                name = get_pg_table_name(migrate_meta.rename.get_old_table_name(old_table_name))
                old = metadata.tables[name]

            new = commands.get_model(context, manifest, new_model_name) if new_model_name else new_model_name
            commands.migrate(context, backend, meta, old, new)
    _clean_up_file_type(inspector, sorted_models, handler, migrate_meta.rename)

    try:
        # Handle autocommit migrations differently
        if migrate_meta.autocommit:
            # Recreate new connection, that auto commits
            with backend.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                ctx = MigrationContext.configure(conn, opts={
                    "as_sql": migrate_meta.plan,
                    "literal_binds": migrate_meta.plan,
                    "target_metadata": backend.schema
                })
                op = Operations(ctx)
                handler.run_migrations(op)
            return

        # If in plan mode, just run the migrations
        # ctx is already setup so that it would not execute the code
        if migrate_meta.plan:
            with ctx.begin_transaction():
                handler.run_migrations(op)
            return

        # Begin transaction or take one, if it has already been opened
        if ctx._in_connection_transaction():
            trx = ctx.connection._transaction
        else:
            trx = ctx.begin_transaction()

        try:
            handler.run_migrations(op)

            # You can add custom logic that you might want to execute after running migrations, but before commiting
            # transaction, like `InternalSqlManifest` can update its own manifest schema, so you might want to try update
            # it here, incase it fails, transaction will be reverted and no changes will be made.
            if migrate_meta.migration_extension is not None:
                migrate_meta.migration_extension()
            trx.commit()
        except Exception:
            trx.rollback()
            raise

    except sa.exc.OperationalError as error:
        exception = create_exception(manifest, error)
        raise exception


def _filter_reflect_datasets(
    inspector: Inspector,
    datasets: list
):
    if not datasets:
        return None

    all_tables = inspector.get_table_names()
    return [table for table in all_tables if any(table.startswith(dataset) for dataset in datasets)]


def _filter_models_and_tables(
    models: Dict[str, Model],
    existing_tables: List[str],
    filtered_datasets: List[str],
    rename: MigrateRename
) -> Tuple[Dict[str, Model], List[str]]:
    tables = []

    # Filter if only specific dataset can be changed
    if filtered_datasets:
        filtered_models = {}
        for key, model in models.items():
            if model.external and model.external.dataset and model.external.dataset.name in filtered_datasets:
                filtered_models[key] = model
        models = filtered_models

        filtered_names = []
        for table_name in existing_tables:
            for dataset_name in filtered_datasets:
                if table_name.startswith(f'{dataset_name}/'):
                    # Check if its model or another sub dataset
                    additional_check = table_name.replace(f'{dataset_name}/', '', 1)
                    if '/' not in additional_check:
                        filtered_names.append(table_name)
        existing_tables = filtered_names

    for table in existing_tables:
        # Do not apply `get_pg_table_name`, since this will be done later on while zipping with `model_name_key`
        name = rename.get_table_name(table)
        if name not in models.keys():
            name = table
        tables.append(name)

    return models, tables


def _handle_foreign_key_constraints(inspector: Inspector, models: List[Model], handler: MigrationHandler,
                                    rename: MigrateRename):
    existing_table_names = set(inspector.get_table_names())

    for model in models:
        source_name = get_table_name(model)
        source_table = get_pg_table_name(source_name)
        old_name = get_pg_table_name(rename.get_old_table_name(source_name))
        foreign_keys = inspector.get_foreign_keys(old_name) if old_name in existing_table_names else []

        # Handle Base _id foreign key constraints
        id_constraint = next(
            (constraint for constraint in foreign_keys if constraint.get("constrained_columns") == ["_id"]), None
        )
        if id_constraint is not None:
            foreign_keys.remove(id_constraint)

        if model.base and commands.identifiable(model.base):
            add_constraint = True
            referent_table = get_pg_table_name(get_table_name(model.base.parent))
            fk_name = get_pg_foreign_key_name(referent_table, "_id")
            if id_constraint is not None:
                if id_constraint["name"] == fk_name and id_constraint["referred_table"] == referent_table:
                    add_constraint = False
                else:
                    handler.add_action(ma.DropConstraintMigrationAction(
                        table_name=source_table,
                        constraint_name=id_constraint["name"]
                    ), True)

            if add_constraint:
                handler.add_action(
                    ma.CreateForeignKeyMigrationAction(
                        source_table=source_table,
                        referent_table=referent_table,
                        constraint_name=fk_name,
                        local_cols=["_id"],
                        remote_cols=["_id"]
                    ), True
                )
        else:
            if id_constraint is not None:
                handler.add_action(ma.DropConstraintMigrationAction(
                    table_name=source_table,
                    constraint_name=id_constraint["name"]
                ), True)

        # Handle Ref foreign key constraints
        required_ref_props = {}
        for prop in model.flatprops.values():
            if isinstance(prop.dtype, Ref):
                if not prop.level or prop.level > 3:
                    column_name = get_pg_column_name(f"{prop.place}._id")
                    name = get_pg_foreign_key_name(source_table, column_name)
                    required_ref_props[name] = {
                        "name": name,
                        "constrained_columns": [column_name],
                        "referred_table": get_pg_table_name(get_table_name(prop.dtype.model)),
                        "referred_columns": ["_id"]
                    }

        for foreign_key in foreign_keys:
            if foreign_key["name"] not in required_ref_props.keys():
                handler.add_action(ma.DropConstraintMigrationAction(
                    table_name=source_table,
                    constraint_name=foreign_key["name"]
                ), True)
                continue

            constraint = required_ref_props[foreign_key["name"]]
            if (
                foreign_key["constrained_columns"] == constraint["constrained_columns"] and
                foreign_key["referred_table"] == constraint["referred_table"] and
                foreign_key["referred_columns"] == constraint["referred_columns"]
            ):
                del required_ref_props[foreign_key["name"]]
            else:
                handler.add_action(ma.DropConstraintMigrationAction(
                    table_name=source_table,
                    constraint_name=foreign_key["name"]
                ), True)

        for prop in required_ref_props.values():
            handler.add_action(ma.CreateForeignKeyMigrationAction(
                source_table=source_table,
                referent_table=prop["referred_table"],
                constraint_name=prop["name"],
                local_cols=prop["constrained_columns"],
                remote_cols=prop["referred_columns"]
            ), True)


def _clean_up_file_type(inspector: Inspector, models: List[Model], handler: MigrationHandler, rename: MigrateRename):
    allowed_file_tables = []
    existing_tables = []
    for model in models:
        existing_tables.append(rename.get_old_table_name(model.name))
        for prop in model.properties.values():
            if isinstance(prop.dtype, File):
                old_table = rename.get_old_table_name(get_table_name(model))
                old_column = rename.get_old_column_name(old_table, get_column_name(prop))
                allowed_file_tables.append(
                    get_pg_file_name(old_table, old_column)
                )

    for table in inspector.get_table_names():
        if TableType.FILE.value in table:
            split = table.split(f'{TableType.FILE.value}/')
            if split[0] in existing_tables:
                if table not in allowed_file_tables and not split[1].startswith("__"):
                    new_name = get_pg_file_name(split[0], f'__{split[1]}')
                    if inspector.has_table(new_name):
                        handler.add_action(ma.DropTableMigrationAction(
                            table_name=new_name
                        ))
                    handler.add_action(ma.RenameTableMigrationAction(
                        old_table_name=table,
                        new_table_name=new_name
                    ))
                    drop_all_indexes_and_constraints(inspector, table, new_name, handler)
