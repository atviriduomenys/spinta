from typing import List

import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.commands.migrate.constants import EXCLUDED_MODELS
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.actions import MigrationHandler
from spinta.backends.postgresql.helpers.migrate.migrate import (
    PostgresqlMigrationContext,
    CastMatrix,
    validate_rename_map,
    RenameMap,
    part_of_dataset,
    generate_model_tables_mapping,
    ModelTables,
    name_key,
)
from spinta.backends.postgresql.helpers.name import (
    get_pg_table_name,
    get_pg_column_name,
    get_pg_foreign_key_name,
    PG_NAMING_CONVENTION,
)
from spinta.cli.helpers.migrate import MigrationConfig
from spinta.commands import create_exception
from spinta.components import Context, Model
from spinta.datasets.inspect.helpers import zipitems
from spinta.manifests.components import Manifest
from spinta.types.datatype import Ref
from spinta.types.namespace import sort_models_by_ref_and_base
from spinta.utils.schema import NA
from spinta.utils.sqlalchemy import get_metadata_naming_convention


@commands.migrate.register(Context, Manifest, PostgreSQL, MigrationConfig)
def migrate(context: Context, manifest: Manifest, backend: PostgreSQL, migration_config: MigrationConfig):
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    conn = context.get(f"transaction.{backend.name}")
    ctx = MigrationContext.configure(
        conn,
        opts={
            "as_sql": migration_config.plan,
            "literal_binds": migration_config.plan,
            "target_metadata": backend.schema,
        },
    )
    op = Operations(ctx)
    inspector = sa.inspect(conn)
    metadata = sa.MetaData(bind=conn, naming_convention=get_metadata_naming_convention(PG_NAMING_CONVENTION))
    metadata.reflect(only=_filter_reflect_datasets(inspector, migration_config.datasets))

    handler = MigrationHandler()
    migration_ctx = PostgresqlMigrationContext(
        config=migration_config,
        inspector=inspector,
        handler=handler,
        rename=RenameMap(rename_src=migration_config.rename_src),
        cast_matrix=CastMatrix(backend.engine),
    )
    validate_rename_map(context, migration_ctx.rename, manifest)

    mapped_model_tables = generate_model_tables_mapping(metadata, inspector, [EXCLUDED_MODELS])

    models = commands.get_models(context, manifest)
    models, tables = _filter_models_and_tables(
        models=models,
        model_tables=mapped_model_tables,
        filtered_datasets=migration_config.datasets,
        rename=migration_ctx.rename,
    )

    sorted_models = sort_models_by_ref_and_base(list(models.values()))
    sorted_models_mapping = {model.name: model for model in sorted_models}
    # Do reverse zip, to ensure that sorted models get selected first
    zipped_names = zipitems(sorted_models_mapping.keys(), tables.keys(), name_key)

    for zipped_name in zipped_names:
        for model_name, model_tables_name in zipped_name:
            model = NA
            model_tables = NA
            if model_name:
                model = sorted_models_mapping[model_name]
            if model_tables_name:
                model_tables = tables[model_tables_name]

            commands.migrate(context, backend, migration_ctx, model_tables, model)

    try:
        # Handle autocommit migrations differently
        if migration_config.autocommit:
            # Recreate new connection, that auto commits
            with backend.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                ctx = MigrationContext.configure(
                    conn,
                    opts={
                        "as_sql": migration_config.plan,
                        "literal_binds": migration_config.plan,
                        "target_metadata": backend.schema,
                    },
                )
                op = Operations(ctx)
                handler.run_migrations(op)
            return

        # If in plan mode, just run the migrations
        # ctx is already setup so that it would not execute the code
        if migration_config.plan:
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
            if migration_config.migration_extension is not None:
                migration_config.migration_extension()
            trx.commit()
        except Exception:
            trx.rollback()
            raise

    except sa.exc.OperationalError as error:
        exception = create_exception(manifest, error)
        raise exception


def _filter_reflect_datasets(inspector: Inspector, datasets: list):
    if not datasets:
        return None

    all_tables = inspector.get_table_names()
    return [
        table
        for table in all_tables
        if any(part_of_dataset(table, dataset, ignore_compression=False) for dataset in datasets)
    ]


def _filter_models_and_tables(
    models: dict[str, Model],
    model_tables: dict[str, ModelTables],
    filtered_datasets: list[str],
    rename: RenameMap,
) -> tuple[dict[str, Model], dict[str, ModelTables]]:
    # tables = []

    # Filter if only specific dataset can be changed
    if filtered_datasets:
        filtered_models = {}
        for key, model in models.items():
            if model.external and model.external.dataset and model.external.dataset.name in filtered_datasets:
                filtered_models[key] = model
        models = filtered_models

        filtered_names = {}
        for table_name in model_tables:
            for dataset_name in filtered_datasets:
                if part_of_dataset(table_name, dataset_name, ignore_compression=False):
                    filtered_names[table_name] = model_tables[table_name]
        model_tables = filtered_names

    remapped_tables = {}
    for name, table in model_tables.items():
        # Do not apply `get_pg_table_name`, since this will be done later on while zipping with `model_name_key`
        table_name = rename.get_table_name(name)
        if table_name not in models.keys():
            table_name = name
        remapped_tables[table_name] = table

    return models, remapped_tables


def _handle_foreign_key_constraints(
    inspector: Inspector, models: List[Model], handler: MigrationHandler, rename: RenameMap
):
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
                    handler.add_action(
                        ma.DropConstraintMigrationAction(
                            table_name=source_table, constraint_name=id_constraint["name"]
                        ),
                        True,
                    )

            if add_constraint:
                handler.add_action(
                    ma.CreateForeignKeyMigrationAction(
                        source_table=source_table,
                        referent_table=referent_table,
                        constraint_name=fk_name,
                        local_cols=["_id"],
                        remote_cols=["_id"],
                    ),
                    True,
                )
        else:
            if id_constraint is not None:
                handler.add_action(
                    ma.DropConstraintMigrationAction(table_name=source_table, constraint_name=id_constraint["name"]),
                    True,
                )

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
                        "referred_columns": ["_id"],
                    }

        for foreign_key in foreign_keys:
            if foreign_key["name"] not in required_ref_props.keys():
                handler.add_action(
                    ma.DropConstraintMigrationAction(table_name=source_table, constraint_name=foreign_key["name"]), True
                )
                continue

            constraint = required_ref_props[foreign_key["name"]]
            if (
                foreign_key["constrained_columns"] == constraint["constrained_columns"]
                and foreign_key["referred_table"] == constraint["referred_table"]
                and foreign_key["referred_columns"] == constraint["referred_columns"]
            ):
                del required_ref_props[foreign_key["name"]]
            else:
                handler.add_action(
                    ma.DropConstraintMigrationAction(table_name=source_table, constraint_name=foreign_key["name"]), True
                )

        for prop in required_ref_props.values():
            handler.add_action(
                ma.CreateForeignKeyMigrationAction(
                    source_table=source_table,
                    referent_table=prop["referred_table"],
                    constraint_name=prop["name"],
                    local_cols=prop["constrained_columns"],
                    remote_cols=prop["referred_columns"],
                ),
                True,
            )
