import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector

from spinta import commands
from spinta.backends.postgresql.commands.migrate.constants import EXCLUDED_MODELS
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.actions import MigrationHandler
from spinta.backends.postgresql.helpers.migrate.migrate import (
    PostgresqlMigrationContext,
    validate_rename_map,
    part_of_dataset,
    generate_model_tables_mapping,
    ModelTables,
    name_key,
    get_spinta_schemas,
    create_missing_schemas,
)
from spinta.backends.postgresql.helpers.migrate.name import RenameMap
from spinta.backends.postgresql.helpers.migrate.cast import CastMatrix
from spinta.backends.postgresql.helpers.name import (
    PG_NAMING_CONVENTION,
)
from spinta.cli.helpers.migrate import MigrationConfig
from spinta.commands import create_exception
from spinta.components import Context, Model
from spinta.datasets.inspect.helpers import zipitems
from spinta.manifests.components import Manifest
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
    schemas = get_spinta_schemas(backend.engine)

    inspector = sa.inspect(conn)
    metadata = sa.MetaData(bind=conn, naming_convention=get_metadata_naming_convention(PG_NAMING_CONVENTION))
    for schema in schemas:
        metadata.reflect(only=_filter_reflect_datasets(inspector, schema, migration_config.datasets), schema=schema)

    handler = MigrationHandler()
    migration_ctx = PostgresqlMigrationContext(
        config=migration_config,
        inspector=inspector,
        handler=handler,
        rename=RenameMap(rename_src=migration_config.rename_src),
        cast_matrix=CastMatrix(backend.engine),
    )
    validate_rename_map(context, migration_ctx.rename, manifest)
    create_missing_schemas(
        context=context,
        manifest=manifest,
        handler=handler,
        schemas=schemas,
        datasets=migration_config.datasets,
    )

    mapped_model_tables = generate_model_tables_mapping(metadata, inspector, schemas, EXCLUDED_MODELS)

    models = commands.get_models(context, manifest)
    models, tables = _filter_models_and_tables(
        models=models,
        model_tables=mapped_model_tables,
        filtered_datasets=migration_config.datasets,
        rename=migration_ctx.rename,
    )

    sorted_models = sort_models_by_ref_and_base(list(models.values()))
    sorted_models_mapping = {model.model_type(): model for model in sorted_models}
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


def _filter_reflect_datasets(inspector: Inspector, schema: str, datasets: list):
    if not datasets:
        return None

    all_tables = inspector.get_table_names(schema=schema)
    return [
        table
        for table in all_tables
        if any(
            part_of_dataset(inspector.get_table_comment(table, schema=schema)["text"], dataset) for dataset in datasets
        )
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
                if part_of_dataset(table_name, dataset_name):
                    filtered_names[table_name] = model_tables[table_name]
        model_tables = filtered_names

    remapped_tables = {}
    for name, table in model_tables.items():
        # Do not apply `get_pg_table_name`, since this will be done later on while zipping with `model_name_key`
        table_identifier = rename.to_new_table(name)
        table_name = table_identifier.logical_qualified_name
        if table_name not in models.keys():
            table_name = name
        remapped_tables[table_name] = table

    return models, remapped_tables
