from collections.abc import Generator

import sqlalchemy as sa
from tqdm import tqdm

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_identifier
from spinta.backends.postgresql.components import PostgreSQL
from spinta.cli.helpers.script.helpers import ensure_store_is_loaded
from spinta.components import Context, Model


def cli_requires_changelog_compressed_migration(context: Context, **kwargs) -> bool:
    ensure_store_is_loaded(context)
    return next(collect_models_without_changelog_compressed(context), None) is not None


def collect_models_without_changelog_compressed(context: Context) -> Generator[Model, None, None]:
    store = context.get("store")
    inspectors = {}

    for model in commands.get_models(context, store.manifest).values():
        backend = model.backend
        if not isinstance(backend, PostgreSQL):
            continue

        if backend.name not in inspectors:
            inspectors[backend.name] = sa.inspect(backend.engine)
        inspector = inspectors[backend.name]
        table_identifier = get_table_identifier(model, TableType.CHANGELOG)

        if not inspector.has_table(table_identifier.pg_table_name, schema=table_identifier.pg_schema_name):
            continue

        columns = inspector.get_columns(table_identifier.pg_table_name, schema=table_identifier.pg_schema_name)
        if not any(column["name"] == "_compressed" for column in columns):
            yield model


def migrate_changelog_compressed(context: Context, **kwargs) -> None:
    from alembic.operations import Operations
    from alembic.runtime.migration import MigrationContext

    ensure_store_is_loaded(context)
    affected_models = list(collect_models_without_changelog_compressed(context))

    for model in tqdm(affected_models, desc="Adding changelog compression columns"):
        table_identifier = get_table_identifier(model, TableType.CHANGELOG)
        with model.backend.begin() as conn:
            operations = Operations(MigrationContext.configure(conn))
            operations.add_column(
                table_identifier.pg_table_name,
                sa.Column("_compressed", sa.DateTime, nullable=True, comment="_compressed"),
                schema=table_identifier.pg_schema_name,
            )
