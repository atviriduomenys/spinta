import sqlalchemy as sa

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.migrate import PostgresqlMigrationContext, PropertyMigrationContext
from spinta.components import Context
from spinta.types.datatype import DataType
from spinta.types.geometry.components import Geometry


@commands.migrate.register(
    Context, PostgreSQL, PostgresqlMigrationContext, PropertyMigrationContext, sa.Table, sa.Column, Geometry
)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    property_ctx: PropertyMigrationContext,
    table: sa.Table,
    old: sa.Column,
    new: DataType,
    **kwargs,
):
    columns = commands.prepare(context, backend, new.prop)
    col = None
    for column in columns:
        if isinstance(column, sa.Column):
            col = column
            break
    commands.migrate(context, backend, migration_ctx, property_ctx, table, old, col, **kwargs)
