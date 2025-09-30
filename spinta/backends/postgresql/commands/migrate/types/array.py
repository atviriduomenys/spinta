import sqlalchemy as sa

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.migrate import (
    PostgresqlMigrationContext,
    PropertyMigrationContext,
    create_table_migration,
)
from spinta.components import Context
from spinta.types.datatype import Array
from spinta.utils.schema import NotAvailable


@commands.migrate.register(
    Context, PostgreSQL, PostgresqlMigrationContext, PropertyMigrationContext, sa.Table, NotAvailable, Array
)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    property_ctx: PropertyMigrationContext,
    table: sa.Table,
    old: NotAvailable,
    new: Array,
    **kwargs,
):
    columns = commands.prepare(context, backend, new.prop)
    array_table = backend.get_table(new.prop, TableType.LIST)
    handler = migration_ctx.handler
    create_table_migration(handler, array_table)

    if not isinstance(columns, list):
        columns = [columns]
    for column in columns:
        if isinstance(column, sa.Column):
            commands.migrate(context, backend, migration_ctx, property_ctx, table, old, column, **kwargs)


#
# @commands.migrate.register(
#     Context, PostgreSQL, PostgresqlMigrationContext, PropertyMigrationContext, sa.Table, NotAvailable, Array
# )
# def migrate(
#     context: Context,
#     backend: PostgreSQL,
#     migration_ctx: PostgresqlMigrationContext,
#     property_ctx: PropertyMigrationContext,
#     table: sa.Table,
#     old: NotAvailable,
#     new: Array,
#     **kwargs,
# ):
#     columns = commands.prepare(context, backend, new.prop)
#     array_table = backend.get_table(new.prop, TableType.LIST)
#     handler = migration_ctx.handler
#     create_table_migration(handler, array_table)
#
#     if not isinstance(columns, list):
#         columns = [columns]
#     for column in columns:
#         if isinstance(column, sa.Column):
#             commands.migrate(context, backend, migration_ctx, property_ctx, table, old, column, **kwargs)
