import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.actions import MigrationHandler
from spinta.cli.migrate import MigrateRename
from spinta.components import Context, Property
from spinta.utils.schema import NotAvailable


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, list, Property, MigrationHandler,
                           MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: list, new: Property, handler: MigrationHandler, rename: MigrateRename):
    commands.migrate(context, backend, inspector, table, old, new.dtype, handler, rename)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, sa.Column, Property, MigrationHandler,
                           MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: sa.Column, new: Property, handler: MigrationHandler, rename: MigrateRename):
    commands.migrate(context, backend, inspector, table, old, new.dtype, handler, rename)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, NotAvailable, Property, MigrationHandler,
                           MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: NotAvailable, new: Property, handler: MigrationHandler, rename: MigrateRename):
    commands.migrate(context, backend, inspector, table, old, new.dtype, handler, rename)
