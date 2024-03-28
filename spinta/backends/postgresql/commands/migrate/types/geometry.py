import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.actions import MigrationHandler
from spinta.cli.migrate import MigrateRename
from spinta.components import Context
from spinta.types.datatype import DataType
from spinta.types.geometry.components import Geometry


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, sa.Column, Geometry, MigrationHandler,
                           MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: sa.Column, new: DataType, handler: MigrationHandler, rename: MigrateRename):
    columns = commands.prepare(context, backend, new.prop)
    col = None
    for column in columns:
        if isinstance(column, sa.Column):
            col = column
            break
    commands.migrate(context, backend, inspector, table, old, col, handler, rename, False)
