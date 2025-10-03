from multipledispatch import dispatch

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.actions import MigrationHandler
from spinta.backends.postgresql.helpers.migrate.migrate import create_table_migration
from spinta.components import Context, Property
from spinta.types.datatype import DataType, File, Array


@dispatch(Context, PostgreSQL, Property, MigrationHandler)
def prepare_migration_columns(context: Context, backend: PostgreSQL, prop: Property, handler: MigrationHandler) -> list:
    return prepare_migration_columns(context, backend, prop.dtype, handler)


@dispatch(Context, PostgreSQL, DataType, MigrationHandler)
def prepare_migration_columns(
    context: Context, backend: PostgreSQL, dtype: DataType, handler: MigrationHandler
) -> list:
    return commands.prepare(context, backend, dtype)


@dispatch(Context, PostgreSQL, File, MigrationHandler)
def prepare_migration_columns(context: Context, backend: PostgreSQL, dtype: File, handler: MigrationHandler) -> list:
    columns = commands.prepare(context, backend, dtype)
    table = backend.get_table(dtype.prop, TableType.FILE)
    create_table_migration(handler, table)
    return columns


@dispatch(Context, PostgreSQL, Array, MigrationHandler)
def prepare_migration_columns(context: Context, backend: PostgreSQL, dtype: Array, handler: MigrationHandler) -> list:
    columns = commands.prepare(context, backend, dtype)
    table = backend.get_table(dtype.prop, TableType.LIST)
    create_table_migration(handler, table)
    return columns
