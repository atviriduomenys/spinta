import sqlalchemy as sa

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.migrate import MigratePostgresMeta
from spinta.components import Context, Property
from spinta.utils.schema import NotAvailable


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, list, Property)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: list, new: Property, **kwargs):
    commands.migrate(context, backend, meta, table, old, new.dtype, **kwargs)


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, sa.Column, Property)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: sa.Column, new: Property, **kwargs):
    commands.migrate(context, backend, meta, table, old, new.dtype, **kwargs)


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, NotAvailable, Property)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: NotAvailable, new: Property, **kwargs):
    commands.migrate(context, backend, meta, table, old, new.dtype, **kwargs)
