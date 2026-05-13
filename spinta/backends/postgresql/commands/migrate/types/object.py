from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.migrate import (
    PostgresqlMigrationContext,
    PropertyMigrationContext,
    zip_and_migrate_properties,
    get_source_table,
)
from spinta.components import Context
from spinta.types.datatype import Object
from spinta.utils.itertools import ensure_list
from spinta.utils.schema import NotAvailable

import sqlalchemy as sa


@commands.migrate.register(
    Context, PostgreSQL, PostgresqlMigrationContext, PropertyMigrationContext, NotAvailable, Object
)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    property_ctx: PropertyMigrationContext,
    old: NotAvailable,
    new: Object,
    **kwargs,
):
    for prop in new.properties.values():
        commands.migrate(context, backend, migration_ctx, property_ctx.model_context, old, prop, **kwargs)


@commands.migrate.register(
    Context, PostgreSQL, PostgresqlMigrationContext, PropertyMigrationContext, (list, sa.Column), Object
)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    property_ctx: PropertyMigrationContext,
    old: list | sa.Column,
    new: Object,
    **kwargs,
):
    old = ensure_list(old)
    source_table = get_source_table(property_ctx, old)
    zip_and_migrate_properties(
        context=context,
        backend=backend,
        source_table=source_table,
        model=new.prop.model,
        old_columns=old,
        new_properties=list(new.properties.values()),
        migration_context=migration_ctx,
        model_context=property_ctx.model_context,
        root_name=new.prop.place,
        **kwargs,
    )
