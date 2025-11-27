import sqlalchemy as sa

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.migrate import (
    PostgresqlMigrationContext,
    ModelMigrationContext,
    PropertyMigrationContext,
)
from spinta.components import Context, Property
from spinta.utils.schema import NotAvailable


@commands.migrate.register(Context, PostgreSQL, PostgresqlMigrationContext, ModelMigrationContext, list, Property)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    model_ctx: ModelMigrationContext,
    old: list,
    new: Property,
    **kwargs,
):
    commands.migrate(
        context,
        backend,
        migration_ctx,
        PropertyMigrationContext(prop=new, model_context=model_ctx),
        old,
        new.dtype,
        **kwargs,
    )


@commands.migrate.register(Context, PostgreSQL, PostgresqlMigrationContext, ModelMigrationContext, sa.Column, Property)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    model_ctx: ModelMigrationContext,
    old: sa.Column,
    new: Property,
    **kwargs,
):
    commands.migrate(
        context,
        backend,
        migration_ctx,
        PropertyMigrationContext(prop=new, model_context=model_ctx),
        old,
        new.dtype,
        **kwargs,
    )


@commands.migrate.register(
    Context, PostgreSQL, PostgresqlMigrationContext, ModelMigrationContext, NotAvailable, Property
)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    model_ctx: ModelMigrationContext,
    old: NotAvailable,
    new: Property,
    **kwargs,
):
    commands.migrate(
        context,
        backend,
        migration_ctx,
        PropertyMigrationContext(prop=new, model_context=model_ctx),
        old,
        new.dtype,
        **kwargs,
    )
