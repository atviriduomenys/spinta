import sqlalchemy as sa

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.migrate import (
    PostgresqlMigrationContext,
    PropertyMigrationContext,
    create_table_migration,
    gather_prepare_columns,
    get_target_table,
    get_source_table,
)
from spinta.backends.postgresql.helpers.name import get_pg_column_name, name_changed
from spinta.components import Context
from spinta.types.datatype import Array
from spinta.utils.schema import NotAvailable


@commands.migrate.register(
    Context, PostgreSQL, PostgresqlMigrationContext, PropertyMigrationContext, NotAvailable, Array
)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    property_ctx: PropertyMigrationContext,
    old: NotAvailable,
    new: Array,
    **kwargs,
):
    inspector = migration_ctx.inspector
    handler = migration_ctx.handler

    target_table = backend.get_table(property_ctx.prop)
    table_name = target_table.name

    columns = gather_prepare_columns(context, backend, new.prop)
    for column in columns:
        handler.add_action(ma.AddColumnMigrationAction(table_name=table_name, column=column))

    array_table = backend.get_table(new.prop, TableType.LIST)
    if not inspector.has_table(array_table.name):
        create_table_migration(array_table, handler=handler)


@commands.migrate.register(Context, PostgreSQL, PostgresqlMigrationContext, PropertyMigrationContext, sa.Column, Array)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    property_ctx: PropertyMigrationContext,
    old: sa.Column,
    new: Array,
    **kwargs,
):
    rename = migration_ctx.rename
    inspector = migration_ctx.inspector
    handler = migration_ctx.handler

    source_table = get_source_table(property_ctx, old)
    target_table = get_target_table(backend, property_ctx.prop)
    table_name = target_table.name

    column_name = rename.get_column_name(source_table, new.prop.place)
    old_name = rename.get_old_column_name(source_table, new.prop.place)

    pg_column_name = get_pg_column_name(column_name)
    if name_changed(old.name, pg_column_name):
        handler.add_action(
            ma.AlterColumnMigrationAction(
                table_name=table_name,
                column_name=old.name,
                new_column_name=column_name,
                comment=pg_column_name,
            )
        )

    source_array_table = property_ctx.model_context.model_tables.property_tables.get(old_name)
    if source_array_table and source_array_table[0] is TableType.LIST:
        source_array_table = source_array_table[1]
    else:
        source_array_table = None

    target_array_table = backend.get_table(new.prop, TableType.LIST)

    if (
        source_array_table is not None
        and inspector.has_table(source_array_table.name)
        and name_changed(old_name, column_name, source_array_table.name, target_array_table.name)
    ):
        handler.add_action(
            ma.RenameTableMigrationAction(
                old_table_name=source_array_table.name,
                new_table_name=target_array_table.name,
                comment=target_array_table.comment,
            )
        )
        if source_array_table.columns.get(old_name):
            handler.add_action(
                ma.AlterColumnMigrationAction(
                    table_name=target_array_table.name,
                    column_name=old_name,
                    new_column_name=new.prop.place,
                )
            )

    columns = {value.comment: value for value in gather_prepare_columns(context, backend, new.items)}
    target_columns = dict(target_array_table.columns)
    reserved_columns = {key: value for key, value in target_columns.items() if key not in columns}

    source_property_columns = [
        value
        for key, value in source_array_table.columns.items()
        if key not in reserved_columns and not key.startswith("__")
    ]
    commands.migrate(
        context,
        backend,
        migration_ctx,
        property_ctx.model_context,
        source_property_columns,
        new.items,
        **kwargs,
    )
