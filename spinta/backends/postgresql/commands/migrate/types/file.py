import sqlalchemy as sa

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.migrate import (
    get_root_attr,
    PostgresqlMigrationContext,
    PropertyMigrationContext,
    create_table_migration,
    gather_prepare_columns,
    get_source_table,
    update_primary_key,
)
from spinta.backends.postgresql.helpers.name import name_changed
from spinta.components import Context
from spinta.types.datatype import File
from spinta.utils.schema import NotAvailable


@commands.migrate.register(
    Context, PostgreSQL, PostgresqlMigrationContext, PropertyMigrationContext, NotAvailable, File
)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    property_ctx: PropertyMigrationContext,
    old: NotAvailable,
    new: File,
    **kwargs,
):
    inspector = migration_ctx.inspector
    handler = migration_ctx.handler
    columns = gather_prepare_columns(context, backend, new.prop)

    target_file_table = backend.get_table(new.prop, TableType.FILE)
    target_file_identifier = migration_ctx.get_table_identifier(property_ctx.prop, TableType.FILE)
    for column in columns:
        if not isinstance(column, sa.Column):
            continue

        handler.add_action(ma.AddColumnMigrationAction(table_identifier=target_file_identifier, column=column))

    if not inspector.has_table(target_file_identifier.pg_table_name, schema=target_file_identifier.pg_schema_name):
        create_table_migration(target_file_table, handler, table_identifier=target_file_identifier)


@commands.migrate.register(Context, PostgreSQL, PostgresqlMigrationContext, PropertyMigrationContext, list, File)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    property_ctx: PropertyMigrationContext,
    old: list,
    new: File,
    **kwargs,
):
    rename = migration_ctx.rename
    inspector = migration_ctx.inspector
    handler = migration_ctx.handler

    source_table = get_source_table(property_ctx, old)
    target_table_identifier = migration_ctx.get_table_identifier(property_ctx.prop)

    column_name = rename.to_new_column_name(source_table, new.prop.name)
    old_name = rename.to_old_column_name(source_table, new.prop.name)
    for item in old:
        nullable = new.required if new.required == item.nullable else None
        item_name = item.name
        new_name = (
            item_name.replace(get_root_attr(item_name), column_name) if not item_name.startswith(column_name) else None
        )
        if nullable is not None or new_name is not None:
            handler.add_action(
                ma.AlterColumnMigrationAction(
                    table_identifier=target_table_identifier,
                    column_name=item.name,
                    nullable=nullable,
                    new_column_name=new_name,
                    comment=new_name,
                )
            )

    source_file_table = property_ctx.model_context.model_tables.property_tables.get(old_name)
    if source_file_table and source_file_table[0] is TableType.FILE:
        source_file_identifier = migration_ctx.get_table_identifier(source_file_table[1])
    else:
        source_file_identifier = None

    target_file_table = backend.get_table(new.prop, TableType.FILE)
    target_file_identifier = migration_ctx.get_table_identifier(new.prop, TableType.FILE)
    if (
        source_file_identifier is not None
        and inspector.has_table(source_file_identifier.pg_table_name, schema=source_file_identifier.pg_schema_name)
        and name_changed(
            old_name, column_name, source_file_identifier.pg_qualified_name, target_file_identifier.pg_qualified_name
        )
    ):
        handler.add_action(
            ma.RenameTableMigrationAction(
                old_table_identifier=source_file_identifier,
                new_table_identifier=target_file_identifier,
                comment=target_file_table.comment,
            )
        )
        update_primary_key(
            inspector=inspector,
            source_table_identifier=source_file_identifier,
            target_table_identifier=target_file_identifier,
            handler=handler,
        )
