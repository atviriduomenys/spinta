import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import BIGINT, ARRAY

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.constants import BackendFeatures, TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.migrate import (
    get_root_attr,
    PostgresqlMigrationContext,
    PropertyMigrationContext,
    create_table_migration,
)
from spinta.backends.postgresql.helpers.name import name_changed, get_pg_column_name
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

    name = new.prop.name
    nullable = not new.required

    target_table = backend.get_table(property_ctx.prop)

    table_name = target_table.name
    pkey_type = commands.get_primary_key_type(context, new.backend)
    handler.add_action(
        ma.AddColumnMigrationAction(
            table_name=table_name, column=sa.Column(get_pg_column_name(f"{name}._id"), sa.String, nullable=nullable)
        )
    )
    handler.add_action(
        ma.AddColumnMigrationAction(
            table_name=table_name,
            column=sa.Column(get_pg_column_name(f"{name}._content_type"), sa.String, nullable=nullable),
        )
    )
    handler.add_action(
        ma.AddColumnMigrationAction(
            table_name=table_name, column=sa.Column(get_pg_column_name(f"{name}._size"), BIGINT, nullable=nullable)
        )
    )
    if BackendFeatures.FILE_BLOCKS in new.backend.features:
        handler.add_action(
            ma.AddColumnMigrationAction(
                table_name=table_name,
                column=sa.Column(get_pg_column_name(f"{name}._bsize"), sa.Integer, nullable=nullable),
            )
        )
        handler.add_action(
            ma.AddColumnMigrationAction(
                table_name=table_name,
                column=sa.Column(
                    get_pg_column_name(f"{name}._blocks"),
                    ARRAY(
                        pkey_type,
                    ),
                    nullable=nullable,
                ),
            )
        )

    target_file_table = backend.get_table(new.prop, TableType.FILE)
    if not inspector.has_table(target_file_table.name):
        create_table_migration(target_file_table, handler)


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

    source_table = property_ctx.model_context.model_tables.main_table
    target_table = backend.get_table(property_ctx.prop)
    table_name = target_table.name

    column_name = rename.get_column_name(source_table, new.prop.name)
    old_name = rename.get_old_column_name(source_table, new.prop.name)
    for item in old:
        nullable = new.required if new.required == item.nullable else None
        item_name = item.name
        new_name = (
            item_name.replace(get_root_attr(item_name), column_name) if not item_name.startswith(column_name) else None
        )
        if nullable is not None or new_name is not None:
            handler.add_action(
                ma.AlterColumnMigrationAction(
                    table_name=table_name,
                    column_name=item.name,
                    nullable=nullable,
                    new_column_name=new_name,
                    comment=new_name,
                )
            )

    source_file_table = property_ctx.model_context.model_tables.property_tables.get(old_name)
    if source_file_table and source_file_table[0] is TableType.FILE:
        source_file_table = source_file_table[1]
    else:
        source_file_table = None

    target_file_table = backend.get_table(new.prop, TableType.FILE)

    if (
        source_file_table is not None
        and inspector.has_table(source_file_table.name)
        and name_changed(old_name, column_name, source_file_table.name, target_file_table.name)
    ):
        handler.add_action(
            ma.RenameTableMigrationAction(
                old_table_name=source_file_table.name,
                new_table_name=target_file_table.name,
                comment=target_file_table.comment,
            )
        )
