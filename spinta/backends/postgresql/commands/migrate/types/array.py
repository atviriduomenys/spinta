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
    get_source_table,
    index_with_columns,
    index_not_handled_condition,
    constraint_with_foreign_key_columns,
    update_primary_key,
)
from spinta.backends.postgresql.helpers.name import (
    get_pg_column_name,
    name_changed,
    get_pg_index_name,
    get_pg_foreign_key_name,
)
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

    target_table_identifier = migration_ctx.get_table_identifier(property_ctx.prop)

    columns = gather_prepare_columns(context, backend, new.prop)
    for column in columns:
        handler.add_action(ma.AddColumnMigrationAction(table_identifier=target_table_identifier, column=column))

    array_table = backend.get_table(new.prop, TableType.LIST)
    array_table_identifier = migration_ctx.get_table_identifier(new.prop, TableType.LIST)
    if not inspector.has_table(array_table_identifier.pg_table_name, schema=array_table_identifier.pg_schema_name):
        create_table_migration(array_table, handler=handler, table_identifier=array_table_identifier)


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
    source_table_identifier = migration_ctx.get_table_identifier(source_table)
    target_table_identifier = migration_ctx.get_table_identifier(property_ctx.prop)

    column_name = rename.to_new_column_name(source_table, new.prop.place)
    old_name = rename.to_old_column_name(source_table, new.prop.place)

    pg_column_name = get_pg_column_name(column_name)
    # Rename models json column
    if name_changed(old.name, pg_column_name):
        handler.add_action(
            ma.AlterColumnMigrationAction(
                table_identifier=target_table_identifier,
                column_name=old.name,
                new_column_name=column_name,
                comment=pg_column_name,
            )
        )

    source_array_table = property_ctx.model_context.model_tables.property_tables.get(old_name)
    if source_array_table and source_array_table[0] is TableType.LIST:
        source_array_table = source_array_table[1]
        source_array_identifier = migration_ctx.get_table_identifier(source_array_table)
    else:
        source_array_table = None
        source_array_identifier = None

    target_array_table = backend.get_table(new.prop, TableType.LIST)
    target_array_identifier = migration_ctx.get_table_identifier(new.prop, TableType.LIST)
    table_name_changed = (
        source_array_table is not None
        and inspector.has_table(source_array_table.name, schema=source_array_table.schema)
        and name_changed(
            old_name, column_name, source_array_identifier.pg_qualified_name, target_array_identifier.pg_qualified_name
        )
    )
    # Rename list table
    if table_name_changed:
        handler.add_action(
            ma.RenameTableMigrationAction(
                old_table_identifier=source_array_identifier,
                new_table_identifier=target_array_identifier,
                comment=target_array_table.comment,
            )
        )
        update_primary_key(
            inspector=inspector,
            source_table_identifier=source_array_identifier,
            target_table_identifier=target_array_identifier,
            handler=handler,
        )

    if source_array_table is not None:
        # reserved _txn column index
        indexes = inspector.get_indexes(source_array_table.name, schema=source_array_table.schema)
        existing_index = index_with_columns(
            indexes,
            ["_txn"],
            condition=index_not_handled_condition(
                property_ctx.model_context, source_array_identifier.logical_qualified_name
            ),
        )
        index_name = get_pg_index_name(target_array_table.name, ["_txn"])
        if existing_index is None:
            handler.add_action(
                ma.CreateIndexMigrationAction(
                    table_identifier=target_array_identifier, index_name=index_name, columns=["_txn"]
                )
            )
        else:
            property_ctx.model_context.mark_index_handled(
                source_array_identifier.logical_qualified_name, existing_index["name"]
            )
            if table_name_changed:
                handler.add_action(
                    ma.RenameIndexMigrationAction(
                        old_index_name=existing_index["name"],
                        new_index_name=index_name,
                        table_identifier=target_array_identifier,
                        old_table_identifier=source_array_identifier,
                    )
                )

        constraints = inspector.get_foreign_keys(source_array_table.name, schema=source_array_table.schema)
        existing_constraint = constraint_with_foreign_key_columns(constraints, source_table_identifier, ["_rid"])
        constraint_name = get_pg_foreign_key_name(
            table_identifier=target_array_identifier,
            referred_table_identifier=target_table_identifier,
            column_name="_rid",
        )
        if existing_constraint is None:
            handler.add_action(
                ma.CreateForeignKeyMigrationAction(
                    source_table_identifier=target_array_identifier,
                    referent_table_identifier=target_table_identifier,
                    local_cols=["_rid"],
                    remote_cols=["_id"],
                    constraint_name=constraint_name,
                )
            )
        else:
            source_constraint_name = existing_constraint["name"]
            property_ctx.model_context.mark_foreign_constraint_handled(
                source_array_identifier.logical_qualified_name, source_constraint_name
            )
            if table_name_changed:
                handler.add_action(
                    ma.RenameConstraintMigrationAction(
                        table_identifier=target_array_identifier,
                        old_constraint_name=source_constraint_name,
                        new_constraint_name=constraint_name,
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
