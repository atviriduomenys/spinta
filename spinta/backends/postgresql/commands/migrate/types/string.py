from typing import List

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.migrate import (
    json_has_key,
    get_root_attr,
    jsonb_keys,
    PostgresqlMigrationContext,
    PropertyMigrationContext,
)
from spinta.backends.postgresql.helpers.name import name_changed, get_pg_removed_name, get_pg_table_name
from spinta.components import Context
from spinta.types.datatype import String
from spinta.types.text.helpers import determine_langauge_for_text
from spinta.utils.nestedstruct import get_last_attr
from spinta.utils.schema import NA


@commands.migrate.register(
    Context, PostgreSQL, PostgresqlMigrationContext, PropertyMigrationContext, sa.Table, list, String
)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    property_ctx: PropertyMigrationContext,
    table: sa.Table,
    old: List[sa.Column],
    new: String,
    **kwargs,
):
    rename = migration_ctx.rename
    handler = migration_ctx.handler

    column: sa.Column = commands.prepare(context, backend, new.prop)
    old_name = rename.get_old_column_name(table.name, column.name, root_only=False)
    columns = old.copy()

    table_name = get_pg_table_name(rename.get_table_name(table.name))

    json_column = None
    for col in old:
        if isinstance(col.type, JSONB):
            json_column = col
            break

    json_column_meta = None
    if json_column is not None:
        if json_column.name in property_ctx.model_context.json_columns:
            json_column_meta = property_ctx.model_context.json_columns[json_column.name]
        else:
            json_column_meta = property_ctx.model_context.create_json_context(backend, json_column, new.prop)
    requires_removal = True
    # Check if column was renamed and if there already existed column of the new name
    # If it did, remove it
    if name_changed(column.name, old_name):
        if json_column is not None:
            key = get_last_attr(old_name)
            if json_column_meta and json_column_meta.keys:
                contains_key = key in json_column_meta.keys
            else:
                contains_key = json_has_key(backend, json_column, table, key)
            requires_removal = contains_key

            if not contains_key:
                columns.remove(json_column)

        if requires_removal:
            for col in old:
                if not name_changed(col.name, column.name):
                    commands.migrate(context, backend, migration_ctx, property_ctx, table, col, NA, **kwargs)
                    columns.remove(col)
                    break

    for col in columns.copy():
        if col.name != column.name and not isinstance(col.type, JSONB):
            name = rename.get_old_column_name(table.name, col.name)
            if name != col.name:
                name = get_root_attr(name)
                if json_column is not None and name == json_column.name:
                    columns.remove(col)

    if len(columns) <= 1:
        col = columns[0] if len(columns) == 1 else NA

        # Check if it's text -> string, or just normal string migration
        if col != NA and isinstance(col.type, JSONB):
            old_name = rename.get_old_column_name(table.name, column.name, root_only=False)
            key = get_last_attr(old_name)

            # No key was extracted
            cast_json_to_string = key == old_name
            if cast_json_to_string:
                default_langs = context.get("config").languages
                if json_column_meta and json_column_meta.keys:
                    all_keys = json_column_meta.keys
                else:
                    all_keys = jsonb_keys(backend, column, table)
                key = determine_langauge_for_text(all_keys, [], default_langs)
                json_column_meta.cast_to = (column, key)
            else:
                commands.migrate(context, backend, migration_ctx, property_ctx, table, NA, column, **kwargs)
                handler.add_action(ma.TransferJSONDataMigrationAction(table_name, col, columns=[(key, column)]))
                renamed_key = get_pg_removed_name(key)
                if json_column_meta is None:
                    if json_has_key(backend, col, table, renamed_key):
                        handler.add_action(ma.RemoveJSONAttributeMigrationAction(table_name, col, renamed_key))
                    handler.add_action(ma.RenameJSONAttributeMigrationAction(table_name, col, key, renamed_key))
                else:
                    json_column_meta.add_new_key(key, renamed_key)
        else:
            commands.migrate(context, backend, migration_ctx, property_ctx, table, col, column, **kwargs)
    else:
        raise Exception("String cannot migrate to multiple columns")


@commands.migrate.register(
    Context, PostgreSQL, PostgresqlMigrationContext, PropertyMigrationContext, sa.Table, sa.Column, String
)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    property_ctx: PropertyMigrationContext,
    table: sa.Table,
    old: sa.Column,
    new: String,
    **kwargs,
):
    commands.migrate(context, backend, migration_ctx, property_ctx, table, [old], new, **kwargs)
