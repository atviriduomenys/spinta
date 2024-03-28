from typing import List

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine.reflection import Inspector

import spinta.backends.postgresql.helpers.migrate as ma
from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.actions import MigrationHandler
from spinta.backends.postgresql.helpers.migrate.migrate import get_last_attr, json_has_key, get_root_attr, jsonb_keys
from spinta.cli.migrate import MigrateRename
from spinta.components import Context
from spinta.types.datatype import String
from spinta.types.text.helpers import determine_langauge_for_text
from spinta.utils.schema import NA


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, list, String, MigrationHandler,
                           MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: List[sa.Column], new: String, handler: MigrationHandler, rename: MigrateRename):
    column: sa.Column = commands.prepare(context, backend, new.prop)
    old_name = rename.get_old_column_name(table.name, column.name, root_only=False)
    columns = old.copy()

    json_column = None
    for col in old:
        if isinstance(col.type, JSONB):
            json_column = col
            break

    contains_key = False
    requires_removal = True

    # Check if column was renamed and if there already existed column of the new name
    # If it did, remove it
    print("STRING GIVEN", old, new.prop, column, old_name)
    if column.name != old_name:
        if json_column is not None:
            key = get_last_attr(old_name)
            contains_key = json_has_key(backend, json_column, table, key)
            requires_removal = contains_key

            if not contains_key:
                columns.remove(json_column)

        if requires_removal:
            for col in old:
                if col.name == column.name:
                    commands.migrate(context, backend, inspector, table, col, NA, handler, rename, False)
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
        if col != NA and isinstance(col.type, JSONB):
            old_name = rename.get_old_column_name(table.name, column.name)
            key = get_root_attr(old_name)

            # No key was extracted
            if key == old_name:
                default_langs = context.get('config').languages
                all_keys = jsonb_keys(backend, column, table)
                key = determine_langauge_for_text(all_keys, [], default_langs)

            commands.migrate(context, backend, inspector, table, NA, column, handler, rename, False)
            handler.add_action(
                ma.TransferJSONDataMigrationAction(table, col, columns=[
                    (key, column)
                ])
            )
            renamed_key = f'__{key}'
            if json_has_key(backend, col, table, renamed_key):
                handler.add_action(
                    ma.RemoveJSONAttributeMigrationAction(table, col, renamed_key)
                )
            handler.add_action(
                ma.RenameJSONAttributeMigrationAction(table, col, key, renamed_key)
            )
            print("CALLED MIGRATE TEXT String", column, col, new, old_name, key)
        else:
            commands.migrate(context, backend, inspector, table, col, column, handler, rename, False)
    else:
        raise Exception("String cannot migrate to multiple columns")


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, sa.Column, String, MigrationHandler,
                           MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: sa.Column, new: String, handler: MigrationHandler, rename: MigrateRename):
    commands.migrate(context, backend, inspector, table, [old], new, handler, rename)
