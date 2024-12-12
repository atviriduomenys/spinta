from typing import List

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.migrate import MigratePostgresMeta, MigrateModelMeta, json_has_key, \
    adjust_kwargs
from spinta.backends.postgresql.helpers.name import name_changed, get_pg_removed_name, get_pg_table_name
from spinta.components import Context
from spinta.types.text.components import Text
from spinta.utils.nestedstruct import get_last_attr
from spinta.utils.schema import NA


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, list, Text)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: List[sa.Column], new: Text, model_meta: MigrateModelMeta = None, **kwargs):
    rename = meta.rename
    handler = meta.handler
    column: sa.Column = commands.prepare(context, backend, new.prop)
    columns = old.copy()

    adjusted_kwargs = adjust_kwargs(kwargs, {
        "model_meta": model_meta
    })

    table_name = get_pg_table_name(rename.get_table_name(table.name))

    # Affected keys, are keys that have already been processed, since
    # scalar -> text conversion is done separately as well as lang rename
    # we need to make sure that we only remove / add langs that have not yet been processed
    affected_keys = []

    # Search for json column, to see if one existed beforehand
    # Remove it from columns list, to handle it separately at the end
    json_column = None
    for col in old:
        if isinstance(col.type, JSONB):
            json_column = col
            columns.remove(json_column)
            break

    # By default, json columns are all removed, if you find it, then handle removal manually
    json_column_meta = None
    if json_column is not None and model_meta is not None:
        if json_column.name in model_meta.json_columns:
            json_column_meta = model_meta.json_columns[json_column.name]
            json_column_meta.full_remove = False

    # Add empty jsonb column, if it was not found
    if json_column_meta is None and json_column is None:
        commands.migrate(context, backend, meta, table, NA, column, **adjusted_kwargs)

    # Handle scalar -> text conversion
    for item in columns.copy():
        full_name = rename.get_column_name(table.name, item.name)
        key = get_last_attr(full_name)

        if json_column is not None:
            if json_has_key(backend, json_column, table, key):
                renamed_key = get_pg_removed_name(key)
                if json_has_key(backend, json_column, table, renamed_key):
                    handler.add_action(
                        ma.RemoveJSONAttributeMigrationAction(table_name, json_column, renamed_key)
                    )
                handler.add_action(
                    ma.RenameJSONAttributeMigrationAction(table_name, json_column, key, renamed_key)
                )

        col = json_column if json_column is not None else column
        handler.add_action(
            ma.TransferColumnDataToJSONMigrationAction(table_name, col, [
                (key, item)
            ])
        )
        affected_keys.append(key)
        commands.migrate(context, backend, meta, table, item, NA, **adjusted_kwargs)
        columns.remove(item)

    # Handle lang rename, remove and add
    if json_column is not None and json_column_meta and not json_column_meta.empty:
        missing_keys = set(json_column_meta.keys).symmetric_difference(set(new.langs.keys()))

        # Handle renaming of keys
        for key in missing_keys:
            if key not in affected_keys:
                formatted_name = f'{json_column.name}@{key}'
                new_name = rename.get_column_name(table.name, formatted_name)
                if name_changed(formatted_name, new_name):
                    extracted_key = get_last_attr(new_name)
                    json_column_meta.add_new_key(
                        key, extracted_key
                    )
                    affected_keys.append(extracted_key)
        missing_keys -= set(affected_keys)
        for missing_key in missing_keys:
            if missing_key not in json_column_meta.keys:
                handler.add_action(ma.AddEmptyAttributeToJSONMigrationAction(
                    table=table_name,
                    source=json_column,
                    key=missing_key
                ))
            elif missing_key not in json_column_meta.new_keys:
                json_column_meta.add_new_key(missing_key, get_pg_removed_name(missing_key))

    # Handle column rename, which will be run at the end
    if json_column is not None and name_changed(json_column.name, column.name):
        if json_column_meta:
            json_column_meta.new_name = column.name
        else:
            commands.migrate(context, backend, meta, table, json_column, column, **adjusted_kwargs)


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, sa.Column, Text)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: sa.Column, new: Text, **kwargs):
    commands.migrate(context, backend, meta, table, [old], new, **kwargs)
