from typing import List

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.migrate import MigratePostgresMeta, MigrateModelMeta, json_has_key, \
    has_been_renamed
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

    # Search for json column, to see if one existed beforehand
    # Remove if from columns list, to handle it separately at the end
    json_column = None
    for col in old:
        if isinstance(col.type, JSONB):
            json_column = col
            columns.remove(json_column)
            break

    json_column_meta = None
    if json_column is not None and model_meta is not None:
        if json_column.name in model_meta.json_columns:
            json_column_meta = model_meta.json_columns[json_column.name]
            json_column_meta.full_remove = False

    if json_column_meta is None and json_column is None:
        commands.migrate(context, backend, meta, table, NA, column, model_meta=model_meta, foreign_key=False, **kwargs)

    for item in columns.copy():
        full_name = rename.get_column_name(table.name, item.name)
        key = get_last_attr(full_name)

        if json_column is not None:
            if json_has_key(backend, json_column, table, key):
                renamed_key = f'__{key}'
                if json_has_key(backend, json_column, table, renamed_key):
                    handler.add_action(
                        ma.RemoveJSONAttributeMigrationAction(table, json_column, renamed_key)
                    )
                handler.add_action(
                    ma.RenameJSONAttributeMigrationAction(table, json_column, key, renamed_key)
                )

        col = json_column if json_column is not None else column
        handler.add_action(
            ma.TransferColumnDataToJSONMigrationAction(table, col, [
                (key, item)
            ])
        )
        commands.migrate(context, backend, meta, table, item, NA, model_meta=model_meta, **kwargs)
        columns.remove(item)

    if json_column is not None:
        if json_column_meta:
            missing_keys = set(json_column_meta.keys).symmetric_difference(set(new.langs.keys()))
            for missing_key in missing_keys:
                if missing_key not in json_column_meta.new_keys:
                    json_column_meta.add_new_key(missing_key, f'__{missing_key}')

    # Rename column
    if json_column is not None and has_been_renamed(json_column.name, column.name):
        json_column_meta.new_name = column.name


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, sa.Column, Text)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: sa.Column, new: Text, **kwargs):
    commands.migrate(context, backend, meta, table, [old], new, **kwargs)
