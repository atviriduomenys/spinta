from typing import List

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.migrate import MigratePostgresMeta, MigrateModelMeta
from spinta.components import Context
from spinta.types.text.components import Text
from spinta.utils.schema import NA


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, list, Text)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: List[sa.Column], new: Text, model_meta: MigrateModelMeta = None, **kwargs):
    rename = meta.rename
    handler = meta.handler

    column: sa.Column = commands.prepare(context, backend, new.prop)
    old_name = rename.get_old_column_name(table.name, column.name, root_only=False)
    columns = old.copy()

    json_column = None
    for col in old:
        if isinstance(col.type, JSONB):
            json_column = col
            break

    json_column_meta = None
    if json_column is not None and model_meta is not None:
        if json_column.name in model_meta.json_columns:
            json_column_meta = model_meta.json_columns[json_column.name]

            # By default, all json columns are removed at the end if there are types that inherit json then do not remove it
            if json_column_meta.full_remove:
                json_column_meta.full_remove = False

    # contains_key = False
    # requires_removal = True
    #
    # # Check if column was renamed and if there already existed column of the new name
    # # If it did, remove it
    # if column.name != old_name:
    #     if json_column is not None:
    #         key = get_last_attr(old_name)
    #         if json_column_meta and json_column_meta.keys:
    #             contains_key = key in json_column_meta.keys
    #         else:
    #             contains_key = json_has_key(backend, json_column, table, key)
    #         requires_removal = contains_key
    #
    #         if not contains_key:
    #             columns.remove(json_column)
    #
    #     if requires_removal:
    #         for col in old:
    #             if col.name == column.name:
    #                 commands.migrate(context, backend, meta, table, col, NA, foreign_key=False, **kwargs)
    #                 columns.remove(col)
    #                 break
    #
    # for col in columns.copy():
    #     if col.name != column.name and not isinstance(col.type, JSONB):
    #         name = rename.get_old_column_name(table.name, col.name)
    #         if name != col.name:
    #             name = get_root_attr(name)
    #             if json_column is not None and name == json_column.name:
    #                 columns.remove(col)

    if len(columns) <= 1:
        col = columns[0] if len(columns) == 1 else NA
        commands.migrate(context, backend, meta, table, col, column, foreign_key=False, **kwargs)
    else:
        raise Exception("String cannot migrate to multiple columns")


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, sa.Column, Text)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: sa.Column, new: Text, **kwargs):
    commands.migrate(context, backend, meta, table, [old], new, **kwargs)
