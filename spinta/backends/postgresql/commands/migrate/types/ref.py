from typing import List

import sqlalchemy as sa

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_column_name
from spinta.backends.postgresql.helpers.migrate.migrate import name_key, MigratePostgresMeta
from spinta.components import Context
from spinta.datasets.inspect.helpers import zipitems
from spinta.types.datatype import Ref
from spinta.utils.schema import NotAvailable, NA


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, NotAvailable, Ref)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: NotAvailable, new: Ref, **kwargs):
    columns = commands.prepare(context, backend, new.prop)
    if not isinstance(columns, list):
        columns = [columns]
    for column in columns:
        if isinstance(column, sa.Column):
            commands.migrate(context, backend, meta, table, old, column, foreign_key=True, **kwargs)


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, sa.Column, Ref)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: sa.Column, new: Ref, **kwargs):
    commands.migrate(context, backend, meta, table, [old], new, **kwargs)


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, list, Ref)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: List[sa.Column], new: Ref, **kwargs):
    rename = meta.rename
    inspector = meta.inspector
    handler = meta.handler

    new_columns = commands.prepare(context, backend, new.prop)
    if not isinstance(new_columns, list):
        new_columns = [new_columns]
    table_name = rename.get_table_name(table.name)
    old_ref_table = rename.get_old_table_name(get_table_name(new.model))
    old_prop_name = rename.get_old_column_name(table.name, get_column_name(new.prop))

    old_names = {}
    new_names = {}
    for item in old:
        base_name = item.name.split(".")
        name = f'{rename.get_column_name(table.name, base_name[0])}.{rename.get_column_name(old_ref_table, base_name[1])}'
        old_names[name] = item
    for item in new_columns:
        new_names[item.name] = item

    if not new.prop.level or new.prop.level > 3:
        if len(old) == 1 and old[0].name == f'{old_prop_name}._id':
            column = old[0]
            new_column = new_columns[0]
            commands.migrate(context, backend, meta, table, column, new_column, foreign_key=True, **kwargs)
        else:
            column_list = []
            drop_list = []
            for column in old:
                column_list.append(rename.get_column_name(table.name, column.name))
                if column.name != f'{old_prop_name}._id':
                    drop_list.append(column)

            inspector_columns = inspector.get_columns(table.name)
            old_col = NA
            if any(f'{old_prop_name}._id' == col["name"] for col in inspector_columns):
                old_col = table.c.get(f'{old_prop_name}._id')
            commands.migrate(context, backend, meta, table, old_col, new_columns[0], foreign_key=True, **kwargs)
            if old_names.keys() != new_names.keys():
                handler.add_action(ma.UpgradeTransferDataMigrationAction(
                    table_name=table_name,
                    foreign_table_name=get_table_name(new.refprops[0]),
                    columns=column_list
                ), True)
            for col in drop_list:
                commands.migrate(context, backend, meta, table, col, NA, foreign_key=True, **kwargs)
    else:
        if len(old) == 1 and old[0].name == f'{old_prop_name}._id':
            requires_drop = True
            for new_column in new_columns:
                if isinstance(new_column, sa.Column):
                    if new_column.name == f'{old_prop_name}._id':
                        requires_drop = False
                        commands.migrate(context, backend, meta, table, old[0], new_column, foreign_key=True, **kwargs)
                    else:
                        commands.migrate(context, backend, meta, table, NA, new_column, foreign_key=True, **kwargs)
            names = []
            for item in new_columns:
                names.append(item.name)
            if old_names.keys() != new_names.keys():
                handler.add_action(
                    ma.DowngradeTransferDataMigrationAction(
                        table_name=table_name,
                        foreign_table_name=get_table_name(new.refprops[0]),
                        columns=names
                    ), True
                )
            if requires_drop:
                commands.migrate(context, backend, meta, table, old[0], NA, foreign_key=True, **kwargs)
        else:
            props = zipitems(
                old_names.keys(),
                new_names.keys(),
                name_key
            )
            drop_list = []
            for prop in props:
                for old_prop, new_prop in prop:
                    if old_prop is not NA:
                        old_prop = old_names[old_prop]
                    if new_prop is not NA:
                        new_prop = new_names[new_prop]
                    if old_prop is not None and new_prop is None:
                        drop_list.append(old_prop)
                    else:
                        commands.migrate(context, backend, meta, table, old_prop, new_prop, foreign_key=True, **kwargs)
            for drop in drop_list:
                commands.migrate(context, backend, meta, table, drop, NA, foreign_key=True, **kwargs)
