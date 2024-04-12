from typing import List

import sqlalchemy as sa

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_column_name
from spinta.backends.postgresql.helpers.migrate.migrate import name_key, MigratePostgresMeta, is_internal_ref, \
    adjust_kwargs, is_name_complex, extract_literal_name_from_column, generate_type_missmatch_exception_details
from spinta.components import Context
from spinta.datasets.inspect.helpers import zipitems
from spinta.exceptions import MigrateScalarToRefTooManyKeys, MigrateScalarToRefTypeMissmatch
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
            commands.migrate(context, backend, meta, table, old, column, **adjust_kwargs(kwargs, 'foreign_key', True))


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

    adjusted_kwargs = adjust_kwargs(kwargs, 'foreign_key', True)

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
        name = rename.get_column_name(table.name, item.name)
        if len(base_name) > 1:
            name = f'{rename.get_column_name(table.name, base_name[0])}.{rename.get_column_name(old_ref_table, base_name[1])}'
        old_names[name] = item

    for item in new_columns:
        new_names[item.name] = item

    if is_internal_ref(new):
        column = new_columns[0]
        if len(old) == 1 and old[0].name == f'{old_prop_name}._id':
            old_column = old[0]
            commands.migrate(context, backend, meta, table, old_column, column, **adjusted_kwargs)
        else:
            column_list = dict()
            drop_list = []
            for old_column in old:
                new_name = rename.get_column_name(table.name, old_column.name)
                key = new_name.split('.')[-1]

                # Check for scalar -> ref 4, can only accept 1 primary key
                if key == new_name:
                    model_external = new.model.external
                    key = '_id'
                    if model_external and not model_external.unknown_primary_key and model_external.pkeys:
                        if len(model_external.pkeys) > 1:
                            raise MigrateScalarToRefTooManyKeys(new, primary_keys=[pkey.name for pkey in model_external.pkeys])
                        target = model_external.pkeys[0]
                        target_column = commands.prepare(context, backend, target)
                        key = target.name
                        old_type = extract_literal_name_from_column(old_column)
                        new_type = extract_literal_name_from_column(target_column)
                        if old_type != new_type:
                            raise MigrateScalarToRefTypeMissmatch(new,
                                  details=generate_type_missmatch_exception_details(
                                      [
                                          (
                                              (old_column.name, old_type),
                                              (key, new_type)
                                          )
                                      ]
                                  ))

                column_list[key] = sa.Column(new_name, old_column.type)
                if old_column.name != f'{old_prop_name}._id':
                    drop_list.append(old_column)

            inspector_columns = inspector.get_columns(table.name)
            old_col = NA
            if any(f'{old_prop_name}._id' == col["name"] for col in inspector_columns):
                old_col = table.c.get(f'{old_prop_name}._id')
            commands.migrate(context, backend, meta, table, old_col, column, **adjusted_kwargs)
            if old_names.keys() != new_names.keys():
                handler.add_action(ma.UpgradeTransferDataMigrationAction(
                    table_name=table_name,
                    foreign_table_name=get_table_name(new.refprops[0]),
                    target=column,
                    columns=column_list
                ), True)
            for col in drop_list:
                commands.migrate(context, backend, meta, table, col, NA, **adjusted_kwargs)
    else:
        old_name_with_id = f'{old_prop_name}._id'
        # Might going to need to add better check to see if its transform or rename
        # This will prob not work with nested structure
        if len(old) == 1 and (
            old[0].name == old_name_with_id or
            not is_name_complex(old[0].name)
        ):
            column = old[0]
            requires_drop = True
            if len(new_columns) > 1 and column.name != old_name_with_id:
                raise MigrateScalarToRefTooManyKeys(new, primary_keys=[col.name.split('.')[-1] for col in new_columns])

            target = '_id'

            # Scalar to ref checks and adjustments
            if not column.name.endswith('._id') and len(new_columns) == 1 and isinstance(new_columns[0], sa.Column):
                target_column = new_columns[0]
                target = target_column.name.split('.')[-1]
                old_type = extract_literal_name_from_column(column)
                new_type = extract_literal_name_from_column(target_column)
                if old_type != new_type:
                    raise MigrateScalarToRefTypeMissmatch(new, details=generate_type_missmatch_exception_details(
                        [
                            (
                                (column.name, old_type),
                                (target_column.name, new_type)
                            )
                        ]
                    ))

            for new_column in new_columns:
                if isinstance(new_column, sa.Column):
                    if new_column.name == old_name_with_id:
                        requires_drop = False
                        commands.migrate(context, backend, meta, table, old[0], new_column, **adjusted_kwargs)
                    else:
                        commands.migrate(context, backend, meta, table, NA, new_column, **adjusted_kwargs)
            column_list = dict()

            for item in new_columns:
                name = item.name
                column_list[name] = sa.Column(name.split('.')[-1], item.type)
            if old_names.keys() != new_names.keys():
                handler.add_action(
                    ma.DowngradeTransferDataMigrationAction(
                        table_name=table_name,
                        foreign_table_name=get_table_name(new.refprops[0]),
                        source=column,
                        columns=column_list,
                        target=target
                    ), True
                )
            if requires_drop:
                commands.migrate(context, backend, meta, table, old[0], NA, **adjusted_kwargs)
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
                        commands.migrate(context, backend, meta, table, old_prop, new_prop, **adjusted_kwargs)
            for drop in drop_list:
                commands.migrate(context, backend, meta, table, drop, NA, **adjusted_kwargs)
