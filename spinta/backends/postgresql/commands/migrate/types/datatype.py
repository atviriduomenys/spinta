from typing import List

import geoalchemy2.types
import sqlalchemy as sa

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.migrate import MigratePostgresMeta, \
    adjust_kwargs, extract_literal_name_from_column, handle_unique_constraint_migration, contains_unique_constraint, \
    handle_index_migration, extract_using_from_columns, MigrateModelMeta, contains_constraint_name, \
    constraint_with_columns, extract_sqlalchemy_columns, reduce_columns
from spinta.backends.postgresql.helpers.name import name_changed, get_pg_table_name, get_pg_constraint_name, \
    get_pg_removed_name, get_pg_index_name
from spinta.components import Context
from spinta.types.datatype import DataType
from spinta.utils.itertools import ensure_list
from spinta.utils.schema import NotAvailable, NA


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, NotAvailable, DataType)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: NotAvailable, new: DataType, **kwargs):
    columns = commands.prepare(context, backend, new.prop)
    columns = ensure_list(columns)
    columns = extract_sqlalchemy_columns(columns)
    columns = reduce_columns(columns)
    if columns is not None and columns != []:
        commands.migrate(context, backend, meta, table, old, columns, **kwargs)


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, sa.Column, DataType)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: sa.Column, new: DataType, **kwargs):
    columns = commands.prepare(context, backend, new.prop)
    columns = ensure_list(columns)
    columns = extract_sqlalchemy_columns(columns)
    columns = reduce_columns(columns)
    commands.migrate(context, backend, meta, table, old, columns, **kwargs)


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, list, DataType)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: List[sa.Column], new: DataType, **kwargs):
    rename = meta.rename

    column: sa.Column = commands.prepare(context, backend, new.prop)
    old_name = rename.get_old_column_name(table.name, column.name)

    columns = old.copy()

    if name_changed(column.name, old_name):
        for col in old:
            if col.name == column.name:
                commands.migrate(context, backend, meta, table, col, NA, **kwargs)
                columns.remove(col)
                break

    for col in columns:
        commands.migrate(context, backend, meta, table, col, column, **kwargs)


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, sa.Column, sa.Column)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: sa.Column, new: sa.Column, model_meta: MigrateModelMeta, foreign_key: bool = False, **kwargs):
    rename = meta.rename
    inspector = meta.inspector
    handler = meta.handler

    column_name = new.name
    table_name = get_pg_table_name(rename.get_table_name(table.name))
    old_type = extract_literal_name_from_column(old)
    new_type = extract_literal_name_from_column(new)

    nullable = new.nullable if new.nullable != old.nullable else None
    type_ = new.type if old_type != new_type else None
    new_name = column_name if old.name != new.name else None

    using = extract_using_from_columns(
        old,
        new,
        type_
    )

    if nullable is not None or type_ is not None or new_name is not None or using is not None:
        handler.add_action(ma.AlterColumnMigrationAction(
            table_name=table_name,
            column_name=old.name,
            nullable=nullable,
            type_=type_,
            new_column_name=new_name,
            using=using,
        ), foreign_key)

    is_renamed = name_changed(table.name, table_name, old.name, new.name)
    # Order has to be UniqueConstraint -> Index
    # because UniqueConstraint also contain Unique Index
    # we mark them as handled if they are part of UniqueConstraint
    handle_unique_constraint_migration(
        table,
        table_name,
        old,
        new,
        column_name,
        handler,
        inspector,
        foreign_key,
        is_renamed,
        meta=model_meta
    )
    handle_index_migration(
        table,
        table_name,
        old,
        new,
        column_name,
        handler,
        inspector,
        foreign_key,
        is_renamed,
        meta=model_meta
    )


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, NotAvailable, sa.Column)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: NotAvailable, new: sa.Column, model_meta: MigrateModelMeta, foreign_key: bool = False, **kwargs):
    rename = meta.rename
    inspector = meta.inspector
    handler = meta.handler

    table_name = get_pg_table_name(rename.get_table_name(table.name))
    handler.add_action(ma.AddColumnMigrationAction(
        table_name=table_name,
        column=new,
    ), foreign_key)

    new_column_name = new.name
    if new.unique:
        constraint_name = get_pg_constraint_name(table_name, [new_column_name])
        unique_constraints = inspector.get_unique_constraints(table_name=table_name)
        model_meta.handle_unique_constraint(constraint_name)
        if not contains_unique_constraint(unique_constraints, new_column_name):
            handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                constraint_name=constraint_name,
                table_name=table_name,
                columns=[new_column_name]
            ))
        elif not contains_constraint_name(unique_constraints, constraint_name):
            constraint = constraint_with_columns(unique_constraints, [new_column_name])
            if constraint:
                model_meta.handle_unique_constraint(constraint['name'])
                handler.add_action(ma.RenameConstraintMigrationAction(
                    table_name=table_name,
                    old_constraint_name=constraint['name'],
                    new_constraint_name=constraint_name
                ))

    index_required = isinstance(new.type, geoalchemy2.types.Geometry)
    if index_required:
        index_name = get_pg_index_name(table_name, new_column_name)
        model_meta.handle_index(index_name)
        handler.add_action(ma.CreateIndexMigrationAction(
            table_name=table_name,
            columns=[new_column_name],
            index_name=index_name,
            using='GIST'
        ))


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, list, NotAvailable)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: list, new: NotAvailable, foreign_key: bool = False, **kwargs):
    for item in old:
        commands.migrate(context, backend, meta, table, item, new, **adjust_kwargs(kwargs, {
            'foreign_key': foreign_key
        }))


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, sa.Column, NotAvailable)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: sa.Column, new: NotAvailable, model_meta: MigrateModelMeta, foreign_key: bool = False, **kwargs):
    if old.name.startswith("_"):
        return

    rename = meta.rename
    inspector = meta.inspector
    handler = meta.handler

    table_name = get_pg_table_name(rename.get_table_name(table.name))
    columns = inspector.get_columns(table.name)
    remove_name = get_pg_removed_name(old.name)

    if any(remove_name == column["name"] for column in columns):
        handler.add_action(ma.DropColumnMigrationAction(
            table_name=table_name,
            column_name=remove_name
        ), foreign_key)
    handler.add_action(ma.AlterColumnMigrationAction(
        table_name=table_name,
        column_name=old.name,
        new_column_name=remove_name
    ), foreign_key)
    indexes = inspector.get_indexes(table_name=table.name)
    for index in indexes:
        if index["column_names"] == [old.name]:
            model_meta.handle_index(index["name"])
            handler.add_action(ma.DropIndexMigrationAction(
                table_name=table_name,
                index_name=index["name"],
            ), foreign_key)
    if old.unique:
        unique_constraints = inspector.get_unique_constraints(table_name=table.name)
        for constraint in unique_constraints:
            if old.name in constraint["column_names"]:
                model_meta.handle_unique_constraint(constraint["name"])
                handler.add_action(ma.DropConstraintMigrationAction(
                    table_name=table_name,
                    constraint_name=constraint["name"],
                ), foreign_key)


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, NotAvailable, list)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table, old: NotAvailable,
            new: List[sa.Column], foreign_key: bool = False, **kwargs):
    for column in new:
        commands.migrate(context, backend, meta, table, old, column, foreign_key=foreign_key, **kwargs)
