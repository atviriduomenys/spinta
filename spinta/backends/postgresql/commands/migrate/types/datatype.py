from typing import List

import geoalchemy2.types
import sqlalchemy as sa

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_name
from spinta.backends.postgresql.helpers.migrate.migrate import check_if_renamed, rename_index_name, MigratePostgresMeta, \
    adjust_kwargs, extract_literal_name_from_column
from spinta.components import Context
from spinta.types.datatype import DataType
from spinta.utils.schema import NotAvailable, NA


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, NotAvailable, DataType)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: NotAvailable, new: DataType, **kwargs):
    column = commands.prepare(context, backend, new.prop)
    if column is not None and column != []:
        commands.migrate(context, backend, meta, table, old, column, **kwargs)


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, sa.Column, DataType)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: sa.Column, new: DataType, **kwargs):
    column = commands.prepare(context, backend, new.prop)
    commands.migrate(context, backend, meta, table, old, column, **kwargs)


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, list, DataType)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: List[sa.Column], new: DataType, **kwargs):
    rename = meta.rename

    column: sa.Column = commands.prepare(context, backend, new.prop)
    old_name = rename.get_old_column_name(table.name, column.name)

    columns = old.copy()

    if column.name != old_name:
        for col in old:
            if col.name == column.name:
                commands.migrate(context, backend, meta, table, col, NA, **kwargs)
                columns.remove(col)
                break

    for col in columns:
        commands.migrate(context, backend, meta, table, col, column, **kwargs)


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, sa.Column, sa.Column)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: sa.Column, new: sa.Column, foreign_key: bool = False, **kwargs):
    rename = meta.rename
    inspector = meta.inspector
    handler = meta.handler

    column_name = new.name
    table_name = rename.get_table_name(table.name)
    old_type = extract_literal_name_from_column(old)
    new_type = extract_literal_name_from_column(new)
    using = None

    requires_cast = True
    if isinstance(old.type, geoalchemy2.types.Geometry) and isinstance(new.type, geoalchemy2.types.Geometry):
        if old.type.srid != new.type.srid:
            srid_name = old
            srid = new.type.srid
            if old.type.srid == -1:
                srid_name = sa.func.ST_SetSRID(old, 4326)
            if new.type.srid == -1:
                srid = 4326
            using = sa.func.ST_Transform(srid_name, srid).compile(compile_kwargs={"literal_binds": True})
            requires_cast = False

    nullable = new.nullable if new.nullable != old.nullable else None
    type_ = new.type if old_type != new_type else None
    new_name = column_name if old.name != new.name else None

    if type_ and requires_cast:
        using = sa.func.cast(old, type_).compile(compile_kwargs={"literal_binds": True})

    if nullable is not None or type_ is not None or new_name is not None or using is not None:
        handler.add_action(ma.AlterColumnMigrationAction(
            table_name=table_name,
            column_name=old.name,
            nullable=nullable,
            type_=type_,
            new_column_name=new_name,
            using=using
        ), foreign_key)

    renamed = check_if_renamed(table.name, table_name, old.name, new.name)
    unique_name = get_pg_name(f'{table_name}_{column_name}_key')
    removed = []
    if renamed:
        unique_constraints = inspector.get_unique_constraints(table_name=table.name)
        for constraint in unique_constraints:
            if constraint["column_names"] == [old.name]:
                removed.append(constraint["name"])
                handler.add_action(ma.DropConstraintMigrationAction(
                    constraint_name=constraint["name"],
                    table_name=table_name
                ), foreign_key)
                unique_name = get_pg_name(
                    rename_index_name(constraint["name"], table.name, table_name, old.name, new.name))
                if new.unique:
                    handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                        constraint_name=unique_name,
                        table_name=table_name,
                        columns=[column_name]
                    ), foreign_key)
    else:
        if new.unique:
            if not any(constraint["column_names"] == [column_name] for constraint in
                       inspector.get_unique_constraints(table_name=table.name)) and not any(
                index["column_names"] == [column_name] and index["unique"] for index in
                inspector.get_indexes(table_name=table.name)):
                handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                    constraint_name=unique_name,
                    table_name=table_name,
                    columns=[column_name]
                ), foreign_key)
        else:
            for constraint in inspector.get_unique_constraints(table_name=table.name):
                if constraint["column_names"] == [column_name]:
                    # Check if old column was unique, if not add it to removed list, but don't actually remove it, other part of the code handles this case
                    if not old.unique:
                        removed.append(constraint["name"])
                        continue
                    removed.append(constraint["name"])
                    handler.add_action(ma.DropConstraintMigrationAction(
                        constraint_name=constraint["name"],
                        table_name=table_name,
                    ), foreign_key)
            for index in inspector.get_indexes(table_name=table.name):
                if index["column_names"] == [column_name] and index["unique"] and index["name"] not in removed:
                    index_name = index["name"]
                    handler.add_action(ma.DropIndexMigrationAction(
                        index_name=index_name,
                        table_name=table_name,
                    ), foreign_key)
                    handler.add_action(ma.CreateIndexMigrationAction(
                        index_name=index_name,
                        table_name=table_name,
                        columns=[column_name]
                    ), foreign_key)

    index_name = get_pg_name(f'ix_{table_name}_{column_name}')
    if renamed:
        indexes = inspector.get_indexes(table_name=table.name)
        for index in indexes:
            if index["column_names"] == [old.name] and index["name"] not in removed:
                index_name = get_pg_name(rename_index_name(index["name"], table.name, table_name, old.name, new.name))
                handler.add_action(
                    ma.DropIndexMigrationAction(
                        index_name=index["name"],
                        table_name=table_name
                    ), foreign_key)

                if new.index:
                    handler.add_action(ma.CreateIndexMigrationAction(
                        index_name=index_name,
                        table_name=table_name,
                        columns=[column_name]
                    ), foreign_key)
                elif isinstance(new.type, geoalchemy2.types.Geometry):
                    handler.add_action(ma.CreateIndexMigrationAction(
                        index_name=index_name,
                        table_name=table_name,
                        columns=[column_name],
                        using="gist"
                    ), foreign_key)
    else:
        if new.index:
            if not any(
                index["column_names"] == [column_name] for index in inspector.get_indexes(table_name=table.name)):
                handler.add_action(ma.CreateIndexMigrationAction(
                    index_name=index_name,
                    table_name=table_name,
                    columns=[column_name]
                ), foreign_key)
        elif isinstance(new.type, geoalchemy2.types.Geometry):
            if not any(
                index["column_names"] == [column_name] for index in inspector.get_indexes(table_name=table.name)):
                handler.add_action(ma.CreateIndexMigrationAction(
                    index_name=index_name,
                    table_name=table_name,
                    columns=[column_name],
                    using="gist"
                ), foreign_key)
        else:
            for index in inspector.get_indexes(table_name=table.name):
                if index["column_names"] == [column_name] and index["name"] not in removed:
                    if not (index["unique"] and new.unique):
                        handler.add_action(ma.DropIndexMigrationAction(
                            index_name=index["name"],
                            table_name=table_name,
                        ), foreign_key)


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, NotAvailable, sa.Column)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: NotAvailable, new: sa.Column, foreign_key: bool = False, **kwargs):
    rename = meta.rename
    inspector = meta.inspector
    handler = meta.handler

    table_name = get_pg_name(rename.get_table_name(table.name))
    handler.add_action(ma.AddColumnMigrationAction(
        table_name=table_name,
        column=new,
    ), foreign_key)
    if new.unique:
        constraint_name = get_pg_name(f'{table_name}_{new.name}_key')
        if not any(
            constraint["name"] == constraint_name for constraint in inspector.get_unique_constraints(table_name)):
            handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                constraint_name=constraint_name,
                table_name=table_name,
                columns=[new.name]
            ))
    index_required = isinstance(new.type, geoalchemy2.types.Geometry)
    if index_required:
        handler.add_action(ma.CreateIndexMigrationAction(
            table_name=table_name,
            columns=[new.name],
            index_name=get_pg_name(f'idx_{table_name}_{new.name}'),
            using='gist'
        ))


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, list, NotAvailable)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: list, new: NotAvailable, foreign_key: bool = False, **kwargs):
    for item in old:
        commands.migrate(context, backend, meta, table, item, new, **adjust_kwargs(kwargs, 'foreign_key', foreign_key))


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, sa.Column, NotAvailable)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: sa.Column, new: NotAvailable, foreign_key: bool = False, **kwargs):
    rename = meta.rename
    inspector = meta.inspector
    handler = meta.handler

    if not old.name.startswith("_"):
        table_name = rename.get_table_name(table.name)
        columns = inspector.get_columns(table.name)
        remove_name = get_pg_name(f'__{old.name}')

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
                handler.add_action(ma.DropIndexMigrationAction(
                    table_name=table_name,
                    index_name=index["name"],
                ), foreign_key)
        if old.unique:
            unique_constraints = inspector.get_unique_constraints(table_name=table.name)
            for constraint in unique_constraints:
                if old.name in constraint["column_names"]:
                    handler.add_action(ma.DropConstraintMigrationAction(
                        table_name=table_name,
                        constraint_name=constraint["name"],
                    ), foreign_key)
