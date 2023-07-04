from typing import Any

from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.helpers import get_pg_name, get_column_name
from spinta.backends.postgresql.helpers.migrate import MigrationHandler
from spinta.cli.inspect import zipitems
import spinta.backends.postgresql.helpers.migrate as ma

import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.dialects.postgresql import JSONB, BIGINT
from sqlalchemy.dialects import postgresql

from spinta.cli.migrate import MigrateMeta, MigrateRename
from spinta.types.datatype import DataType, Ref
from spinta.utils.schema import NotAvailable, NA

from alembic.migration import MigrationContext
from alembic.operations import Operations

from spinta import commands
from spinta.components import Context, Model, Property
from spinta.manifests.components import Manifest
from spinta.backends.postgresql.components import PostgreSQL


def _convert_tables_to_pg_name(names: list):
    for i, item in enumerate(names):
        names[i] = get_pg_name(item)


@commands.migrate.register(Context, Manifest, PostgreSQL, MigrateMeta)
def migrate(context: Context, manifest: Manifest, backend: PostgreSQL, migrate_meta: MigrateMeta):
    conn = context.get(f'transaction.{backend.name}')
    ctx = MigrationContext.configure(conn, opts={
        "as_sql": migrate_meta.plan
    })
    op = Operations(ctx)
    inspector = sa.inspect(conn)
    table_names = inspector.get_table_names()
    metadata = sa.MetaData(bind=conn)
    metadata.reflect()

    tables = []
    for table in table_names:
        tables.append(migrate_meta.rename.get_table_name(table))

    models = zipitems(
        tables,
        manifest.models.keys(),
        _model_name_key
    )
    handler = MigrationHandler()
    for items in models:
        for old_model, new_model in items:
            if old_model and old_model.endswith(TableType.CHANGELOG.value):
                continue

            old = NA
            if old_model:
                old = metadata.tables[migrate_meta.rename.get_old_table_name(old_model)]
            new = manifest.models.get(new_model) if new_model else new_model
            commands.migrate(context, backend, inspector, old, new, handler, migrate_meta.rename)
    _handle_foregin_key_constraints()
    with ctx.begin_transaction():
        handler.run_migrations(op)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, Model, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, old: sa.Table, new: Model, handler: MigrationHandler, rename: MigrateRename):
    columns = []
    table_name = rename.get_table_name(old.name)
    for column in old.columns:
        columns.append(rename.get_column_name(old.name, column.name))
    if table_name != old.name:
        handler.add_action(ma.RenameTableMigrationAction(old.name, table_name))
        remove_changelog_name = get_pg_name(f'{old.name}{TableType.CHANGELOG.value}')
        if inspector.has_table(remove_changelog_name):
            handler.add_action(ma.RenameTableMigrationAction(
                old_table_name=remove_changelog_name,
                new_table_name=get_pg_name(f'{table_name}{TableType.CHANGELOG.value}')
            ))

    props = zipitems(
        columns,
        new.properties.values(),
        _property_name_key
    )
    for items in props:
        for old_prop, new_prop in items:
            if new_prop and new_prop.name.startswith('_'):
                continue
            if old_prop:
                old_prop = old.columns.get(rename.get_old_column_name(old.name, old_prop))
            commands.migrate(context, backend, inspector, old, old_prop, new_prop, handler, rename)

    if new.unique:
        for val in new.unique:
            prop_list = []
            for prop in val:
                name = prop.name
                if isinstance(prop.dtype, Ref):
                    name = f'{name}.{prop.dtype.refprops[0].name}'
                prop_list.append(get_pg_name(name))
            constraints = inspector.get_unique_constraints(old.name)
            constraint_name = f'{old.name}_{"_".join(prop_list)}_key'
            if old.name != table_name:
                if any(constraint['name'] == constraint_name for constraint in constraints):
                    handler.add_action(ma.DropConstraintMigrationAction(
                        table_name=table_name,
                        constraint_name=constraint_name
                    ))
                constraint_name = f'{table_name}_{"_".join(prop_list)}_key'
            if not any(constraint['name'] == constraint_name for constraint in constraints):
                handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                    table_name=table_name,
                    constraint_name=constraint_name,
                    columns=prop_list
                ))


@commands.migrate.register(Context, PostgreSQL, Inspector, NotAvailable, Model, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, old: NotAvailable, new: Model, handler: MigrationHandler, rename: MigrateRename):
    table_name = get_pg_name(get_table_name(new))

    pkey_type = commands.get_primary_key_type(context, backend)

    columns = []
    for prop in new.properties.values():
        if not prop.name.startswith("_"):
            cols = commands.prepare(context, backend, prop)
            if isinstance(cols, list):
                for column in cols:
                    if isinstance(column, sa.Column):
                        columns.append(column)
            else:
                if isinstance(cols, sa.Column):
                    columns.append(cols)

    handler.add_action(ma.CreateTableMigrationAction(
        table_name=table_name,
        columns=[
            sa.Column('_txn', pkey_type, index=True),
            sa.Column('_created', sa.DateTime),
            sa.Column('_updated', sa.DateTime),
            sa.Column("_id", pkey_type, nullable=False, primary_key=True),
            sa.Column("_revision", sa.Text),
            *columns
        ]

    ))

    if new.unique:
        for val in new.unique:
            prop_list = []
            for prop in val:
                name = prop.name
                if isinstance(prop.dtype, Ref):
                    name = f'{name}.{prop.dtype.refprops[0].name}'
                prop_list.append(get_pg_name(name))
            constraint_name = f'{table_name}_{"_".join(prop_list)}_key'
            handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                table_name=table_name,
                constraint_name=constraint_name,
                columns=prop_list
            ))

    _create_changelog_table(context, new, handler, rename)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, NotAvailable, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, old: sa.Table, new: NotAvailable, handler: MigrationHandler, rename: MigrateRename):
    old_name = old.name
    remove_name = _get_remove_name(old_name)

    if not old_name.split("/")[-1].startswith("__"):
        if inspector.has_table(remove_name):
            handler.add_action(ma.DropTableMigrationAction(
                table_name=remove_name
            ))
            remove_changelog_name = get_pg_name(f'{remove_name}{TableType.CHANGELOG.value}')
            if inspector.has_table(remove_changelog_name):
                handler.add_action(ma.DropTableMigrationAction(
                    table_name=remove_changelog_name
                ))

        handler.add_action(ma.RenameTableMigrationAction(
            old_table_name=old_name,
            new_table_name=remove_name
        ))
        _drop_all_indexes_and_constraints(inspector, old_name, remove_name, handler)

        old_changelog_name = get_pg_name(f'{old_name}{TableType.CHANGELOG.value}')
        new_changelog_name = get_pg_name(f'{remove_name}{TableType.CHANGELOG.value}')
        if inspector.has_table(old_changelog_name):
            handler.add_action(ma.RenameTableMigrationAction(
                old_table_name=old_changelog_name,
                new_table_name=new_changelog_name
            ))
            _drop_all_indexes_and_constraints(inspector, old_changelog_name, new_changelog_name, handler)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, sa.Column, Property, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: sa.Column, new: Property, handler: MigrationHandler, rename: MigrateRename):
    commands.migrate(context, backend, inspector, table, old, new.dtype, handler, rename)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, NotAvailable, Property, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: NotAvailable, new: Property, handler: MigrationHandler, rename: MigrateRename):
    commands.migrate(context, backend, inspector, table, old, new.dtype, handler, rename)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, sa.Column, NotAvailable, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: sa.Column, new: NotAvailable, handler: MigrationHandler, rename: MigrateRename):
    if not old.name.startswith("_"):
        table_name = rename.get_table_name(table.name)
        columns = inspector.get_columns(table.name)
        remove_name = get_pg_name(f'__{old.name}')
        if remove_name in columns:
            handler.add_action(ma.DropColumnMigrationAction(
                table_name=table_name,
                column_name=remove_name
            ))
        handler.add_action(ma.AlterColumnMigrationAction(
            table_name=table_name,
            column_name=old.name,
            new_column_name=remove_name
        ))

        if old.unique:
            unique_constraints = inspector.get_unique_constraints(table_name=table.name)
            for constraint in unique_constraints:
                if old.name in constraint["column_names"]:
                    handler.add_action(ma.DropConstraintMigrationAction(
                        table_name=table_name,
                        constraint_name=constraint["name"],
                    ))


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, NotAvailable, Ref, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: NotAvailable, new: Ref, handler: MigrationHandler, rename: MigrateRename):
    columns = commands.prepare(context, backend, new.prop)
    table_name = rename.get_table_name(table.name)
    if not isinstance(columns, list):
        columns = [columns]
    for column in columns:
        if isinstance(column, sa.Column):
            handler.add_action(
                ma.AddColumnMigrationAction(
                    table_name=table_name,
                    column=column
                )
            )


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, sa.Column, Ref, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: sa.Column, new: Ref, handler: MigrationHandler, rename: MigrateRename):
    print(old)
    print(new)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, NotAvailable, DataType, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: NotAvailable, new: DataType, handler: MigrationHandler, rename: MigrateRename):
    column = commands.prepare(context, backend, new.prop)
    table_name = rename.get_table_name(table.name)
    handler.add_action(ma.AddColumnMigrationAction(
        table_name=table_name,
        column=column,
    ))


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, sa.Column, DataType, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: sa.Column, new: DataType, handler: MigrationHandler, rename: MigrateRename):
    column = commands.prepare(context, backend, new.prop)

    column_name = rename.get_column_name(table.name, old.name)
    table_name = rename.get_table_name(table.name)

    new_type = column.type.compile(dialect=postgresql.dialect())
    old_type = old.type.compile(dialect=postgresql.dialect())

    nullable = column.nullable if column.nullable != old.nullable else None
    type_ = column.type if new_type != old_type else None
    new_name = column_name if old.name != get_pg_name(get_column_name(new.prop)) else None

    if nullable is not None or type_ is not None or new_name is not None:
        handler.add_action(ma.AlterColumnMigrationAction(
            table_name=table_name,
            column_name=old.name,
            nullable=nullable,
            type_=type_,
            new_column_name=new_name
        ))

    unique_name = f'{table_name}_{column_name}_key'
    old_unique_name = f'{table.name}_{old.name}_key'
    unique_constraints = inspector.get_unique_constraints(table_name=table.name)
    if any(constraint["name"] == old_unique_name for constraint in unique_constraints):
        if not new.unique or table_name != table.name:
            handler.add_action(ma.DropConstraintMigrationAction(
                constraint_name=old_unique_name,
                table_name=table_name
            ))
            if new.unique:
                handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                    constraint_name=unique_name,
                    table_name=table_name,
                    columns=[column_name]
                ))

    else:
        if new.unique:
            handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                constraint_name=unique_name,
                table_name=table_name,
                columns=[column_name]
            ))


def _drop_all_indexes_and_constraints(inspector: Inspector, table: str, new_table: str, handler: MigrationHandler):
    constraints = inspector.get_unique_constraints(table)
    removed = []
    for constraint in constraints:
        removed.append(constraint["name"])
        handler.add_action(
            ma.DropConstraintMigrationAction(
                table_name=new_table,
                constraint_name=constraint["name"]
            )
        )
    indexes = inspector.get_indexes(table)
    for index in indexes:
        if index["name"] not in removed:
            handler.add_action(
                ma.DropIndexMigrationAction(
                    table_name=new_table,
                    index_name=index["name"]
                )
            )


def _handle_rename_indexes(inspector: Inspector, op: Operations, table: str, old_table: str, new_table: str,
                           is_changelog: bool = False):
    primary_key_constraint = inspector.get_pk_constraint(table)
    _rename_primary_key_constraint(primary_key_constraint, op, table, old_table, new_table, is_changelog)

    unique_constraint = inspector.get_unique_constraints(table)
    _rename_unique_constraint(unique_constraint, op, table, old_table, new_table, is_changelog)

    indexes = inspector.get_indexes(table)
    _rename_index(indexes, op, table, old_table, new_table, is_changelog)


def _rename_index(indexes: list, op: Operations, table: str, old_table: str, new_table: str, is_changelog: bool):
    for index in indexes:
        if "duplicates_constraint" not in index.keys():
            op.drop_index(index['name'], table_name=table)
            new_name = index['name'].split("/")
            if is_changelog:
                new_name[-2] = new_name[-2].replace(old_table.split("/")[-1], new_table.split("/")[-1], 1)
            else:
                new_name[-1] = new_name[-1].replace(old_table.split("/")[-1], new_table.split("/")[-1], 1)
            new_name = '/'.join(new_name)
            op.create_index(new_name, table, columns=index['column_names'], unique=index['unique'])


def _rename_primary_key_constraint(constraint: dict, op: Operations, table: str, old_table: str, new_table: str,
                                   is_changelog: bool):
    op.drop_constraint(constraint['name'], table_name=table)
    new_name = constraint['name'].split("/")
    if is_changelog:
        new_name[-2] = new_name[-2].replace(old_table.split("/")[-1], new_table.split("/")[-1], 1)
    else:
        new_name[-1] = new_name[-1].replace(old_table.split("/")[-1], new_table.split("/")[-1], 1)
    new_name = '/'.join(new_name)
    op.create_primary_key(new_name, table, constraint["constrained_columns"])


def _rename_unique_constraint(constraints: list, op: Operations, table: str, old_table: str, new_table: str,
                              is_changelog: bool):
    for constraint in constraints:
        op.drop_constraint(constraint['name'], table_name=table)
        new_name = constraint['name'].split("/")
        if is_changelog:
            new_name[-2] = new_name[-2].replace(old_table.split("/")[-1], new_table.split("/")[-1], 1)
        else:
            new_name[-1] = new_name[-1].replace(old_table.split("/")[-1], new_table.split("/")[-1], 1)
        new_name = '/'.join(new_name)
        op.create_unique_constraint(new_name, table, constraint["column_names"])


def _handle_foregin_key_constraints():
    pass


def _create_changelog_table(context: Context, new: Model, handler: MigrationHandler, rename: MigrateRename):
    table_name = get_pg_name(get_table_name(new, TableType.CHANGELOG))
    pkey_type = commands.get_primary_key_type(context, new.backend)
    handler.add_action(ma.CreateTableMigrationAction(
        table_name=table_name,
        columns=[
            sa.Column('_id', BIGINT, primary_key=True),
            sa.Column('_revision', sa.String),
            sa.Column('_txn', pkey_type, index=True),
            sa.Column('_rid', pkey_type),
            sa.Column('datetime', sa.DateTime),
            sa.Column('action', sa.String(8)),
            sa.Column('data', JSONB)
        ]
    ))


def _model_name_key(model: str) -> str:
    return get_pg_name(model)


def _property_name_key(prop: Any) -> str:
    name = prop
    if isinstance(prop, Property):
        name = get_column_name(prop)
    return get_pg_name(name).split(".")[0]


def _get_remove_name(name: str) -> str:
    new_name = name.split("/")
    if not new_name[-1].startswith("__"):
        new_name[-1] = f'__{new_name[-1]}'
    new_name = '/'.join(new_name)
    new_name = get_pg_name(new_name)
    return new_name
