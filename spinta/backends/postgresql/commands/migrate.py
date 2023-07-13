from typing import Any, List

import geoalchemy2.types

from spinta.backends.components import BackendFeatures
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.helpers import get_pg_name, get_column_name
from spinta.backends.postgresql.helpers.migrate import MigrationHandler
from spinta.cli.inspect import zipitems
import spinta.backends.postgresql.helpers.migrate as ma

import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.dialects.postgresql import JSONB, BIGINT, ARRAY
from sqlalchemy.dialects import postgresql

from spinta.cli.migrate import MigrateMeta, MigrateRename
from spinta.types.datatype import DataType, Ref, File, Inherit
from spinta.utils.schema import NotAvailable, NA

from alembic.migration import MigrationContext
from alembic.operations import Operations

from spinta import commands
from spinta.components import Context, Model, Property
from spinta.manifests.components import Manifest
from spinta.backends.postgresql.components import PostgreSQL

EXCLUDED_MODELS = (
    'spatial_ref_sys'
)


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
        name = migrate_meta.rename.get_table_name(table)
        if name not in manifest.models.keys():
            name = table
        tables.append(name)

    models = zipitems(
        tables,
        manifest.models.keys(),
        _model_name_key
    )
    handler = MigrationHandler()
    for items in models:
        for old_model, new_model in items:
            if old_model and any(value in old_model for value in (TableType.CHANGELOG.value, TableType.FILE.value)):
                continue
            if old_model and old_model in EXCLUDED_MODELS:
                continue
            old = NA
            if old_model:
                old = metadata.tables[migrate_meta.rename.get_old_table_name(old_model)]
            new = manifest.models.get(new_model) if new_model else new_model
            commands.migrate(context, backend, inspector, old, new, handler, migrate_meta.rename)
    _handle_foreign_key_constraints(inspector, manifest, handler, migrate_meta.rename)
    _clean_up_file_type(inspector, manifest, handler, migrate_meta.rename)
    with ctx.begin_transaction():
        handler.run_migrations(op)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, Model, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, old: sa.Table, new: Model, handler: MigrationHandler, rename: MigrateRename):
    columns = []
    table_name = rename.get_table_name(old.name)

    # Handle table renames
    for column in old.columns:
        columns.append(rename.get_column_name(old.name, column.name))
    if table_name != old.name:
        handler.add_action(ma.RenameTableMigrationAction(old.name, table_name))
        changelog_name = get_pg_name(f'{old.name}{TableType.CHANGELOG.value}')
        file_name = get_pg_name(f'{old.name}{TableType.FILE.value}')
        if inspector.has_table(changelog_name):
            handler.add_action(ma.RenameTableMigrationAction(
                old_table_name=changelog_name,
                new_table_name=get_pg_name(f'{table_name}{TableType.CHANGELOG.value}')
            ))
        for table in inspector.get_table_names():
            if table.startswith(file_name):
                split = table.split(TableType.FILE.value)
                handler.add_action(ma.RenameTableMigrationAction(
                    old_table_name=file_name,
                    new_table_name=get_pg_name(f'{table_name}{TableType.FILE.value}{split[1]}')
                ))

    # Handle Refs (required because, level < 4 can create multiple columns) and Files
    properties = list(new.properties.values())
    for prop in new.properties.values():
        if isinstance(prop.dtype, (Ref, File)):
            items = []
            column_name = rename.get_old_column_name(old.name, get_column_name(prop))
            for column in old.columns:
                if column.name.startswith(column_name):
                    renamed = rename.get_column_name(old.name, column.name)
                    if renamed in columns:
                        columns.remove(renamed)
                    items.append(column)
            if prop in properties and items:
                properties.remove(prop)
                commands.migrate(context, backend, inspector, old, items, prop.dtype, handler, rename)
        elif isinstance(prop.dtype, Inherit):
            properties.remove(prop)

    props = zipitems(
        columns,
        properties,
        _property_name_key
    )
    for items in props:
        for old_prop, new_prop in items:
            if new_prop and new_prop.name.startswith('_'):
                continue
            if old_prop:
                if new_prop and old_prop == get_column_name(new_prop):
                    old_prop = old.columns.get(old_prop)
                else:
                    old_prop = old.columns.get(rename.get_old_column_name(old.name, old_prop))
            if not new_prop:
                commands.migrate(context, backend, inspector, old, old_prop, new_prop, handler, rename, False)
            else:
                commands.migrate(context, backend, inspector, old, old_prop, new_prop, handler, rename)

    required_unique_constraints = []
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
                constraint_name = f'{table_name}_{"_".join(prop_list)}_key'
            required_unique_constraints.append(constraint_name)
            if not any(constraint['name'] == constraint_name for constraint in constraints):
                handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                    table_name=table_name,
                    constraint_name=constraint_name,
                    columns=prop_list
                ))

    # Clean up old multi constraints for non required models (happens when model.ref is changed)
    # Solo unique constraints are handled with property migrate
    if not old.name.startswith("_"):
        unique_constraints = inspector.get_unique_constraints(old.name)
        for constraint in unique_constraints:
            if constraint["name"] not in required_unique_constraints and len(constraint["column_names"]) > 1:
                handler.add_action(ma.DropConstraintMigrationAction(
                    table_name=table_name,
                    constraint_name=constraint["name"]
                ))


@commands.migrate.register(Context, PostgreSQL, Inspector, NotAvailable, Model, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, old: NotAvailable, new: Model, handler: MigrationHandler, rename: MigrateRename):
    table_name = get_pg_name(get_table_name(new))

    pkey_type = commands.get_primary_key_type(context, backend)

    columns = []
    for prop in new.properties.values():
        if not prop.name.startswith("_"):
            if isinstance(prop.dtype, File):
                name = get_column_name(prop)
                nullable = not prop.dtype.required
                columns += [
                    sa.Column(f'{name}._id', sa.String, nullable=nullable),
                    sa.Column(f'{name}._content_type', sa.String, nullable=nullable),
                    sa.Column(f'{name}._size', BIGINT, nullable=nullable)
                ]
                if BackendFeatures.FILE_BLOCKS in prop.dtype.backend.features:
                    columns += [
                        sa.Column(f'{name}._bsize', sa.Integer, nullable=nullable),
                        sa.Column(f'{name}._blocks', ARRAY(pkey_type, ), nullable=nullable),
                    ]
                new_table = get_pg_name(f'{table_name}{TableType.FILE.value}/{name}')
                if not inspector.has_table(new_table):
                    handler.add_action(ma.CreateTableMigrationAction(
                        table_name=new_table,
                        columns=[
                            sa.Column('_id', pkey_type, primary_key=True),
                            sa.Column('_block', sa.LargeBinary)
                        ]
                    ))
            else:
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
            remove_file_name = get_pg_name(f'{remove_name}{TableType.FILE.value}')
            for table in inspector.get_table_names():
                if table.startswith(remove_file_name):
                    handler.add_action(ma.DropTableMigrationAction(
                        table_name=table
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
            handler.add_action(ma.RenameSequenceMigrationAction(
                old_name=f'{old_changelog_name}__id_seq',
                new_name=f'{new_changelog_name}__id_seq'
            ))
            _drop_all_indexes_and_constraints(inspector, old_changelog_name, new_changelog_name, handler)
        for table in inspector.get_table_names():
            if table.startswith(f'{old_name}{TableType.FILE.value}'):
                split = table.split(TableType.FILE.value)
                new_name = get_pg_name(f'{remove_name}{TableType.FILE.value}{split[1]}')
                handler.add_action(ma.RenameTableMigrationAction(
                    old_table_name=table,
                    new_table_name=new_name
                ))
                _drop_all_indexes_and_constraints(inspector, table, new_name, handler)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, sa.Column, Property, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: sa.Column, new: Property, handler: MigrationHandler, rename: MigrateRename):
    commands.migrate(context, backend, inspector, table, old, new.dtype, handler, rename)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, NotAvailable, Property, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: NotAvailable, new: Property, handler: MigrationHandler, rename: MigrateRename):
    commands.migrate(context, backend, inspector, table, old, new.dtype, handler, rename)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, NotAvailable, File, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: NotAvailable, new: File, handler: MigrationHandler, rename: MigrateRename):
    name = get_column_name(new.prop)
    nullable = not new.required
    table_name = rename.get_table_name(table.name)
    pkey_type = commands.get_primary_key_type(context, new.backend)
    handler.add_action(ma.AddColumnMigrationAction(
        table_name=table_name,
        column=sa.Column(f'{name}._id', sa.String, nullable=nullable)
    ))
    handler.add_action(ma.AddColumnMigrationAction(
        table_name=table_name,
        column=sa.Column(f'{name}._content_type', sa.String, nullable=nullable)
    ))
    handler.add_action(ma.AddColumnMigrationAction(
        table_name=table_name,
        column=sa.Column(f'{name}._size', BIGINT, nullable=nullable)
    ))
    if BackendFeatures.FILE_BLOCKS in new.backend.features:
        handler.add_action(ma.AddColumnMigrationAction(
            table_name=table_name,
            column=sa.Column(f'{name}._bsize', sa.Integer, nullable=nullable)
        ))
        handler.add_action(ma.AddColumnMigrationAction(
            table_name=table_name,
            column=sa.Column(f'{name}._blocks', ARRAY(pkey_type, ), nullable=nullable)
        ))
    old_table = get_pg_name(f'{table.name}{TableType.FILE.value}/{name}')
    new_table = get_pg_name(f'{table_name}{TableType.FILE.value}/{name}')
    if not inspector.has_table(old_table):
        handler.add_action(ma.CreateTableMigrationAction(
            table_name=new_table,
            columns=[
                sa.Column('_id', pkey_type, primary_key=True),
                sa.Column('_block', sa.LargeBinary)
            ]
        ))


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, list, File, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: list, new: File, handler: MigrationHandler, rename: MigrateRename):
    column_name = rename.get_column_name(table.name, new.prop.name)
    old_name = rename.get_old_column_name(table.name, new.prop.name)
    for item in old:
        table_name = rename.get_table_name(table.name)
        nullable = new.required if new.required == item.nullable else None
        new_name = column_name if not item.name.startswith(column_name) else None
        if nullable is not None or new_name is not None:
            handler.add_action(ma.AlterColumnMigrationAction(
                table_name=table_name,
                column_name=item.name,
                nullable=nullable,
                new_column_name=new_name
            ))
    table_name = rename.get_table_name(table.name)
    old_table = get_pg_name(f'{table.name}{TableType.FILE.value}/{old_name}')
    new_table_old_prop = get_pg_name(f'{table_name}{TableType.FILE.value}/{old_name}')
    new_table_new_prop = get_pg_name(f'{table_name}{TableType.FILE.value}/{column_name}')
    if old_name != column_name and inspector.has_table(old_table):
        handler.add_action(
            ma.RenameTableMigrationAction(
                old_table_name=new_table_old_prop,
                new_table_name=new_table_new_prop
            )
        )


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, NotAvailable, Ref, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: NotAvailable, new: Ref, handler: MigrationHandler, rename: MigrateRename):
    columns = commands.prepare(context, backend, new.prop)
    if not isinstance(columns, list):
        columns = [columns]
    for column in columns:
        if isinstance(column, sa.Column):
            commands.migrate(context, backend, inspector, table, old, column, handler, rename, True)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, list, Ref, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: List[sa.Column], new: Ref, handler: MigrationHandler, rename: MigrateRename):

    new_columns = commands.prepare(context, backend, new.prop)
    if not isinstance(new_columns, list):
        new_columns = [new_columns]
    table_name = rename.get_table_name(table.name)
    foreign_keys = inspector.get_foreign_keys(table.name)
    old_prop_name = rename.get_old_column_name(table.name, get_column_name(new.prop))

    old_names = {}
    new_names = {}
    for item in old:
        base_name = item.name.split(".")
        name = f'{rename.get_column_name(table.name, base_name[0])}.{base_name[1]}'
        old_names[name] = item
    for item in new_columns:
        new_names[item.name] = item

    if not new.prop.level or new.prop.level > 3:
        if len(old) == 1 and old[0].name == f'{old_prop_name}._id':
            column = old[0]
            new_column = new_columns[0]
            commands.migrate(context, backend, inspector, table, column, new_column, handler, rename, True)
            if column.name != new_column.name or table_name != table.name:
                for key in foreign_keys:
                    if key["constrained_columns"] == [column.name]:
                        handler.add_action(ma.DropConstraintMigrationAction(
                            table_name=table_name,
                            constraint_name=key["name"]
                        ), True)
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
            commands.migrate(context, backend, inspector, table, old_col, new_columns[0], handler, rename, True)
            if old_names.keys() != new_names.keys():
                handler.add_action(ma.UpgradeTransferDataMigrationAction(
                    table_name=table_name,
                    foreign_table_name=get_table_name(new.refprops[0]),
                    columns=column_list
                ), True)
            for col in drop_list:
                commands.migrate(context, backend, inspector, table, col, NA, handler, rename, True)

            for key in foreign_keys:
                if key["constrained_columns"] == column_list:
                    handler.add_action(ma.DropConstraintMigrationAction(
                        table_name=table_name,
                        constraint_name=key["name"]
                    ), True)
    else:
        if len(old) == 1 and old[0].name == f'{old_prop_name}._id':
            requires_drop = True
            for new_column in new_columns:
                if isinstance(new_column, sa.Column):
                    if new_column == f'{old_prop_name}._id':
                        requires_drop = False
                        commands.migrate(context, backend, inspector, table, old[0], new_column, handler, rename, True)
                    else:
                        commands.migrate(context, backend, inspector, table, NA, new_column, handler, rename, True)
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
                commands.migrate(context, backend, inspector, table, old[0], NA, handler, rename, True)
        else:
            props = zipitems(
                old_names.keys(),
                new_names.keys(),
                _name_key
            )
            drop_list = []
            for prop in props:
                for old_prop, new_prop in prop:
                    old_prop = old_names[old_prop]
                    new_prop = new_names[new_prop]
                    if old_prop is not None and new_prop is None:
                        drop_list.append(old_prop)
                    else:
                        commands.migrate(context, backend, inspector, table, old_prop, new_prop, handler, rename, True)
            for drop in drop_list:
                commands.migrate(context, backend, inspector, table, drop, NA, handler, rename, True)
        for key in foreign_keys:
            if key["constrained_columns"] == [f'{old_prop_name}._id']:
                handler.add_action(ma.DropConstraintMigrationAction(
                    table_name=table_name,
                    constraint_name=key["name"]
                ), True)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, NotAvailable, DataType, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: NotAvailable, new: DataType, handler: MigrationHandler, rename: MigrateRename):
    column = commands.prepare(context, backend, new.prop)
    commands.migrate(context, backend, inspector, table, old, column, handler, rename, False)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, sa.Column, DataType, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: sa.Column, new: DataType, handler: MigrationHandler, rename: MigrateRename):
    column = commands.prepare(context, backend, new.prop)
    commands.migrate(context, backend, inspector, table, old, column, handler, rename, False)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, sa.Column, sa.Column, MigrationHandler, MigrateRename, bool)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: sa.Column, new: sa.Column, handler: MigrationHandler, rename: MigrateRename, foreign_key: bool = False):
    column_name = rename.get_column_name(table.name, old.name)
    table_name = rename.get_table_name(table.name)

    new_type = new.type.compile(dialect=postgresql.dialect())
    old_type = old.type.compile(dialect=postgresql.dialect())

    # Convert sa.Float, to postgresql DOUBLE PRECISION type
    if isinstance(new.type, sa.Float):
        new_type = 'DOUBLE PRECISION'

    nullable = new.nullable if new.nullable != old.nullable else None
    type_ = new.type if old_type != new_type else None
    new_name = column_name if old.name != new.name else None

    if nullable is not None or type_ is not None or new_name is not None:
        handler.add_action(ma.AlterColumnMigrationAction(
            table_name=table_name,
            column_name=old.name,
            nullable=nullable,
            type_=type_,
            new_column_name=new_name
        ), foreign_key)

    unique_name = f'{table_name}_{column_name}_key'
    old_unique_name = f'{table.name}_{old.name}_key'
    unique_constraints = inspector.get_unique_constraints(table_name=table.name)
    if any(constraint["name"] == old_unique_name for constraint in unique_constraints):
        if not new.unique or table_name != table.name:
            handler.add_action(ma.DropConstraintMigrationAction(
                constraint_name=old_unique_name,
                table_name=table_name
            ), foreign_key)
            if new.unique:
                handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                    constraint_name=unique_name,
                    table_name=table_name,
                    columns=[column_name]
                ), foreign_key)
    else:
        if new.unique:
            handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                constraint_name=unique_name,
                table_name=table_name,
                columns=[column_name]
            ), foreign_key)

    index_name = f'idx_{table_name}_{column_name}'
    old_index_name = f'idx_{table.name}_{old.name}'
    indexes = inspector.get_indexes(table_name=table.name)
    index_required = isinstance(new.type, geoalchemy2.types.Geometry)
    if any(index["name"] == old_index_name for index in indexes):
        if not index_required or table_name != table.name:
            handler.add_action(
                ma.DropIndexMigrationAction(
                    index_name=old_index_name,
                    table_name=table_name
                ), foreign_key)
            if index_required:
                handler.add_action(ma.CreateIndexMigrationAction(
                    index_name=index_name,
                    table_name=table_name,
                    columns=[column_name]
                ), foreign_key)
    else:
        if index_required:
            handler.add_action(ma.CreateIndexMigrationAction(
                index_name=index_name,
                table_name=table_name,
                columns=[column_name]
            ), foreign_key)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, NotAvailable, sa.Column, MigrationHandler, MigrateRename, bool)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: NotAvailable, new: sa.Column, handler: MigrationHandler, rename: MigrateRename, foreign_key: bool = False):
    table_name = rename.get_table_name(table.name)
    handler.add_action(ma.AddColumnMigrationAction(
        table_name=table_name,
        column=new,
    ), foreign_key)
    if new.unique:
        handler.add_action(ma.CreateUniqueConstraintMigrationAction(
            constraint_name=f'{table_name}_{new.name}_key',
            table_name=table_name,
            columns=[new.name]
        ))
    index_required = isinstance(new.type, geoalchemy2.types.Geometry)
    if index_required:
        handler.add_action(ma.CreateIndexMigrationAction(
            table_name=table_name,
            columns=[new.name],
            index_name=f'idx_{table_name}_{new.name}'
        ))


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, sa.Column, NotAvailable, MigrationHandler, MigrateRename, bool)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: sa.Column, new: NotAvailable, handler: MigrationHandler, rename: MigrateRename, foreign_key: bool = False):
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

        if old.unique:
            unique_constraints = inspector.get_unique_constraints(table_name=table.name)
            for constraint in unique_constraints:
                if old.name in constraint["column_names"]:
                    handler.add_action(ma.DropConstraintMigrationAction(
                        table_name=table_name,
                        constraint_name=constraint["name"],
                    ), foreign_key)
        index_required = isinstance(old.type, geoalchemy2.types.Geometry)
        if index_required:
            indexes = inspector.get_indexes(table_name=table.name)
            for index in indexes:
                if old.name in index["column_names"]:
                    handler.add_action(ma.DropIndexMigrationAction(
                        table_name=table_name,
                        index_name=index["name"]
                    ), foreign_key)


def _drop_all_indexes_and_constraints(inspector: Inspector, table: str, new_table: str, handler: MigrationHandler):
    constraints = inspector.get_unique_constraints(table)
    removed = []
    foreign_keys = inspector.get_foreign_keys(table)
    for key in foreign_keys:
        handler.add_action(ma.DropConstraintMigrationAction(
            table_name=new_table,
            constraint_name=key["name"]
        ), True)

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


def _clean_up_file_type(inspector: Inspector, manifest: Manifest, handler: MigrationHandler, rename: MigrateRename):
    allowed_file_tables = []
    existing_tables = []
    for model in manifest.models.values():
        existing_tables.append(rename.get_old_table_name(model.name))
        for prop in model.properties.values():
            if isinstance(prop.dtype, File):
                old_table = rename.get_old_table_name(get_table_name(model))
                old_column = rename.get_old_column_name(old_table, get_column_name(prop))
                allowed_file_tables.append(get_pg_name(f'{old_table}{TableType.FILE.value}/{old_column}'))

    for table in inspector.get_table_names():
        if TableType.FILE.value in table:
            split = table.split(f'{TableType.FILE.value}/')
            if split[0] in existing_tables:
                if table not in allowed_file_tables and not split[1].startswith("__"):
                    new_name = f'{split[0]}{TableType.FILE.value}/__{split[1]}'
                    if inspector.has_table(new_name):
                        handler.add_action(ma.DropTableMigrationAction(
                            table_name=new_name
                        ))
                    handler.add_action(ma.RenameTableMigrationAction(
                        old_table_name=table,
                        new_table_name=new_name
                    ))


def _handle_foreign_key_constraints(inspector: Inspector, manifest: Manifest, handler: MigrationHandler, rename: MigrateRename):
    for model in manifest.models.values():
        source_table = get_pg_name(get_table_name(model))
        old_name = rename.get_old_table_name(source_table)
        foreign_keys = []
        if old_name in inspector.get_table_names():
            foreign_keys = inspector.get_foreign_keys(old_name)
        if model.base:
            referent_table = get_pg_name(get_table_name(model.base.parent))
            if not model.base.level or model.base.level > 3:
                check = False
                for key in foreign_keys:
                    if key["constrained_columns"] == ["_id"]:
                        if key["name"] == f'fk_{referent_table}_id' and key["referred_table"] == referent_table:
                            check = True
                        else:
                            handler.add_action(ma.DropConstraintMigrationAction(
                                table_name=source_table,
                                constraint_name=key["name"]
                            ), True)
                        break
                if not check:
                    handler.add_action(
                        ma.CreateForeignKeyMigrationAction(
                            source_table=source_table,
                            referent_table=referent_table,
                            constraint_name=f'fk_{referent_table}_id',
                            local_cols=["_id"],
                            remote_cols=["_id"]
                        ), True
                    )
            else:
                for key in foreign_keys:
                    if key["constrained_columns"] == ["_id"]:
                        handler.add_action(ma.DropConstraintMigrationAction(
                            table_name=source_table,
                            constraint_name=key["name"]
                        ), True)
                        break
        else:
            for key in foreign_keys:
                if key["constrained_columns"] == ["_id"]:
                    handler.add_action(ma.DropConstraintMigrationAction(
                        table_name=source_table,
                        constraint_name=key["name"]
                    ), True)
                    break

        required_ref_props = {}
        for prop in model.properties.values():
            if isinstance(prop.dtype, Ref):
                if not prop.level or prop.level > 3:
                    name = f"fk_{source_table}_{prop.name}._id"
                    required_ref_props[name] = {
                        "name": name,
                        "constrained_columns": [f"{prop.name}._id"],
                        "referred_table": prop.dtype.model.name,
                        "referred_columns": ["_id"]
                    }

        for key in foreign_keys:
            if key["constrained_columns"] != ["_id"]:
                if key["name"] not in required_ref_props.keys():
                    handler.add_action(ma.DropConstraintMigrationAction(
                        table_name=source_table,
                        constraint_name=key["name"]
                    ), True)
                else:
                    constraint = required_ref_props[key["name"]]
                    if key["constrained_columns"] == constraint["constrained_columns"] and key["referred_table"] == constraint["referred_table"] and key["referred_columns"] == constraint["referred_columns"]:
                        del required_ref_props[key["name"]]
                    else:
                        handler.add_action(ma.DropConstraintMigrationAction(
                            table_name=source_table,
                            constraint_name=key["name"]
                        ), True)

        for prop in required_ref_props.values():
            handler.add_action(ma.CreateForeignKeyMigrationAction(
                source_table=source_table,
                referent_table=prop["referred_table"],
                constraint_name=prop["name"],
                local_cols=prop["constrained_columns"],
                remote_cols=prop["referred_columns"]
            ), True)


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


def _name_key(name: str):
    return name


def _get_remove_name(name: str) -> str:
    new_name = name.split("/")
    if not new_name[-1].startswith("__"):
        new_name[-1] = f'__{new_name[-1]}'
    new_name = '/'.join(new_name)
    new_name = get_pg_name(new_name)
    return new_name
