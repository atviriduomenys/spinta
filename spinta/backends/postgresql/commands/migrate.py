from typing import Any, List

import geoalchemy2.types

from spinta.backends.components import BackendFeatures
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.helpers import get_pg_name, get_column_name
from spinta.backends.postgresql.helpers.migrate import MigrationHandler
from spinta.datasets.inspect.helpers import zipitems
import spinta.backends.postgresql.helpers.migrate as ma

import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.dialects.postgresql import JSONB, BIGINT, ARRAY
from sqlalchemy.dialects import postgresql

from spinta.cli.migrate import MigrateMeta, MigrateRename
from spinta.commands import create_exception
from spinta.datasets.enums import Level
from spinta.types.datatype import DataType, Ref, File, Inherit, Array, Object
from spinta.types.namespace import sort_models_by_ref_and_base
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
    models = commands.get_models(context, manifest)
    for table in table_names:
        name = migrate_meta.rename.get_table_name(table)
        if name not in models.keys():
            name = table
        tables.append(name)
    sorted_models = sort_models_by_ref_and_base(list(models.values()))
    sorted_model_names = list([model.name for model in sorted_models])
    # Do reversed zip, to ensure that sorted models get selected first
    models = zipitems(
        sorted_model_names,
        tables,
        _model_name_key
    )
    handler = MigrationHandler()
    for items in models:
        for new_model, old_model in items:
            if old_model and any(value in old_model for value in (TableType.CHANGELOG.value, TableType.FILE.value)):
                continue
            if old_model and old_model in EXCLUDED_MODELS:
                continue
            old = NA
            if old_model:
                old = metadata.tables[migrate_meta.rename.get_old_table_name(old_model)]
            new = commands.get_model(context, manifest, new_model) if new_model else new_model
            commands.migrate(context, backend, inspector, old, new, handler, migrate_meta.rename)
    _handle_foreign_key_constraints(inspector, sorted_models, handler, migrate_meta.rename)
    _clean_up_file_type(inspector, sorted_models, handler, migrate_meta.rename)
    try:
        if migrate_meta.autocommit:
            # Recreate new connection, that auto commits
            with backend.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                ctx = MigrationContext.configure(conn, opts={
                    "as_sql": migrate_meta.plan
                })
                op = Operations(ctx)
                handler.run_migrations(op)
        else:
            with ctx.begin_transaction():
                handler.run_migrations(op)
    except sa.exc.OperationalError as error:
        exception = create_exception(manifest, error)
        raise exception


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, Model, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, old: sa.Table, new: Model,
            handler: MigrationHandler, rename: MigrateRename):
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
                if '.' in column.name and column.name.split('.')[0] == column_name:
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
                for name in _get_prop_names(prop):
                    prop_list.append(get_pg_name(name))

            constraints = inspector.get_unique_constraints(old.name)
            constraint_name = f'{old.name}_{"_".join(prop_list)}_key'
            if old.name != table_name:
                constraint_name = f'{table_name}_{"_".join(prop_list)}_key'
            constraint_name = get_pg_name(constraint_name)
            required_unique_constraints.append(prop_list)

            # Skip unique properties, since they are already handled in other parts
            if not (len(val) == 1 and val[0].dtype.unique):
                if not any(constraint['name'] == constraint_name for constraint in constraints):
                    handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                        table_name=table_name,
                        constraint_name=constraint_name,
                        columns=prop_list
                    ))

    for prop in new.properties.values():
        if prop.dtype.unique:
            result = commands.prepare(context, backend, prop.dtype)
            if not isinstance(result, list):
                result = [result]
            prop_list = []
            for item in result:
                if isinstance(item, sa.Column):
                    prop_list.append(item.name)
            if prop_list:
                required_unique_constraints.append(prop_list)

    # Clean up old multi constraints for non required models (happens when model.ref is changed)
    # Solo unique constraints are handled with property migrate
    if not old.name.startswith("_"):
        unique_constraints = inspector.get_unique_constraints(old.name)
        for constraint in unique_constraints:
            if constraint["column_names"] not in required_unique_constraints:
                if not handler.has_constraint_been_dropped(constraint["name"]):
                    handler.add_action(ma.DropConstraintMigrationAction(
                        table_name=table_name,
                        constraint_name=constraint["name"]
                    ))


@commands.migrate.register(Context, PostgreSQL, Inspector, NotAvailable, Model, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, old: NotAvailable, new: Model,
            handler: MigrationHandler, rename: MigrateRename):
    table_name = get_pg_name(get_table_name(new))

    pkey_type = commands.get_primary_key_type(context, backend)

    columns = []
    for prop in new.properties.values():
        if not prop.name.startswith("_"):
            if isinstance(prop.dtype, File):
                columns += _handle_new_file_type(context, backend, inspector, prop, pkey_type, handler)
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
                for name in _get_prop_names(prop):
                    prop_list.append(get_pg_name(name))
            constraint_name = get_pg_name(f'{table_name}_{"_".join(prop_list)}_key')
            handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                table_name=table_name,
                constraint_name=constraint_name,
                columns=prop_list
            ))

    _create_changelog_table(context, new, handler, rename)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, NotAvailable, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, old: sa.Table, new: NotAvailable,
            handler: MigrationHandler, rename: MigrateRename):
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


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, sa.Column, Property, MigrationHandler,
                           MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: sa.Column, new: Property, handler: MigrationHandler, rename: MigrateRename):
    commands.migrate(context, backend, inspector, table, old, new.dtype, handler, rename)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, NotAvailable, Property, MigrationHandler,
                           MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: NotAvailable, new: Property, handler: MigrationHandler, rename: MigrateRename):
    commands.migrate(context, backend, inspector, table, old, new.dtype, handler, rename)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, NotAvailable, File, MigrationHandler,
                           MigrateRename)
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
        item_name = item.name
        new_name = item_name.replace(item_name.split(".")[0], column_name) if not item_name.startswith(column_name) else None
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
            commands.migrate(context, backend, inspector, table, column, new_column, handler, rename, True)
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
    else:
        if len(old) == 1 and old[0].name == f'{old_prop_name}._id':
            requires_drop = True
            for new_column in new_columns:
                if isinstance(new_column, sa.Column):
                    if new_column.name == f'{old_prop_name}._id':
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


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, NotAvailable, DataType, MigrationHandler,
                           MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: NotAvailable, new: DataType, handler: MigrationHandler, rename: MigrateRename):
    column = commands.prepare(context, backend, new.prop)
    commands.migrate(context, backend, inspector, table, old, column, handler, rename, False)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, sa.Column, DataType, MigrationHandler,
                           MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: sa.Column, new: DataType, handler: MigrationHandler, rename: MigrateRename):
    column = commands.prepare(context, backend, new.prop)
    commands.migrate(context, backend, inspector, table, old, column, handler, rename, False)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, sa.Column, sa.Column, MigrationHandler,
                           MigrateRename, bool)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: sa.Column, new: sa.Column, handler: MigrationHandler, rename: MigrateRename,
            foreign_key: bool = False):
    column_name = new.name
    table_name = rename.get_table_name(table.name)
    new_type = new.type.compile(dialect=postgresql.dialect())
    old_type = old.type.compile(dialect=postgresql.dialect())
    using = None
    # Convert sa.Float, to postgresql DOUBLE PRECISION type
    if isinstance(new.type, sa.Float):
        new_type = 'DOUBLE PRECISION'
    if isinstance(old.type, geoalchemy2.types.Geometry) and isinstance(new.type, geoalchemy2.types.Geometry):
        if old.type.srid != new.type.srid:
            srid_name = f'"{old.name}"'
            srid = new.type.srid
            if old.type.srid == -1:
                srid_name = f'ST_SetSRID({srid_name}, 4326)'
            if new.type.srid == -1:
                srid = 4326
            using = f'ST_Transform({srid_name}, {srid})'

    nullable = new.nullable if new.nullable != old.nullable else None
    type_ = new.type if old_type != new_type else None
    new_name = column_name if old.name != new.name else None

    if nullable is not None or type_ is not None or new_name is not None or using is not None:
        handler.add_action(ma.AlterColumnMigrationAction(
            table_name=table_name,
            column_name=old.name,
            nullable=nullable,
            type_=type_,
            new_column_name=new_name,
            using=using
        ), foreign_key)

    renamed = _check_if_renamed(table.name, table_name, old.name, new.name)
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
                unique_name = get_pg_name(_rename_index_name(constraint["name"], table.name, table_name, old.name, new.name))
                if new.unique:
                    handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                        constraint_name=unique_name,
                        table_name=table_name,
                        columns=[column_name]
                    ), foreign_key)
    else:
        if new.unique:
            if not any(constraint["column_names"] == [column_name] for constraint in inspector.get_unique_constraints(table_name=table.name)) and not any(index["column_names"] == [column_name] and index["unique"] for index in inspector.get_indexes(table_name=table.name)):
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
                handler.add_action(
                    ma.DropIndexMigrationAction(
                        index_name=index["name"],
                        table_name=table_name
                    ), foreign_key)
                index_name = get_pg_name(_rename_index_name(index["name"], table.name, table_name, old.name, new.name))
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
            if not any(index["column_names"] == [column_name] for index in inspector.get_indexes(table_name=table.name)):
                handler.add_action(ma.CreateIndexMigrationAction(
                    index_name=index_name,
                    table_name=table_name,
                    columns=[column_name]
                ), foreign_key)
        elif isinstance(new.type, geoalchemy2.types.Geometry):
            if not any(index["column_names"] == [column_name] for index in inspector.get_indexes(table_name=table.name)):
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


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, NotAvailable, sa.Column, MigrationHandler,
                           MigrateRename, bool)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: NotAvailable, new: sa.Column, handler: MigrationHandler, rename: MigrateRename,
            foreign_key: bool = False):
    table_name = get_pg_name(rename.get_table_name(table.name))
    handler.add_action(ma.AddColumnMigrationAction(
        table_name=table_name,
        column=new,
    ), foreign_key)
    if new.unique:
        constraint_name = get_pg_name(f'{table_name}_{new.name}_key')
        if not any(constraint["name"] == constraint_name for constraint in inspector.get_unique_constraints(table_name)):
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


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, sa.Column, NotAvailable, MigrationHandler,
                           MigrateRename, bool)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, table: sa.Table,
            old: sa.Column, new: NotAvailable, handler: MigrationHandler, rename: MigrateRename,
            foreign_key: bool = False):
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


def _clean_up_file_type(inspector: Inspector, models: List[Model], handler: MigrationHandler, rename: MigrateRename):
    allowed_file_tables = []
    existing_tables = []
    for model in models:
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
                    new_name = get_pg_name(f'{split[0]}{TableType.FILE.value}/__{split[1]}')
                    if inspector.has_table(new_name):
                        handler.add_action(ma.DropTableMigrationAction(
                            table_name=new_name
                        ))
                    handler.add_action(ma.RenameTableMigrationAction(
                        old_table_name=table,
                        new_table_name=new_name
                    ))
                    _drop_all_indexes_and_constraints(inspector, table, new_name, handler)


def _handle_foreign_key_constraints(inspector: Inspector, models: List[Model], handler: MigrationHandler,
                                    rename: MigrateRename):
    for model in models:
        source_table = get_pg_name(get_table_name(model))
        old_name = get_pg_name(rename.get_old_table_name(source_table))
        foreign_keys = []
        if old_name in inspector.get_table_names():
            foreign_keys = inspector.get_foreign_keys(old_name)
        if model.base:
            referent_table = get_pg_name(get_table_name(model.base.parent))
            if not model.base.level or model.base.level > 3:
                check = False
                fk_name = get_pg_name(f'fk_{referent_table}_id')
                for key in foreign_keys:
                    if key["constrained_columns"] == ["_id"]:
                        if key["name"] == fk_name and key["referred_table"] == referent_table:
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
                            constraint_name=fk_name,
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
                    name = get_pg_name(f"fk_{source_table}_{prop.name}._id")
                    required_ref_props[name] = {
                        "name": name,
                        "constrained_columns": [f"{prop.name}._id"],
                        "referred_table": get_pg_name(prop.dtype.model.name),
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
                    if key["constrained_columns"] == constraint["constrained_columns"] and key["referred_table"] == \
                        constraint["referred_table"] and key["referred_columns"] == constraint["referred_columns"]:
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


def _handle_new_file_type(context: Context, backend: PostgreSQL, inspector: Inspector, prop: Property, pkey_type: Any, handler: MigrationHandler) -> list:
    name = get_column_name(prop)
    nullable = not prop.dtype.required
    columns = []
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
    new_table = get_pg_name(get_table_name(prop, TableType.FILE))
    if not inspector.has_table(new_table):
        handler.add_action(ma.CreateTableMigrationAction(
            table_name=new_table,
            columns=[
                sa.Column('_id', pkey_type, primary_key=True),
                sa.Column('_block', sa.LargeBinary)
            ]
        ))
    return columns


def _handle_new_array_type(context: Context, backend: PostgreSQL, inspector: Inspector, prop: Property, pkey_type: Any, handler: MigrationHandler):
    columns = []
    if isinstance(prop.dtype, Array) and prop.dtype.items:
        if prop.list is None:
            columns.append(sa.Column(prop.place, JSONB))

        if isinstance(prop.dtype.items.dtype, File):
            new_columns = _handle_new_file_type(context, backend, inspector, prop.dtype.items, pkey_type, handler)
        elif isinstance(prop.dtype.items.dtype, Array):
            new_columns = _handle_new_array_type(context, backend, inspector, prop.dtype.items, pkey_type, handler)
        elif isinstance(prop.dtype.items.dtype, Object):
            new_columns = _handle_new_object_type(context, backend, inspector, prop.dtype.items, pkey_type, handler)
        else:
            new_columns = commands.prepare(context, backend, prop.dtype.items)
        if not isinstance(new_columns, list):
            new_columns = [new_columns]
        for column in new_columns:
            if not isinstance(column, sa.Column):
                new_columns.remove(column)

        new_table = get_pg_name(get_table_name(prop, TableType.LIST))
        if not inspector.has_table(new_table):
            main_table_name = get_pg_name(get_table_name(prop.model))
            handler.add_action(ma.CreateTableMigrationAction(
                table_name=new_table,
                columns=[
                    sa.Column('_txn', pkey_type, index=True),
                    sa.Column('_rid', pkey_type, sa.ForeignKey(
                        f'{main_table_name}._id', ondelete='CASCADE',
                    ), index=True),
                    *new_columns
                ]
            ))
    return columns


def _handle_new_object_type(context: Context, backend: PostgreSQL, inspector: Inspector, prop: Property, pkey_type: Any, handler: MigrationHandler):
    columns = []
    if isinstance(prop.dtype, Object) and prop.dtype.properties:
        for new_prop in prop.dtype.properties.values():
            if prop.name.startswith('_') and prop.name not in ('_revision',):
                continue
            if isinstance(new_prop.dtype, File):
                columns = _handle_new_file_type(context, backend, inspector, new_prop, pkey_type, handler)
            elif isinstance(new_prop.dtype, Array):
                columns = _handle_new_array_type(context, backend, inspector, new_prop, pkey_type, handler)
            elif isinstance(new_prop.dtype, Object):
                columns = _handle_new_object_type(context, backend, inspector, new_prop, pkey_type, handler)
            else:
                columns = commands.prepare(context, backend, new_prop)

            if not isinstance(columns, list):
                columns = [columns]
            for column in columns:
                if not isinstance(column, sa.Column):
                    columns.remove(column)
    return columns


def _check_if_renamed(old_table: str, new_table: str, old_property: str, new_property:str):
    return old_table != new_table or old_property != new_property


def _rename_index_name(index: str, old_table: str, new_table: str, old_property: str, new_property:str):
    new = index.replace(old_table, new_table)
    new = new.replace(old_property, new_property)
    return new


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


def _get_prop_names(prop: Property):
    name = prop.name
    if isinstance(prop.dtype, Ref):
        if not prop.level or prop.level > Level.open:
            name = f'{name}._id'
        else:
            for refprop in prop.dtype.refprops:
                yield f'{name}.{refprop.name}'
    yield name
