from typing import Union

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine.reflection import Inspector

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_name, get_column_name
from spinta.backends.postgresql.helpers.migrate.actions import MigrationHandler
from spinta.backends.postgresql.helpers.migrate.migrate import drop_all_indexes_and_constraints, handle_new_file_type, \
    get_remove_name, get_root_attr, get_prop_names, create_changelog_table, property_and_column_name_key, \
    property_and_column_name_key
from spinta.cli.migrate import MigrateRename
from spinta.components import Context, Model, Property
from spinta.datasets.inspect.helpers import zipitems
from spinta.types.datatype import File, JSON
from spinta.types.text.components import Text
from spinta.utils.schema import NotAvailable, NA


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, Model, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, old: sa.Table, new: Model,
            handler: MigrationHandler, rename: MigrateRename):
    columns = list(old.columns)
    table_name = rename.get_table_name(old.name)

    if table_name != old.name:
        handler.add_action(ma.RenameTableMigrationAction(old.name, table_name))
        changelog_name = get_pg_name(f'{old.name}{TableType.CHANGELOG.value}')
        file_name = get_pg_name(f'{old.name}{TableType.FILE.value}')
        if inspector.has_table(changelog_name):
            new_changelog_name = get_pg_name(f'{table_name}{TableType.CHANGELOG.value}')
            handler.add_action(ma.RenameTableMigrationAction(
                old_table_name=changelog_name,
                new_table_name=new_changelog_name
            ))
            handler.add_action(ma.RenameSequenceMigrationAction(
                old_name=get_pg_name(f'{changelog_name}__id_seq'),
                new_name=get_pg_name(f'{new_changelog_name}__id_seq')
            ))
        for table in inspector.get_table_names():
            if table.startswith(file_name):
                split = table.split(TableType.FILE.value)
                handler.add_action(ma.RenameTableMigrationAction(
                    old_table_name=file_name,
                    new_table_name=get_pg_name(f'{table_name}{TableType.FILE.value}{split[1]}')
                ))

    properties = list(new.properties.values())

    props = zipitems(
        columns,
        properties,
        lambda x: property_and_column_name_key(x, rename, old.name)
    )
    for items in props:
        old_columns = []
        new_properties = []
        for old_prop, new_prop in items:
            if new_prop and new_prop.name.startswith('_'):
                continue

            if old_prop not in old_columns:
                old_columns.append(old_prop)
            if new_prop and new_prop not in new_properties:
                new_properties.append(new_prop)

        if len(old_columns) == 1:
            old_columns = old_columns[0]
        elif not old_columns:
            old_columns = NA
            if not new_properties:
                continue

        if new_properties:
            for prop in new_properties:
                if not prop:
                    commands.migrate(context, backend, inspector, old, old_columns, prop, handler, rename, False)
                else:
                    commands.migrate(context, backend, inspector, old, old_columns, prop, handler, rename)
        else:
            # Remove from JSONB clean up
            if isinstance(old_columns, sa.Column) and isinstance(old_columns.type, JSONB):
                columns.remove(old_columns)
            commands.migrate(context, backend, inspector, old, old_columns, NA, handler, rename, False)

    required_unique_constraints = []
    if new.unique:
        for val in new.unique:
            prop_list = []

            for prop in val:
                for name in get_prop_names(prop):
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

    # Clean up JSONB columns (old Text, JSON properties)
    for column in columns:
        if isinstance(column.type, JSONB):
            flag_for_removal = True
            name = rename.get_column_name(old.name, column.name, only_root=False)
            for prop in new.properties.values():
                if name == prop.name:
                    if isinstance(prop.dtype, JSON) or isinstance(prop.dtype, Text):
                        flag_for_removal = False
                        break

            if flag_for_removal:
                commands.migrate(context, backend, inspector, old, column, NA, handler, rename, False)


@commands.migrate.register(Context, PostgreSQL, Inspector, NotAvailable, Model, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, old: NotAvailable, new: Model,
            handler: MigrationHandler, rename: MigrateRename):
    table_name = get_pg_name(get_table_name(new))

    pkey_type = commands.get_primary_key_type(context, backend)

    columns = []
    for prop in new.properties.values():
        if not prop.name.startswith("_"):
            if isinstance(prop.dtype, File):
                columns += handle_new_file_type(context, backend, inspector, prop, pkey_type, handler)
            else:
                cols = commands.prepare(context, backend, prop)
                if isinstance(cols, list):
                    for column in cols:
                        if isinstance(column, (sa.Column, sa.Index)):
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
                for name in get_prop_names(prop):
                    prop_list.append(get_pg_name(name))
            constraint_name = get_pg_name(f'{table_name}_{"_".join(prop_list)}_key')
            handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                table_name=table_name,
                constraint_name=constraint_name,
                columns=prop_list
            ))

    create_changelog_table(context, new, handler, rename)


@commands.migrate.register(Context, PostgreSQL, Inspector, sa.Table, NotAvailable, MigrationHandler, MigrateRename)
def migrate(context: Context, backend: PostgreSQL, inspector: Inspector, old: sa.Table, new: NotAvailable,
            handler: MigrationHandler, rename: MigrateRename):
    old_name = old.name
    remove_name = get_remove_name(old_name)

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
        drop_all_indexes_and_constraints(inspector, old_name, remove_name, handler)

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
            drop_all_indexes_and_constraints(inspector, old_changelog_name, new_changelog_name, handler)
        for table in inspector.get_table_names():
            if table.startswith(f'{old_name}{TableType.FILE.value}'):
                split = table.split(TableType.FILE.value)
                new_name = get_pg_name(f'{remove_name}{TableType.FILE.value}{split[1]}')
                handler.add_action(ma.RenameTableMigrationAction(
                    old_table_name=table,
                    new_table_name=new_name
                ))
                drop_all_indexes_and_constraints(inspector, table, new_name, handler)



