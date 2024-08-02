import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_name, get_pg_sequence_name
from spinta.backends.postgresql.helpers.migrate.migrate import drop_all_indexes_and_constraints, handle_new_file_type, \
    get_prop_names, create_changelog_table, property_and_column_name_key, MigratePostgresMeta, \
    MigrateModelMeta, handle_internal_ref_to_scalar_conversion
from spinta.backends.postgresql.helpers.migrate.name import has_been_renamed, get_pg_changelog_name, get_pg_file_name, \
    get_pg_column_name, get_pg_constraint_name, get_pg_removed_name, is_removed
from spinta.components import Context, Model
from spinta.datasets.inspect.helpers import zipitems
from spinta.types.datatype import File
from spinta.utils.schema import NotAvailable, NA


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, Model)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, old: sa.Table, new: Model, **kwargs):
    rename = meta.rename
    handler = meta.handler
    inspector = meta.inspector

    columns = list(old.columns)
    new_table_name = rename.get_table_name(old.name)
    old_table_name = old.name

    # Handle table renaming
    if has_been_renamed(old_table_name, new_table_name):
        handler.add_action(ma.RenameTableMigrationAction(
            old_table_name=old_table_name,
            new_table_name=new_table_name
        ))

        old_changelog_name = get_pg_changelog_name(old_table_name)
        old_file_name = get_pg_file_name(old_table_name)

        if inspector.has_table(old_changelog_name):
            new_changelog_name = get_pg_changelog_name(new_table_name)
            handler.add_action(ma.RenameTableMigrationAction(
                old_table_name=old_changelog_name,
                new_table_name=new_changelog_name
            )).add_action(ma.RenameSequenceMigrationAction(
                old_name=get_pg_sequence_name(old_changelog_name),
                new_name=get_pg_sequence_name(new_changelog_name)
            ))

        for table in inspector.get_table_names():
            if table.startswith(old_file_name):
                split = table.split(TableType.FILE.value)
                handler.add_action(ma.RenameTableMigrationAction(
                    old_table_name=table,
                    new_table_name=get_pg_file_name(new_table_name, split[1])
                ))

    properties = list(new.properties.values())

    model_meta = MigrateModelMeta()
    for column in columns:
        if isinstance(column.type, JSONB):
            model_meta.add_json_column(
                backend,
                old,
                column
            )

    # Handle property migrations
    props = zipitems(
        columns,
        properties,
        lambda x: property_and_column_name_key(x, rename, old, new)
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
                    commands.migrate(context, backend, meta, old, old_columns, prop, model_meta=model_meta, foreign_key=False, **kwargs)
                else:
                    handled = handle_internal_ref_to_scalar_conversion(
                        context,
                        backend,
                        meta,
                        old,
                        old_columns,
                        prop
                    )
                    if not handled:
                        commands.migrate(context, backend, meta, old, old_columns, prop, model_meta=model_meta, **kwargs)
        else:
            # Remove from JSONB clean up
            if isinstance(old_columns, sa.Column) and isinstance(old_columns.type, JSONB):
                columns.remove(old_columns)
            commands.migrate(context, backend, meta, old, old_columns, NA, model_meta=model_meta, foreign_key=False, **kwargs)

    # Handle model unique constraint
    required_unique_constraints = []
    if new.unique:
        for val in new.unique:
            prop_list = []

            for prop in val:
                for name in get_prop_names(prop):
                    prop_list.append(get_pg_column_name(name))

            constraints = inspector.get_unique_constraints(old.name)
            constraint_name = get_pg_constraint_name(old_table_name, prop_list)
            if has_been_renamed(old_table_name, new_table_name):
                constraint_name = get_pg_constraint_name(new_table_name, prop_list)
            required_unique_constraints.append(prop_list)

            # Skip unique properties, since they are already handled in other parts
            if not (len(val) == 1 and val[0].dtype.unique):
                if not any(constraint['name'] == constraint_name for constraint in constraints):
                    handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                        table_name=new_table_name,
                        constraint_name=constraint_name,
                        columns=prop_list
                    ))

    # Handle property unique constraints
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
                        table_name=new_table_name,
                        constraint_name=constraint["name"]
                    ))

    # Handle JSON migrations, that need to be run at the end
    for json_meta in model_meta.json_columns.values():
        if json_meta.new_keys and json_meta.cast_to is None:
            removed_keys = [key for key, new_key in json_meta.new_keys.items() if new_key == get_pg_removed_name(key)]

            if removed_keys == json_meta.keys or json_meta.full_remove:
                commands.migrate(context, backend, meta, old, json_meta.column, NA)
            else:
                for old_key, new_key in json_meta.new_keys.items():
                    if new_key in json_meta.keys:
                        handler.add_action(ma.RemoveJSONAttributeMigrationAction(
                            new_table_name,
                            json_meta.column,
                            new_key
                        ))
                    handler.add_action(ma.RenameJSONAttributeMigrationAction(
                        new_table_name,
                        json_meta.column,
                        old_key,
                        new_key
                    ))
        elif isinstance(json_meta.cast_to, tuple) and len(json_meta.cast_to) == 2:
            new_column = json_meta.cast_to[0]
            key = json_meta.cast_to[1]
            commands.migrate(context, backend, meta, old, json_meta.column, NA)
            commands.migrate(context, backend, meta, old, NA, new_column, foreign_key=False, model_meta=model_meta, **kwargs)
            renamed = json_meta.column._copy()
            renamed.name = get_pg_removed_name(json_meta.column.name)
            renamed.key = get_pg_removed_name(json_meta.column.name)
            handler.add_action(
                ma.TransferJSONDataMigrationAction(
                    new_table_name,
                    renamed,
                    [(key, new_column)]
                )
            )
        # Rename column
        if json_meta.new_name:
            handler.add_action(ma.AlterColumnMigrationAction(
                new_table_name,
                json_meta.column.name,
                new_column_name=json_meta.new_name
            ))


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, NotAvailable, Model)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, old: NotAvailable, new: Model, **kwargs):
    rename = meta.rename
    handler = meta.handler
    inspector = meta.inspector

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
            constraint_name = get_pg_constraint_name(table_name, prop_list)
            handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                table_name=table_name,
                constraint_name=constraint_name,
                columns=prop_list
            ))

    create_changelog_table(context, new, handler, rename)


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, NotAvailable)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, old: sa.Table, new: NotAvailable):
    handler = meta.handler
    inspector = meta.inspector

    old_table_name = old.name

    # Skip already deleted table
    if is_removed(old_table_name):
        return

    removed_table_name = get_pg_removed_name(old_table_name)

    if inspector.has_table(removed_table_name):
        # Drop table if it was already flagged for deletion
        handler.add_action(ma.DropTableMigrationAction(
            table_name=removed_table_name
        ))

        # Drop changelog if it was already flagged for deletion
        removed_changelog_name = get_pg_changelog_name(removed_table_name)
        if inspector.has_table(removed_changelog_name):
            handler.add_action(ma.DropTableMigrationAction(
                table_name=removed_changelog_name
            ))

        # Drop file tables if they were already flagged for deletion
        removed_file_name = get_pg_file_name(removed_table_name)
        for table in inspector.get_table_names():
            if table.startswith(removed_file_name):
                handler.add_action(ma.DropTableMigrationAction(
                    table_name=table
                ))

    handler.add_action(ma.RenameTableMigrationAction(
        old_table_name=old_table_name,
        new_table_name=removed_table_name
    ))
    drop_all_indexes_and_constraints(inspector, old_table_name, removed_table_name, handler)

    # Flag changelog for deletion
    old_changelog_name = get_pg_changelog_name(old_table_name)
    new_changelog_name = get_pg_changelog_name(removed_table_name)
    if inspector.has_table(old_changelog_name):
        handler.add_action(ma.RenameTableMigrationAction(
            old_table_name=old_changelog_name,
            new_table_name=new_changelog_name
        ))
        handler.add_action(ma.RenameSequenceMigrationAction(
            old_name=get_pg_sequence_name(old_changelog_name),
            new_name=get_pg_sequence_name(new_changelog_name)
        ))
        drop_all_indexes_and_constraints(inspector, old_changelog_name, new_changelog_name, handler)

    # Flag file tables for deletion
    for table in inspector.get_table_names():
        old_file_name = get_pg_file_name(old_table_name)
        if table.startswith(old_file_name):
            split = table.split(TableType.FILE.value)
            removed_file_name = get_pg_file_name(removed_table_name, split[1])
            handler.add_action(ma.RenameTableMigrationAction(
                old_table_name=table,
                new_table_name=removed_file_name
            ))
            drop_all_indexes_and_constraints(inspector, table, removed_file_name, handler)



