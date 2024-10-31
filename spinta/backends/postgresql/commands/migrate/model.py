import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends import Backend
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_name, get_pg_sequence_name
from spinta.backends.postgresql.helpers.migrate.actions import MigrationHandler
from spinta.backends.postgresql.helpers.migrate.migrate import drop_all_indexes_and_constraints, handle_new_file_type, \
    get_prop_names, create_changelog_table, MigratePostgresMeta, \
    MigrateModelMeta, zip_and_migrate_properties
from spinta.backends.postgresql.helpers.migrate.name import name_changed, get_pg_changelog_name, get_pg_file_name, \
    get_pg_column_name, get_pg_constraint_name, get_pg_removed_name, is_removed
from spinta.components import Context, Model
from spinta.types.datatype import File
from spinta.utils.itertools import ensure_list
from spinta.utils.schema import NotAvailable, NA
from sqlalchemy.engine.reflection import Inspector


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, Model)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, old: sa.Table, new: Model, **kwargs):
    rename = meta.rename
    handler = meta.handler
    inspector = meta.inspector

    columns = list(old.columns)
    new_table_name = rename.get_table_name(old.name)
    old_table_name = old.name

    # Handle table renaming
    _handle_model_rename(
        old_name=old_table_name,
        new_name=new_table_name,
        inspector=inspector,
        handler=handler
    )

    properties = list(new.properties.values())

    model_meta = MigrateModelMeta()
    model_meta.initialize(
        backend=backend,
        table=old,
        columns=columns
    )

    # Handle property migrations
    zip_and_migrate_properties(
        context=context,
        backend=backend,
        old_table=old,
        new_model=new,
        old_columns=columns,
        new_properties=properties,
        meta=meta,
        rename=rename,
        model_meta=model_meta,
        **kwargs
    )

    # Handle model unique constraint
    required_unique_constraints = _handle_model_unique_constraints(
        old_table=old,
        new_model=new,
        table_name=new_table_name,
        inspector=inspector,
        handler=handler
    )

    # Handle property unique constraints
    required_unique_constraints.extend(_handle_property_unique_constraints(
        context=context,
        backend=backend,
        new_model=new
    ))

    # Clean up old multi constraints for non required models (happens when model.ref is changed)
    # Solo unique constraints are handled with property migrate
    _clean_up_old_multi_constraints(
        old_table=old,
        table_name=new_table_name,
        required_unique_constraints=required_unique_constraints,
        inspector=inspector,
        handler=handler
    )

    # Handle JSON migrations, that need to be run at the end
    _handle_json_column_migrations(
        context=context,
        backend=backend,
        meta=meta,
        model_meta=model_meta,
        old_table=old,
        table_name=new_table_name,
        handler=handler,
        **kwargs
    )


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, NotAvailable, Model)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, old: NotAvailable, new: Model, **kwargs):
    rename = meta.rename
    handler = meta.handler
    inspector = meta.inspector

    table_name = get_pg_name(get_table_name(new))
    pkey_type = commands.get_primary_key_type(context, backend)

    columns = []
    for prop in new.properties.values():
        # Ignore deleted / reserved properties
        if prop.name.startswith('_'):
            continue

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

    # Handle new model's unique constraints
    _handle_new_model_unique_constraints(
        new_model=new,
        table_name=table_name,
        handler=handler
    )

    # Create changelog table
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


def _handle_model_rename(
    old_name: str,
    new_name: str,
    inspector: Inspector,
    handler: MigrationHandler
):
    # Do not rename, if name has not been changed
    if not name_changed(old_name, new_name):
        return

    handler.add_action(ma.RenameTableMigrationAction(
        old_table_name=old_name,
        new_table_name=new_name
    ))

    # Handle Changelog table rename
    old_changelog_name = get_pg_changelog_name(old_name)
    if inspector.has_table(old_changelog_name):
        new_changelog_name = get_pg_changelog_name(new_name)
        handler.add_action(ma.RenameTableMigrationAction(
            old_table_name=old_changelog_name,
            new_table_name=new_changelog_name
        )).add_action(ma.RenameSequenceMigrationAction(
            old_name=get_pg_sequence_name(old_changelog_name),
            new_name=get_pg_sequence_name(new_changelog_name)
        ))

    # Handle File table renames
    old_file_name = get_pg_file_name(old_name)
    for table in inspector.get_table_names():
        if table.startswith(old_file_name):
            split = table.split(TableType.FILE.value)
            handler.add_action(ma.RenameTableMigrationAction(
                old_table_name=table,
                new_table_name=get_pg_file_name(new_name, split[1])
            ))


def _handle_model_unique_constraints(
    old_table: sa.Table,
    new_model: Model,
    table_name: str,
    inspector: Inspector,
    handler: MigrationHandler
) -> list:
    if not new_model.unique:
        return []

    required_unique_constraints = []

    for property_combination in new_model.unique:
        column_name_list = []

        for prop in property_combination:
            for name in get_prop_names(prop):
                column_name_list.append(get_pg_column_name(name))

        constraints = inspector.get_unique_constraints(old_table.name)
        constraint_name = get_pg_constraint_name(table_name, column_name_list)
        required_unique_constraints.append(column_name_list)

        # Skip unique properties, since they are already handled in other parts
        if len(property_combination) == 1 and property_combination[0].dtype.unique:
            continue

        if not any(constraint['name'] == constraint_name for constraint in constraints):
            handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                table_name=table_name,
                constraint_name=constraint_name,
                columns=column_name_list
            ))
    return required_unique_constraints


def _handle_property_unique_constraints(
    context: Context,
    backend: PostgreSQL,
    new_model: Model
) -> list:
    required_unique_constraints = []
    for prop in new_model.flatprops.values():
        if not prop.dtype.unique:
            continue

        columns = commands.prepare(context, backend, prop.dtype)
        columns = ensure_list(columns)
        column_name_list = []
        for column in columns:
            if isinstance(column, sa.Column):
                column_name_list.append(column.name)

        if column_name_list:
            required_unique_constraints.append(column_name_list)
    return required_unique_constraints


def _clean_up_old_multi_constraints(
    old_table: sa.Table,
    table_name: str,
    required_unique_constraints: list,
    inspector: Inspector,
    handler: MigrationHandler
):
    # Ignore deleted tables
    if old_table.name.startswith('_'):
        return

    unique_constraints = inspector.get_unique_constraints(old_table.name)
    for constraint in unique_constraints:
        if constraint["column_names"] not in required_unique_constraints:
            if not handler.has_constraint_been_dropped(constraint["name"]):
                handler.add_action(ma.DropConstraintMigrationAction(
                    table_name=table_name,
                    constraint_name=constraint["name"]
                ))


def _handle_json_column_migrations(
    context: Context,
    backend: Backend,
    meta: MigratePostgresMeta,
    model_meta: MigrateModelMeta,
    old_table: sa.Table,
    table_name: str,
    handler: MigrationHandler,
    **kwargs
):
    for json_meta in model_meta.json_columns.values():
        if json_meta.new_keys and json_meta.cast_to is None:
            removed_keys = [key for key, new_key in json_meta.new_keys.items() if new_key == get_pg_removed_name(key)]

            if removed_keys == json_meta.keys or json_meta.full_remove:
                commands.migrate(context, backend, meta, old_table, json_meta.column, NA)
            else:
                for old_key, new_key in json_meta.new_keys.items():
                    if new_key in json_meta.keys:
                        handler.add_action(ma.RemoveJSONAttributeMigrationAction(
                            table_name,
                            json_meta.column,
                            new_key
                        ))
                    handler.add_action(ma.RenameJSONAttributeMigrationAction(
                        table_name,
                        json_meta.column,
                        old_key,
                        new_key
                    ))
        elif isinstance(json_meta.cast_to, tuple) and len(json_meta.cast_to) == 2:
            new_column = json_meta.cast_to[0]
            key = json_meta.cast_to[1]
            commands.migrate(context, backend, meta, old_table, json_meta.column, NA)
            commands.migrate(context, backend, meta, old_table, NA, new_column, foreign_key=False,
                             model_meta=model_meta, **kwargs)
            renamed = json_meta.column._copy()
            renamed.name = get_pg_removed_name(json_meta.column.name)
            renamed.key = get_pg_removed_name(json_meta.column.name)
            handler.add_action(
                ma.TransferJSONDataMigrationAction(
                    table_name,
                    renamed,
                    [(key, new_column)]
                )
            )
        # Rename column
        if json_meta.new_name:
            handler.add_action(ma.AlterColumnMigrationAction(
                table_name,
                json_meta.column.name,
                new_column_name=json_meta.new_name
            ))


def _handle_new_model_unique_constraints(
    new_model: Model,
    table_name: str,
    handler: MigrationHandler
):
    if not new_model.unique:
        return

    for property_combination in new_model.unique:
        column_name_list = []
        for prop in property_combination:
            for name in get_prop_names(prop):
                column_name_list.append(get_pg_name(name))
        constraint_name = get_pg_constraint_name(table_name, column_name_list)
        handler.add_action(ma.CreateUniqueConstraintMigrationAction(
            table_name=table_name,
            constraint_name=constraint_name,
            columns=column_name_list
        ))
