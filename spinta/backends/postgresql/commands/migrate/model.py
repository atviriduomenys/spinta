import sqlalchemy as sa

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
    MigrateModelMeta, zip_and_migrate_properties, constraint_with_name, adjust_kwargs
from spinta.backends.postgresql.helpers.name import name_changed, get_pg_changelog_name, get_pg_file_name, \
    get_pg_column_name, get_pg_constraint_name, get_pg_removed_name, is_removed, get_pg_table_name, \
    get_pg_foreign_key_name
from spinta.cli.helpers.migrate import MigrateRename
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
    new_table_name = get_pg_table_name(rename.get_table_name(old.name))
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
        columns=columns,
        inspector=inspector
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
    _handle_model_unique_constraints(
        old_table=old,
        new_model=new,
        table_name=new_table_name,
        inspector=inspector,
        handler=handler,
        rename=rename,
        meta=model_meta
    )

    # Handle model foreign key constraint (Base `_id`)
    _handle_model_foreign_key_constraints(
        old_table=old,
        new_model=new,
        table_name=new_table_name,
        inspector=inspector,
        handler=handler,
        rename=rename,
        meta=model_meta
    )

    _clean_up_old_constraints(
        old_table=old,
        table_name=new_table_name,
        handler=handler,
        meta=model_meta
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

    table_name = get_pg_table_name(get_table_name(new))
    pkey_type = commands.get_primary_key_type(context, backend)

    columns = []
    for prop in new.properties.values():
        # Ignore deleted / reserved properties
        if prop.name.startswith('_') and prop.name not in ('_id', '_revision'):
            continue

        if isinstance(prop.dtype, File):
            columns += handle_new_file_type(context, backend, inspector, prop, pkey_type, handler)
        else:
            cols = commands.prepare(context, backend, prop)
            if isinstance(cols, list):
                for column in cols:
                    columns.append(column)

            else:
                if isinstance(cols, sa.Column):
                    columns.append(cols)

    # Handle new model's unique constraints
    constraint = _get_new_model_unique_constraint(
        new_model=new
    )
    if constraint is not None:
        columns.append(constraint)

    handler.add_action(ma.CreateTableMigrationAction(
        table_name=table_name,
        columns=[
            sa.Column('_txn', pkey_type, index=True),
            sa.Column('_created', sa.DateTime),
            sa.Column('_updated', sa.DateTime),
            *columns
        ]
    ))

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
    handler: MigrationHandler,
    rename: MigrateRename,
    meta: MigrateModelMeta
):
    if not new_model.unique:
        return

    for property_combination in new_model.unique:
        column_name_list = []
        old_column_name_list = []

        for prop in property_combination:
            for name in get_prop_names(prop):
                column_name_list.append(get_pg_column_name(name))
                old_column_name_list.append(get_pg_column_name(rename.get_old_column_name(new_model.name, name)))

        constraints = inspector.get_unique_constraints(old_table.name)
        constraint_name = get_pg_constraint_name(table_name, column_name_list)

        if meta.unique_constraint_states[constraint_name]:
            continue

        constraint = constraint_with_name(constraints, constraint_name)
        if constraint:
            meta.handle_unique_constraint(constraint_name)
            if constraint['column_names'] == column_name_list:
                continue

            handler.add_action(ma.DropConstraintMigrationAction(
                table_name=table_name,
                constraint_name=constraint_name,
            ))
            handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                table_name=table_name,
                constraint_name=constraint_name,
                columns=column_name_list
            ))
            continue

        for constraint in constraints:
            if constraint['column_names'] == old_column_name_list:
                if meta.unique_constraint_states[constraint['name']]:
                    continue

                meta.handle_unique_constraint(constraint_name)
                if constraint['name'] == constraint_name:
                    continue

                meta.handle_unique_constraint(constraint['name'])
                handler.add_action(ma.RenameConstraintMigrationAction(
                    table_name=table_name,
                    old_constraint_name=constraint['name'],
                    new_constraint_name=constraint_name
                ))

        if not meta.unique_constraint_states[constraint_name]:
            meta.handle_unique_constraint(constraint_name)
            handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                table_name=table_name,
                constraint_name=constraint_name,
                columns=column_name_list
            ))


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


def _clean_up_old_constraints(
    old_table: sa.Table,
    table_name: str,
    handler: MigrationHandler,
    meta: MigrateModelMeta
):
    # Ignore deleted tables
    if old_table.name.startswith('_'):
        return

    for constraint, state in meta.unique_constraint_states.items():
        if not state:
            meta.handle_unique_constraint(constraint)
            handler.add_action(ma.DropConstraintMigrationAction(
                table_name=table_name,
                constraint_name=constraint
            ))

    for constraint, state in meta.foreign_constraint_states.items():
        if not state:
            meta.handle_foreign_constraint(constraint)
            handler.add_action(ma.DropConstraintMigrationAction(
                table_name=table_name,
                constraint_name=constraint
            ), foreign_key=True)

    for index, state in meta.index_states.items():
        if not state:
            meta.handle_index(index)
            handler.add_action(ma.DropIndexMigrationAction(
                table_name=table_name,
                index_name=index
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
    adjusted_kwargs = adjust_kwargs(kwargs, {
        "model_meta": model_meta
    })
    for json_meta in model_meta.json_columns.values():
        if json_meta.new_keys and json_meta.cast_to is None:
            removed_keys = [key for key, new_key in json_meta.new_keys.items() if new_key == get_pg_removed_name(key)]

            if removed_keys == json_meta.keys or json_meta.full_remove:
                commands.migrate(context, backend, meta, old_table, json_meta.column, NA, **adjusted_kwargs)
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
            commands.migrate(context, backend, meta, old_table, json_meta.column, NA, **adjusted_kwargs)
            commands.migrate(context, backend, meta, old_table, NA, new_column, **adjusted_kwargs)
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


def _get_new_model_unique_constraint(
    new_model: Model
):
    if not new_model.unique:
        return

    for property_combination in new_model.unique:
        column_name_list = []
        for prop in property_combination:
            for name in get_prop_names(prop):
                column_name_list.append(get_pg_name(name))

        return sa.UniqueConstraint(*column_name_list)


def _handle_model_foreign_key_constraints(
    old_table: sa.Table,
    new_model: Model,
    table_name: str,
    inspector: Inspector,
    handler: MigrationHandler,
    rename: MigrateRename,
    meta: MigrateModelMeta
):
    foreign_keys = inspector.get_foreign_keys(old_table.name)
    id_constraint = next(
        (constraint for constraint in foreign_keys if constraint.get("constrained_columns") == ["_id"]), None
    )

    if new_model.base and commands.identifiable(new_model.base):
        referent_table = get_pg_table_name(rename.get_old_table_name(get_table_name(new_model.base.parent)))
        fk_name = get_pg_foreign_key_name(referent_table, "_id")
        meta.handle_foreign_constraint(fk_name)
        if id_constraint is not None:
            if id_constraint["name"] == fk_name:
                # Everything matches
                if id_constraint["referred_table"] == referent_table:
                    return

                # Name matches, but tables don't
                handler.add_action(ma.DropConstraintMigrationAction(
                    table_name=table_name,
                    constraint_name=id_constraint["name"]
                ), True)
                handler.add_action(
                    ma.CreateForeignKeyMigrationAction(
                        source_table=table_name,
                        referent_table=referent_table,
                        constraint_name=fk_name,
                        local_cols=["_id"],
                        remote_cols=["_id"]
                    ), True
                )
                return

            meta.handle_foreign_constraint(id_constraint["name"])
            # Tables match, but name does not
            if id_constraint["referred_table"] == referent_table:
                handler.add_action(ma.RenameConstraintMigrationAction(
                    table_name,
                    old_constraint_name=id_constraint["name"],
                    new_constraint_name=fk_name
                ))
                return

            # Nothing matches, but foreign key with _id exists
            handler.add_action(ma.DropConstraintMigrationAction(
                table_name=table_name,
                constraint_name=id_constraint["name"]
            ), True)
            handler.add_action(
                ma.CreateForeignKeyMigrationAction(
                    source_table=table_name,
                    referent_table=referent_table,
                    constraint_name=fk_name,
                    local_cols=["_id"],
                    remote_cols=["_id"]
                ), True
            )
            return

        # If all checks passed, add the constraint
        handler.add_action(
            ma.CreateForeignKeyMigrationAction(
                source_table=table_name,
                referent_table=referent_table,
                constraint_name=fk_name,
                local_cols=["_id"],
                remote_cols=["_id"]
            ), True
        )
