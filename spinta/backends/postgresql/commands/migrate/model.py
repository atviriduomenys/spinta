import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends import Backend
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_name, get_pg_sequence_name
from spinta.backends.postgresql.helpers.migrate.actions import MigrationHandler
from spinta.backends.postgresql.helpers.migrate.migrate import (
    drop_all_indexes_and_constraints,
    get_prop_names,
    PostgresqlMigrationContext,
    ModelMigrationContext,
    zip_and_migrate_properties,
    constraint_with_name,
    RenameMap,
    PropertyMigrationContext,
    ModelTables,
    create_table_migration,
    filter_related_tables,
)
from spinta.backends.postgresql.helpers.name import (
    name_changed,
    get_pg_column_name,
    get_pg_constraint_name,
    get_pg_removed_name,
    is_removed,
    get_pg_table_name,
    get_pg_foreign_key_name,
    get_removed_name,
)
from spinta.components import Context, Model
from spinta.utils.itertools import ensure_list
from spinta.utils.schema import NotAvailable, NA


@commands.migrate.register(Context, PostgreSQL, PostgresqlMigrationContext, ModelTables, Model)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    old: ModelTables,
    new: Model,
    **kwargs,
):
    source_table = old.main_table
    target_table = backend.get_table(new)
    # Clean up loose tables that could have been picked up by zipper
    if source_table is None:
        commands.migrate(context, backend, migration_ctx, NA, new, **kwargs)
        return

    rename = migration_ctx.rename
    handler = migration_ctx.handler
    inspector = migration_ctx.inspector

    columns = list(source_table.columns)
    new_table_name = target_table.name
    old_table_name = source_table.name

    # Handle table renaming
    if name_changed(old_table_name, new_table_name):
        handler.add_action(
            ma.RenameTableMigrationAction(
                old_table_name=old_table_name,
                new_table_name=new_table_name,
                comment=target_table.comment,
            )
        )

    _handle_reserved_tables(
        backend=backend,
        model=new,
        model_tables=old,
        handler=handler,
    )

    properties = list(new.properties.values())

    model_ctx = ModelMigrationContext(model=new, model_tables=old)
    model_ctx.initialize(inspector=inspector)

    # Handle property migrations
    zip_and_migrate_properties(
        context=context,
        backend=backend,
        source_table=source_table,
        model=new,
        old_columns=columns,
        new_properties=properties,
        migration_context=migration_ctx,
        rename=rename,
        model_context=model_ctx,
        **kwargs,
    )

    # Handle model unique constraint
    _handle_model_unique_constraints(
        source_table=source_table,
        target_table=target_table,
        model=new,
        inspector=inspector,
        handler=handler,
        rename=rename,
        model_context=model_ctx,
    )

    # Handle model foreign key constraint (Base `_id`)
    _handle_model_foreign_key_constraints(
        source_table=source_table,
        target_table=target_table,
        model=new,
        inspector=inspector,
        handler=handler,
        rename=rename,
        model_context=model_ctx,
    )

    _clean_up_old_constraints(
        source_table=source_table,
        target_table=target_table,
        handler=handler,
        model_context=model_ctx,
    )

    # Handle JSON migrations, that need to be run at the end
    _handle_json_column_migrations(
        context=context,
        backend=backend,
        migration_context=migration_ctx,
        model_context=model_ctx,
        target_table=target_table,
        handler=handler,
        **kwargs,
    )

    # Clean up property tables, when property no longer exists (column clean up does not know if there are additional tables left)
    _clean_up_property_tables(
        backend=backend,
        model=new,
        model_tables=old,
        handler=handler,
        inspector=inspector,
        rename=rename,
    )


@commands.migrate.register(Context, PostgreSQL, PostgresqlMigrationContext, NotAvailable, Model)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    old: NotAvailable,
    new: Model,
    **kwargs,
):
    handler = migration_ctx.handler
    related_tables = filter_related_tables(new, backend.tables)
    for table in related_tables.values():
        create_table_migration(table=table, handler=handler)


@commands.migrate.register(Context, PostgreSQL, PostgresqlMigrationContext, ModelTables, NotAvailable)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    old: ModelTables,
    new: NotAvailable,
):
    handler = migration_ctx.handler
    inspector = migration_ctx.inspector

    source_table = old.main_table

    # Skip the already deleted table
    if is_removed(source_table.name):
        return

    # Remove soft deleted tables if they exist
    removed_table_name = get_pg_removed_name(source_table.comment)
    if inspector.has_table(removed_table_name):
        # Drop table if it was already flagged for deletion
        handler.add_action(ma.DropTableMigrationAction(table_name=removed_table_name))

    for table_type, table in old.reserved.items():
        removed_name = get_pg_removed_name(table.comment)
        if inspector.has_table(removed_name):
            handler.add_action(ma.DropTableMigrationAction(table_name=removed_name))

    for table_type, table in old.property_tables.values():
        removed_name = get_pg_removed_name(table.comment, remove_model_only=True)
        if inspector.has_table(removed_name):
            handler.add_action(ma.DropTableMigrationAction(table_name=removed_name))

    # Soft delete all related tables and drop constraints / indexes
    handler.add_action(
        ma.RenameTableMigrationAction(
            old_table_name=source_table.name,
            new_table_name=removed_table_name,
            comment=get_removed_name(source_table.comment),
        )
    )
    drop_all_indexes_and_constraints(inspector, source_table.name, removed_table_name, handler)

    for table_type, table in old.reserved.items():
        removed_name = get_pg_removed_name(table.comment)
        handler.add_action(
            ma.RenameTableMigrationAction(
                old_table_name=table.name,
                new_table_name=removed_name,
                comment=get_removed_name(table.comment),
            )
        )
        sequence_name = get_pg_sequence_name(table.name)
        if inspector.has_sequence(sequence_name):
            handler.add_action(
                ma.RenameSequenceMigrationAction(old_name=sequence_name, new_name=get_pg_sequence_name(removed_name))
            )
        drop_all_indexes_and_constraints(inspector, table.name, removed_name, handler)

    for table_type, table in old.property_tables.values():
        removed_name = get_pg_removed_name(table.comment, remove_model_only=True)
        handler.add_action(
            ma.RenameTableMigrationAction(
                old_table_name=table.name,
                new_table_name=removed_name,
                comment=get_removed_name(table.comment, remove_model_only=True),
            )
        )
        drop_all_indexes_and_constraints(inspector, table.name, removed_name, handler)


def _handle_model_unique_constraints(
    source_table: sa.Table,
    target_table: sa.Table,
    model: Model,
    inspector: Inspector,
    handler: MigrationHandler,
    rename: RenameMap,
    model_context: ModelMigrationContext,
):
    if not model.unique:
        return

    table_name = target_table.name

    for property_combination in model.unique:
        column_name_list = []
        old_column_name_list = []

        for prop in property_combination:
            for name in get_prop_names(prop):
                column_name_list.append(get_pg_column_name(name))
                old_column_name_list.append(get_pg_column_name(rename.get_old_column_name(source_table, name)))

        constraints = inspector.get_unique_constraints(source_table.name)
        constraint_name = get_pg_constraint_name(table_name, column_name_list)
        if model_context.unique_constraint_states[constraint_name]:
            continue

        constraint = constraint_with_name(constraints, constraint_name)
        if constraint:
            model_context.mark_unique_constraint_handled(constraint_name)
            if constraint["column_names"] == column_name_list:
                continue

            handler.add_action(
                ma.DropConstraintMigrationAction(
                    table_name=table_name,
                    constraint_name=constraint_name,
                )
            )
            handler.add_action(
                ma.CreateUniqueConstraintMigrationAction(
                    table_name=table_name, constraint_name=constraint_name, columns=column_name_list
                )
            )
            continue

        for constraint in constraints:
            if constraint["column_names"] == old_column_name_list:
                if model_context.unique_constraint_states[constraint["name"]]:
                    continue

                model_context.mark_unique_constraint_handled(constraint_name)
                if constraint["name"] == constraint_name:
                    continue

                model_context.mark_unique_constraint_handled(constraint["name"])
                handler.add_action(
                    ma.RenameConstraintMigrationAction(
                        table_name=table_name,
                        old_constraint_name=constraint["name"],
                        new_constraint_name=constraint_name,
                    )
                )

        if not model_context.unique_constraint_states[constraint_name]:
            model_context.mark_unique_constraint_handled(constraint_name)
            handler.add_action(
                ma.CreateUniqueConstraintMigrationAction(
                    table_name=table_name, constraint_name=constraint_name, columns=column_name_list
                )
            )


def _handle_property_unique_constraints(context: Context, backend: PostgreSQL, new_model: Model) -> list:
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
    source_table: sa.Table,
    target_table: sa.Table,
    handler: MigrationHandler,
    model_context: ModelMigrationContext,
):
    # Ignore deleted tables
    if source_table.name.startswith("_"):
        return

    table_name = target_table.name

    for constraint, state in model_context.unique_constraint_states.items():
        if not state:
            model_context.mark_unique_constraint_handled(constraint)
            handler.add_action(ma.DropConstraintMigrationAction(table_name=table_name, constraint_name=constraint))

    for constraint, state in model_context.foreign_constraint_states.items():
        if not state:
            model_context.mark_foreign_constraint_handled(constraint)
            handler.add_action(
                ma.DropConstraintMigrationAction(table_name=table_name, constraint_name=constraint), foreign_key=True
            )

    for index, state in model_context.index_states.items():
        if not state:
            model_context.mark_index_handled(index)
            handler.add_action(ma.DropIndexMigrationAction(table_name=table_name, index_name=index))


def _handle_json_column_migrations(
    context: Context,
    backend: Backend,
    migration_context: PostgresqlMigrationContext,
    model_context: ModelMigrationContext,
    target_table: sa.Table,
    handler: MigrationHandler,
    **kwargs,
):
    table_name = target_table.name

    for json_context in model_context.json_columns.values():
        property_ctx = PropertyMigrationContext(prop=json_context.prop, model_context=model_context)
        if json_context.new_keys and json_context.cast_to is None:
            removed_keys = [
                key for key, new_key in json_context.new_keys.items() if new_key == get_pg_removed_name(key)
            ]
            if removed_keys == json_context.keys or json_context.full_remove:
                commands.migrate(context, backend, migration_context, property_ctx, json_context.column, NA, **kwargs)
            else:
                for old_key, new_key in json_context.new_keys.items():
                    if new_key in json_context.keys:
                        handler.add_action(
                            ma.RemoveJSONAttributeMigrationAction(table_name, json_context.column, new_key)
                        )
                    handler.add_action(
                        ma.RenameJSONAttributeMigrationAction(table_name, json_context.column, old_key, new_key)
                    )
        elif isinstance(json_context.cast_to, tuple) and len(json_context.cast_to) == 2:
            new_column = json_context.cast_to[0]
            key = json_context.cast_to[1]
            # Remove old column
            commands.migrate(context, backend, migration_context, property_ctx, json_context.column, NA, **kwargs)
            # Create new empty json column
            commands.migrate(context, backend, migration_context, property_ctx, NA, new_column, **kwargs)

            renamed = json_context.column._copy()
            renamed.name = get_pg_removed_name(json_context.column.name)
            renamed.key = get_pg_removed_name(json_context.column.name)
            handler.add_action(ma.TransferJSONDataMigrationAction(table_name, renamed, [(key, new_column)]))
        # Rename column
        if json_context.new_name:
            handler.add_action(
                ma.AlterColumnMigrationAction(
                    table_name,
                    json_context.column.name,
                    new_column_name=json_context.new_name,
                    comment=json_context.comment,
                )
            )


def _get_new_model_unique_constraint(new_model: Model):
    if not new_model.unique:
        return None

    for property_combination in new_model.unique:
        column_name_list = []
        for prop in property_combination:
            for name in get_prop_names(prop):
                column_name_list.append(get_pg_name(name))

        return sa.UniqueConstraint(*column_name_list)
    return None


def _handle_model_foreign_key_constraints(
    source_table: sa.Table,
    target_table: sa.Table,
    model: Model,
    inspector: Inspector,
    handler: MigrationHandler,
    rename: RenameMap,
    model_context: ModelMigrationContext,
):
    table_name = target_table.name
    foreign_keys = inspector.get_foreign_keys(source_table.name)
    id_constraint = next(
        (constraint for constraint in foreign_keys if constraint.get("constrained_columns") == ["_id"]), None
    )

    if model.base and commands.identifiable(model.base):
        referent_table = get_pg_table_name(rename.get_old_table_name(get_table_name(model.base.parent)))
        fk_name = get_pg_foreign_key_name(referent_table, "_id")
        model_context.mark_foreign_constraint_handled(fk_name)
        if id_constraint is not None:
            if id_constraint["name"] == fk_name:
                # Everything matches
                if id_constraint["referred_table"] == referent_table:
                    return

                # Name matches, but tables don't
                handler.add_action(
                    ma.DropConstraintMigrationAction(table_name=table_name, constraint_name=id_constraint["name"]), True
                )
                handler.add_action(
                    ma.CreateForeignKeyMigrationAction(
                        source_table=table_name,
                        referent_table=referent_table,
                        constraint_name=fk_name,
                        local_cols=["_id"],
                        remote_cols=["_id"],
                    ),
                    True,
                )
                return

            model_context.mark_foreign_constraint_handled(id_constraint["name"])
            # Tables match, but name does not
            if id_constraint["referred_table"] == referent_table:
                handler.add_action(
                    ma.RenameConstraintMigrationAction(
                        table_name, old_constraint_name=id_constraint["name"], new_constraint_name=fk_name
                    )
                )
                return

            # Nothing matches, but foreign key with _id exists
            handler.add_action(
                ma.DropConstraintMigrationAction(table_name=table_name, constraint_name=id_constraint["name"]), True
            )
            handler.add_action(
                ma.CreateForeignKeyMigrationAction(
                    source_table=table_name,
                    referent_table=referent_table,
                    constraint_name=fk_name,
                    local_cols=["_id"],
                    remote_cols=["_id"],
                ),
                True,
            )
            return

        # If all checks passed, add the constraint
        handler.add_action(
            ma.CreateForeignKeyMigrationAction(
                source_table=table_name,
                referent_table=referent_table,
                constraint_name=fk_name,
                local_cols=["_id"],
                remote_cols=["_id"],
            ),
            True,
        )


def _handle_reserved_tables(
    backend: PostgreSQL,
    model: Model,
    model_tables: ModelTables,
    handler: MigrationHandler,
):
    # Reserved models should not create additional reserved tables
    if model.name.startswith("_"):
        return

    _handle_changelog_migration(backend=backend, model=model, model_tables=model_tables, handler=handler)
    _handle_redirect_migration(backend=backend, model=model, model_tables=model_tables, handler=handler)


def _handle_changelog_migration(
    backend: PostgreSQL,
    model: Model,
    model_tables: ModelTables,
    handler: MigrationHandler,
):
    changelog_table = model_tables.reserved.get(TableType.CHANGELOG)
    target_table = backend.get_table(model, TableType.CHANGELOG)
    if changelog_table is None:
        create_table_migration(table=target_table, handler=handler)
    else:
        changelog_table_name = get_table_name(model, TableType.CHANGELOG)
        pg_changelog_table_name = get_pg_name(changelog_table_name)
        if changelog_table.name != pg_changelog_table_name:
            handler.add_action(
                ma.RenameTableMigrationAction(
                    old_table_name=changelog_table.name,
                    new_table_name=pg_changelog_table_name,
                    comment=changelog_table_name,
                )
            ).add_action(
                ma.RenameSequenceMigrationAction(
                    old_name=get_pg_sequence_name(changelog_table.name),
                    new_name=get_pg_sequence_name(pg_changelog_table_name),
                )
            )


def _handle_redirect_migration(
    backend: PostgreSQL,
    model: Model,
    model_tables: ModelTables,
    handler: MigrationHandler,
):
    redirect_table = model_tables.reserved.get(TableType.REDIRECT)
    target_table = backend.get_table(model, TableType.REDIRECT)
    if redirect_table is None:
        create_table_migration(table=target_table, handler=handler)
    else:
        redirect_table_name = get_table_name(model, TableType.REDIRECT)
        pg_redirect_table_name = get_pg_name(redirect_table_name)
        if redirect_table.name != pg_redirect_table_name:
            handler.add_action(
                ma.RenameTableMigrationAction(
                    old_table_name=redirect_table.name,
                    new_table_name=pg_redirect_table_name,
                    comment=redirect_table_name,
                )
            )


def _clean_up_property_tables(
    backend: PostgreSQL,
    model_tables: ModelTables,
    model: Model,
    rename: RenameMap,
    inspector: Inspector,
    handler: MigrationHandler,
):
    source_table = model_tables.main_table
    for prop_name, (table_type, table) in model_tables.property_tables.items():
        column_name = rename.get_column_name(source_table, prop_name)
        required_table_name = model.model_type() + table_type.value + "/" + column_name
        if required_table_name not in backend.tables:
            removed_table_name = get_pg_removed_name(table.name)
            if inspector.has_table(removed_table_name):
                handler.add_action(ma.DropTableMigrationAction(table_name=removed_table_name))
            handler.add_action(
                ma.RenameTableMigrationAction(
                    old_table_name=table.name,
                    new_table_name=removed_table_name,
                    comment=get_removed_name(table.comment),
                )
            )
            drop_all_indexes_and_constraints(inspector, table.name, removed_table_name, handler)
