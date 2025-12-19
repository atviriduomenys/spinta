import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends import Backend
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_identifier, TableIdentifier
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
    PropertyMigrationContext,
    ModelTables,
    create_table_migration,
    filter_related_tables,
    gather_prepare_columns,
)
from spinta.backends.postgresql.helpers.migrate.name import RenameMap
from spinta.backends.postgresql.helpers.name import (
    name_changed,
    get_pg_column_name,
    get_pg_constraint_name,
    get_pg_removed_name,
    is_removed,
    get_pg_foreign_key_name,
    get_removed_name,
    get_pg_index_name,
)
from spinta.components import Context, Model
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
    # Clean up loose tables that could have been picked up by zipper
    if source_table is None:
        commands.migrate(context, backend, migration_ctx, NA, new, **kwargs)
        return

    source_table_identifier = migration_ctx.get_table_identifier(source_table)
    target_table = backend.get_table(new)
    target_table_identifier = migration_ctx.get_table_identifier(new)

    rename = migration_ctx.rename
    handler = migration_ctx.handler
    inspector = migration_ctx.inspector

    columns = list(source_table.columns)

    # Handle table rename
    if name_changed(source_table_identifier.pg_qualified_name, target_table_identifier.pg_qualified_name):
        handler.add_action(
            ma.RenameTableMigrationAction(
                old_table_identifier=source_table_identifier,
                new_table_identifier=target_table_identifier,
                comment=target_table.comment,
            )
        )

    properties = list(new.properties.values())

    model_ctx = ModelMigrationContext(model=new, model_tables=old)
    model_ctx.initialize(inspector=inspector)

    _handle_reserved_tables(
        backend=backend,
        model=new,
        model_tables=old,
        model_context=model_ctx,
        handler=handler,
        inspector=inspector,
    )

    # Handle property migrations
    zip_and_migrate_properties(
        context=context,
        backend=backend,
        source_table=source_table,
        model=new,
        old_columns=columns,
        new_properties=properties,
        migration_context=migration_ctx,
        model_context=model_ctx,
        **kwargs,
    )

    _handle_model_reserved_properties(
        context=context,
        backend=backend,
        model_context=model_ctx,
        source_table=source_table,
        target_table=target_table,
        migration_ctx=migration_ctx,
        **kwargs,
    )

    # Handle model unique constraint
    _handle_model_unique_constraints(
        source_table_identifier=source_table_identifier,
        target_table_identifier=target_table_identifier,
        model=new,
        inspector=inspector,
        handler=handler,
        rename=rename,
        model_context=model_ctx,
    )

    # Handle model foreign key constraint (Base `_id`)
    _handle_model_foreign_key_constraints(
        source_table_identifier=source_table_identifier,
        target_table_identifier=target_table_identifier,
        model=new,
        inspector=inspector,
        handler=handler,
        rename=rename,
        model_context=model_ctx,
    )

    # Handle JSON migrations, that need to be run at the end
    _handle_json_column_migrations(
        context=context,
        backend=backend,
        migration_context=migration_ctx,
        model_context=model_ctx,
        target_table_identifier=target_table_identifier,
        handler=handler,
        **kwargs,
    )

    # Clean up property tables, when property no longer exists (column clean up does not know if there are additional tables left)
    _clean_up_property_tables(
        backend=backend,
        source_table_identifier=source_table_identifier,
        model=new,
        model_tables=old,
        handler=handler,
        inspector=inspector,
        rename=rename,
        model_context=model_ctx,
        migration_ctx=migration_ctx,
    )

    _clean_up_old_constraints(
        source_table_identifier=source_table_identifier,
        handler=handler,
        model_context=model_ctx,
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
    **kwargs,
):
    handler = migration_ctx.handler
    inspector = migration_ctx.inspector

    source_table = old.main_table

    # Skip the already deleted table
    if is_removed(source_table.name):
        return

    # Precalculate removed table identifiers
    removed_table_identifiers = []
    table_identifier = migration_ctx.get_table_identifier(source_table)
    removed_table_identifiers.append((table_identifier, table_identifier.apply_removed_prefix()))
    for table in old.reserved.values():
        table_identifier = migration_ctx.get_table_identifier(table)
        removed_table_identifiers.append((table_identifier, table_identifier.apply_removed_prefix()))

    for _, table in old.property_tables.values():
        table_identifier = migration_ctx.get_table_identifier(table)
        removed_table_identifiers.append(
            (table_identifier, table_identifier.apply_removed_prefix(remove_model_only=True))
        )

    for table_identifier, removed_table_identifier in removed_table_identifiers:
        # Remove soft deleted tables if they exist
        if inspector.has_table(removed_table_identifier.pg_table_name, schema=removed_table_identifier.pg_schema_name):
            handler.add_action(ma.DropTableMigrationAction(table_identifier=removed_table_identifier))

        handler.add_action(
            ma.RenameTableMigrationAction(
                old_table_identifier=table_identifier,
                new_table_identifier=removed_table_identifier,
                comment=get_removed_name(source_table.comment),
            )
        )
        sequence_name = get_pg_sequence_name(table_identifier.pg_table_name)
        if inspector.has_sequence(sequence_name, schema=table_identifier.pg_schema_name):
            handler.add_action(
                ma.RenameSequenceMigrationAction(
                    old_name=sequence_name,
                    new_name=get_pg_sequence_name(removed_table_identifier.pg_table_name),
                    table_identifier=table_identifier,
                )
            )
        drop_all_indexes_and_constraints(inspector, source_table, removed_table_identifier, handler)


def _handle_model_unique_constraints(
    source_table_identifier: TableIdentifier,
    target_table_identifier: TableIdentifier,
    model: Model,
    inspector: Inspector,
    handler: MigrationHandler,
    rename: RenameMap,
    model_context: ModelMigrationContext,
):
    if not model.unique:
        return

    source_table_name = source_table_identifier.pg_table_name

    for property_combination in model.unique:
        column_name_list = []
        old_column_name_list = []

        for prop in property_combination:
            for name in get_prop_names(prop):
                column_name_list.append(get_pg_column_name(name))
                old_column_name_list.append(
                    get_pg_column_name(rename.to_old_column_name(source_table_identifier, name))
                )

        constraints = inspector.get_unique_constraints(source_table_name, schema=source_table_identifier.pg_schema_name)
        constraint_name = get_pg_constraint_name(source_table_name, column_name_list)
        unique_constraint_states = model_context.constraint_states[source_table_name].unique_constraint
        if unique_constraint_states[constraint_name]:
            continue

        constraint = constraint_with_name(constraints, constraint_name)
        if constraint:
            model_context.mark_unique_constraint_handled(source_table_name, constraint_name)
            if constraint["column_names"] == column_name_list:
                continue

            handler.add_action(
                ma.DropConstraintMigrationAction(
                    table_identifier=target_table_identifier,
                    constraint_name=constraint_name,
                )
            )
            handler.add_action(
                ma.CreateUniqueConstraintMigrationAction(
                    table_identifier=target_table_identifier, constraint_name=constraint_name, columns=column_name_list
                )
            )
            continue

        for constraint in constraints:
            if constraint["column_names"] == old_column_name_list:
                if unique_constraint_states[constraint["name"]]:
                    continue

                model_context.mark_unique_constraint_handled(source_table_name, constraint_name)
                if constraint["name"] == constraint_name:
                    continue

                model_context.mark_unique_constraint_handled(source_table_name, constraint["name"])
                handler.add_action(
                    ma.RenameConstraintMigrationAction(
                        table_identifier=target_table_identifier,
                        old_constraint_name=constraint["name"],
                        new_constraint_name=constraint_name,
                    )
                )

        if not unique_constraint_states[constraint_name]:
            model_context.mark_unique_constraint_handled(source_table_name, constraint_name)
            handler.add_action(
                ma.CreateUniqueConstraintMigrationAction(
                    table_identifier=target_table_identifier, constraint_name=constraint_name, columns=column_name_list
                )
            )


def _handle_property_unique_constraints(context: Context, backend: PostgreSQL, new_model: Model) -> list:
    required_unique_constraints = []
    for prop in new_model.flatprops.values():
        if not prop.dtype.unique:
            continue

        columns = gather_prepare_columns(context, backend, prop)
        column_name_list = [column.name for column in columns]

        if column_name_list:
            required_unique_constraints.append(column_name_list)
    return required_unique_constraints


def _clean_up_old_constraints(
    source_table_identifier: TableIdentifier,
    handler: MigrationHandler,
    model_context: ModelMigrationContext,
):
    # Ignore deleted tables
    if source_table_identifier.pg_table_name.startswith("_"):
        return

    for constrained_table, constraint_states in model_context.constraint_states.items():
        table_identifier = source_table_identifier.change_table_type(
            new_type=constraint_states.table_type, table_arg=constraint_states.prop
        )

        for constraint, state in constraint_states.unique_constraint.items():
            if state:
                continue
            model_context.mark_unique_constraint_handled(constrained_table, constraint)
            handler.add_action(
                ma.DropConstraintMigrationAction(table_identifier=table_identifier, constraint_name=constraint)
            )

        for constraint, state in constraint_states.foreign_constraint.items():
            if state:
                continue
            model_context.mark_foreign_constraint_handled(constrained_table, constraint)
            handler.add_action(
                ma.DropConstraintMigrationAction(table_identifier=table_identifier, constraint_name=constraint),
                foreign_key=True,
            )

        for index, state in constraint_states.index.items():
            if state:
                continue
            model_context.mark_index_handled(constrained_table, index)
            handler.add_action(
                ma.DropIndexMigrationAction(
                    table_identifier=table_identifier,
                    index_name=index,
                )
            )


def _handle_json_column_migrations(
    context: Context,
    backend: Backend,
    migration_context: PostgresqlMigrationContext,
    model_context: ModelMigrationContext,
    target_table_identifier: TableIdentifier,
    handler: MigrationHandler,
    **kwargs,
):
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
                            ma.RemoveJSONAttributeMigrationAction(
                                table_identifier=target_table_identifier, source=json_context.column, key=new_key
                            )
                        )
                    handler.add_action(
                        ma.RenameJSONAttributeMigrationAction(
                            table_identifier=target_table_identifier,
                            source=json_context.column,
                            old_key=old_key,
                            new_key=new_key,
                        )
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
            handler.add_action(
                ma.TransferJSONDataMigrationAction(
                    table_identifier=target_table_identifier, source=renamed, columns=[(key, new_column)]
                )
            )
        # Rename column
        if json_context.new_name:
            handler.add_action(
                ma.AlterColumnMigrationAction(
                    table_identifier=target_table_identifier,
                    column_name=json_context.column.name,
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
    source_table_identifier: TableIdentifier,
    target_table_identifier: TableIdentifier,
    model: Model,
    inspector: Inspector,
    handler: MigrationHandler,
    rename: RenameMap,
    model_context: ModelMigrationContext,
):
    # table_name = target_table.name
    foreign_keys = inspector.get_foreign_keys(
        source_table_identifier.pg_table_name, schema=source_table_identifier.pg_schema_name
    )
    id_constraint = next(
        (constraint for constraint in foreign_keys if constraint.get("constrained_columns") == ["_id"]), None
    )

    if model.base and commands.identifiable(model.base):
        referent_table_identifier = rename.to_old_table(model.base.parent)
        referent_table = referent_table_identifier.pg_table_name
        referent_schema = referent_table_identifier.pg_schema_name
        fk_name = get_pg_foreign_key_name(referent_table, "_id")
        model_context.mark_foreign_constraint_handled(source_table_identifier.pg_table_name, fk_name)
        if id_constraint is not None:
            if id_constraint["name"] == fk_name:
                # Everything matches
                if (
                    id_constraint["referred_schema"] == referent_schema
                    and id_constraint["referred_table"] == referent_table
                ):
                    return

                # Name matches, but tables don't
                handler.add_action(
                    ma.DropConstraintMigrationAction(
                        table_identifier=target_table_identifier, constraint_name=id_constraint["name"]
                    ),
                    True,
                )
                handler.add_action(
                    ma.CreateForeignKeyMigrationAction(
                        source_table_identifier=target_table_identifier,
                        referent_table_identifier=referent_table_identifier,
                        constraint_name=fk_name,
                        local_cols=["_id"],
                        remote_cols=["_id"],
                    ),
                    True,
                )
                return

            model_context.mark_foreign_constraint_handled(source_table_identifier.pg_table_name, id_constraint["name"])
            # Tables match, but name does not
            if (
                id_constraint["referred_schema"] == referent_schema
                and id_constraint["referred_table"] == referent_table
            ):
                handler.add_action(
                    ma.RenameConstraintMigrationAction(
                        table_identifier=target_table_identifier,
                        old_constraint_name=id_constraint["name"],
                        new_constraint_name=fk_name,
                    )
                )
                return

            # Nothing matches, but foreign key with _id exists
            handler.add_action(
                ma.DropConstraintMigrationAction(
                    table_identifier=target_table_identifier, constraint_name=id_constraint["name"]
                ),
                True,
            )
            handler.add_action(
                ma.CreateForeignKeyMigrationAction(
                    source_table_identifier=target_table_identifier,
                    referent_table_identifier=referent_table_identifier,
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
                source_table_identifier=target_table_identifier,
                referent_table_identifier=referent_table_identifier,
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
    model_context: ModelMigrationContext,
    handler: MigrationHandler,
    inspector: Inspector,
):
    # Reserved models should not create additional reserved tables
    if model.name.startswith("_"):
        return

    _handle_changelog_migration(
        backend=backend,
        model=model,
        model_tables=model_tables,
        handler=handler,
        inspector=inspector,
        model_context=model_context,
    )
    _handle_redirect_migration(
        backend=backend,
        model=model,
        model_tables=model_tables,
        handler=handler,
        model_context=model_context,
        inspector=inspector,
    )


def _handle_changelog_migration(
    backend: PostgreSQL,
    model: Model,
    model_tables: ModelTables,
    model_context: ModelMigrationContext,
    handler: MigrationHandler,
    inspector: Inspector,
):
    changelog_table = model_tables.reserved.get(TableType.CHANGELOG)
    target_table = backend.get_table(model, TableType.CHANGELOG)
    if changelog_table is None:
        create_table_migration(table=target_table, handler=handler)
    else:
        old_table_identifier = get_table_identifier(changelog_table)
        new_table_identifier = get_table_identifier(model, TableType.CHANGELOG)
        renamed = name_changed(old_table_identifier.pg_qualified_name, new_table_identifier.pg_qualified_name)
        if renamed:
            handler.add_action(
                ma.RenameTableMigrationAction(
                    old_table_identifier=old_table_identifier,
                    new_table_identifier=new_table_identifier,
                    comment=new_table_identifier.logical_qualified_name,
                )
            ).add_action(
                ma.RenameSequenceMigrationAction(
                    old_name=get_pg_sequence_name(changelog_table.name),
                    new_name=get_pg_sequence_name(new_table_identifier.pg_table_name),
                    table_identifier=new_table_identifier,
                    old_table_identifier=old_table_identifier,
                )
            )

        _generic_reserved_table_constraint_flag(
            source_table_identifier=old_table_identifier,
            target_table_identifier=new_table_identifier,
            model_context=model_context,
            handler=handler,
            inspector=inspector,
        )


def _handle_redirect_migration(
    backend: PostgreSQL,
    model: Model,
    model_tables: ModelTables,
    model_context: ModelMigrationContext,
    handler: MigrationHandler,
    inspector: Inspector,
):
    redirect_table = model_tables.reserved.get(TableType.REDIRECT)
    target_table = backend.get_table(model, TableType.REDIRECT)
    if redirect_table is None:
        create_table_migration(table=target_table, handler=handler)
    else:
        old_table_identifier = get_table_identifier(redirect_table)
        new_table_identifier = get_table_identifier(model, TableType.REDIRECT)
        if name_changed(old_table_identifier.pg_qualified_name, new_table_identifier.pg_qualified_name):
            handler.add_action(
                ma.RenameTableMigrationAction(
                    old_table_identifier=old_table_identifier,
                    new_table_identifier=new_table_identifier,
                    comment=new_table_identifier.logical_qualified_name,
                )
            )

        _generic_reserved_table_constraint_flag(
            source_table_identifier=old_table_identifier,
            target_table_identifier=new_table_identifier,
            model_context=model_context,
            handler=handler,
            inspector=inspector,
        )


def _generic_reserved_table_constraint_flag(
    source_table_identifier: TableIdentifier,
    target_table_identifier: TableIdentifier,
    model_context: ModelMigrationContext,
    handler: MigrationHandler,
    inspector: Inspector,
):
    target_table_name = target_table_identifier.pg_table_name
    source_table_name = source_table_identifier.pg_table_name
    renamed = name_changed(target_table_identifier.pg_qualified_name, source_table_identifier.pg_qualified_name)
    constraint_states = model_context.constraint_states[source_table_name]

    unique_constraints = {
        constraint["name"]: constraint
        for constraint in inspector.get_unique_constraints(
            source_table_name, schema=source_table_identifier.pg_schema_name
        )
    }
    for unique_constraint, state in constraint_states.unique_constraint.items():
        if state:
            continue

        model_context.mark_unique_constraint_handled(source_table_name, unique_constraint)

        if not renamed:
            continue
        new_constraint_name = get_pg_constraint_name(
            target_table_name, unique_constraints[unique_constraint]["column_names"]
        )
        handler.add_action(
            ma.RenameConstraintMigrationAction(
                table_identifier=target_table_identifier,
                old_constraint_name=unique_constraint,
                new_constraint_name=new_constraint_name,
            )
        )

    foreign_constraints = {
        constraint["name"]: constraint
        for constraint in inspector.get_foreign_keys(source_table_name, schema=source_table_identifier.pg_schema_name)
    }
    for foreign_constraint, state in constraint_states.foreign_constraint.items():
        if state:
            continue

        model_context.mark_foreign_constraint_handled(source_table_name, foreign_constraint)
        if not renamed:
            continue
        new_constraint_name = get_pg_constraint_name(
            target_table_name, unique_constraints[foreign_constraints]["column_names"]
        )
        handler.add_action(
            ma.RenameConstraintMigrationAction(
                table_identifier=target_table_identifier,
                old_constraint_name=foreign_constraint,
                new_constraint_name=new_constraint_name,
            )
        )

    indexes = {
        index["name"]: index
        for index in inspector.get_indexes(source_table_name, schema=source_table_identifier.pg_schema_name)
    }
    for index, state in constraint_states.index.items():
        if state:
            continue

        model_context.mark_index_handled(source_table_name, index)
        if not renamed:
            continue
        new_index_name = get_pg_index_name(target_table_name, indexes[index]["column_names"])
        handler.add_action(
            ma.RenameIndexMigrationAction(
                old_index_name=index,
                new_index_name=new_index_name,
                table_identifier=target_table_identifier,
                old_table_identifier=source_table_identifier,
            )
        )


def _clean_up_property_tables(
    backend: PostgreSQL,
    source_table_identifier: TableIdentifier,
    model_tables: ModelTables,
    model: Model,
    rename: RenameMap,
    inspector: Inspector,
    handler: MigrationHandler,
    model_context: ModelMigrationContext,
    migration_ctx: PostgresqlMigrationContext,
):
    for prop_name, (table_type, table) in model_tables.property_tables.items():
        column_name = rename.to_new_column_name(source_table_identifier, prop_name)
        required_table_name = model.model_type() + table_type.value + "/" + column_name
        if required_table_name not in backend.tables:
            table_identifier = migration_ctx.get_table_identifier(table)
            removed_table_identifier = table_identifier.apply_removed_prefix()
            if inspector.has_table(removed_table_identifier.pg_table_name, schema=removed_table_identifier.schema):
                handler.add_action(ma.DropTableMigrationAction(table_identifier=removed_table_identifier))
            handler.add_action(
                ma.RenameTableMigrationAction(
                    old_table_identifier=table_identifier,
                    new_table_identifier=removed_table_identifier,
                    comment=get_removed_name(table.comment),
                )
            )
            drop_all_indexes_and_constraints(inspector, table.name, removed_table_identifier, handler, model_context)


def _handle_model_reserved_properties(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    source_table: sa.Table,
    target_table: sa.Table,
    model_context: ModelMigrationContext,
    **kwargs,
):
    reserved_properties = {"_txn", "_created", "_id", "_revision"}

    for reserved_prop in reserved_properties:
        _generic_reserved_property_migration(
            context=context,
            backend=backend,
            migration_ctx=migration_ctx,
            source_table=source_table,
            target_table=target_table,
            model_context=model_context,
            column_name=reserved_prop,
            **kwargs,
        )


def _generic_reserved_property_migration(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    source_table: sa.Table,
    target_table: sa.Table,
    model_context: ModelMigrationContext,
    column_name: str,
    prop_name: str = None,
    **kwargs,
):
    if prop_name is None:
        prop_name = column_name

    _reserved_source = source_table.columns.get(column_name)
    if _reserved_source is None:
        _reserved_source = NotAvailable

    _reserved_target = target_table.columns.get(column_name)
    if _reserved_target is None:
        _reserved_target = NotAvailable

    commands.migrate(
        context,
        backend,
        migration_ctx,
        PropertyMigrationContext(prop=model_context.model.properties.get(prop_name), model_context=model_context),
        _reserved_source,
        _reserved_target,
        **kwargs,
    )
