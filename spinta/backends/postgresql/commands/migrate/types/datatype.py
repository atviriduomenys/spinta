from __future__ import annotations

from typing import List

import geoalchemy2.types
import sqlalchemy as sa

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.migrate import (
    PostgresqlMigrationContext,
    adjust_kwargs,
    extract_literal_name_from_column,
    handle_unique_constraint_migration,
    contains_unique_constraint,
    handle_index_migration,
    extract_using_from_columns,
    ModelMigrationContext,
    contains_constraint_name,
    constraint_with_columns,
    PropertyMigrationContext,
    column_cast_warning_message,
    gather_prepare_columns,
    get_source_table,
)
from spinta.backends.postgresql.helpers.migrate.cast import CastSupport
from spinta.backends.postgresql.helpers.name import (
    name_changed,
    get_pg_constraint_name,
    get_pg_removed_name,
    get_pg_index_name,
    get_removed_name,
)
from spinta.components import Context
from spinta.exceptions import UnableToCastColumnTypes
from spinta.types.datatype import DataType
from spinta.utils.schema import NotAvailable, NA

from typer import echo


@commands.migrate.register(
    Context, PostgreSQL, PostgresqlMigrationContext, PropertyMigrationContext, NotAvailable, DataType
)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    property_ctx: PropertyMigrationContext,
    old: NotAvailable,
    new: DataType,
    **kwargs,
):
    columns = gather_prepare_columns(context, backend, new.prop, reduce=True)
    if columns is not None and columns != []:
        commands.migrate(context, backend, migration_ctx, property_ctx, old, columns, **kwargs)


@commands.migrate.register(
    Context, PostgreSQL, PostgresqlMigrationContext, PropertyMigrationContext, sa.Column, DataType
)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    property_ctx: PropertyMigrationContext,
    old: sa.Column,
    new: DataType,
    **kwargs,
):
    columns = gather_prepare_columns(context, backend, new.prop, reduce=True)
    commands.migrate(context, backend, migration_ctx, property_ctx, old, columns, **kwargs)


@commands.migrate.register(Context, PostgreSQL, PostgresqlMigrationContext, PropertyMigrationContext, list, DataType)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    property_ctx: PropertyMigrationContext,
    old: List[sa.Column],
    new: DataType,
    **kwargs,
):
    rename = migration_ctx.rename
    column: sa.Column = commands.prepare(context, backend, new.prop)
    old_name = rename.to_old_column_name(property_ctx.model_context.model_tables.base_name, column.name)

    columns = old.copy()

    if name_changed(column.name, old_name):
        for col in old:
            if col.name == column.name:
                commands.migrate(context, backend, migration_ctx, property_ctx, col, NA, **kwargs)
                columns.remove(col)
                break

    for col in columns:
        commands.migrate(context, backend, migration_ctx, property_ctx, col, column, **kwargs)


@commands.migrate.register(
    Context, PostgreSQL, PostgresqlMigrationContext, PropertyMigrationContext, sa.Column, sa.Column
)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    property_ctx: PropertyMigrationContext,
    old: sa.Column,
    new: sa.Column,
    foreign_key: bool = False,
    **kwargs,
):
    inspector = migration_ctx.inspector
    handler = migration_ctx.handler
    cast_matrix = migration_ctx.cast_matrix

    column_name = new.name
    source_table = get_source_table(property_ctx, old)
    source_table_identifier = migration_ctx.get_table_identifier(source_table)
    target_table_identifier = migration_ctx.get_table_identifier(property_ctx.prop)

    old_type = extract_literal_name_from_column(old)
    new_type = extract_literal_name_from_column(new)

    nullable = new.nullable if new.nullable != old.nullable else None
    type_ = new.type if old_type != new_type else None
    new_name = column_name if old.name != new.name else None

    if type_:
        result = cast_matrix.supports(old_type, new_type)
        if result is CastSupport.INVALID:
            if migration_ctx.config.raise_error:
                raise UnableToCastColumnTypes(
                    property_ctx.prop.dtype, column=column_name, old_type=old_type, new_type=new_type
                )
            else:
                echo(column_cast_warning_message(property_ctx.prop.dtype, column_name, old_type, new_type), err=True)
        elif result is CastSupport.UNSAFE:
            echo(column_cast_warning_message(property_ctx.prop.dtype, column_name, old_type, new_type), err=True)

    using = extract_using_from_columns(old, new, type_)

    if nullable is not None or type_ is not None or new_name is not None or using is not None:
        handler.add_action(
            ma.AlterColumnMigrationAction(
                table_identifier=target_table_identifier,
                column_name=old.name,
                nullable=nullable,
                type_=type_,
                new_column_name=new_name,
                comment=new.comment if new.comment != old.comment else False,
                using=using,
            ),
            foreign_key,
        )

    is_renamed = name_changed(
        source_table_identifier.pg_qualified_name, target_table_identifier.pg_qualified_name, old.name, new.name
    )
    # Order has to be UniqueConstraint -> Index
    # because UniqueConstraint also contain Unique Index
    # we mark them as handled if they are part of UniqueConstraint
    handle_unique_constraint_migration(
        source_table_identifier=source_table_identifier,
        target_table_identifier=target_table_identifier,
        old_column=old,
        new_column=new,
        handler=handler,
        inspector=inspector,
        foreign_key=foreign_key,
        renamed=is_renamed,
        model_context=property_ctx.model_context,
    )
    handle_index_migration(
        source_table_identifier=source_table_identifier,
        target_table_identifier=target_table_identifier,
        old_column=old,
        new_column=new,
        handler=handler,
        inspector=inspector,
        foreign_key=foreign_key,
        renamed=is_renamed,
        model_context=property_ctx.model_context,
    )


@commands.migrate.register(
    Context, PostgreSQL, PostgresqlMigrationContext, PropertyMigrationContext, NotAvailable, sa.Column
)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    property_ctx: PropertyMigrationContext,
    old: NotAvailable,
    new: sa.Column,
    foreign_key: bool = False,
    **kwargs,
):
    inspector = migration_ctx.inspector
    handler = migration_ctx.handler

    source_table = get_source_table(property_ctx, new)
    source_table_identifier = migration_ctx.get_table_identifier(source_table)
    target_table_identifier = migration_ctx.get_table_identifier(property_ctx.prop)
    target_table_name = target_table_identifier.pg_table_name
    handler.add_action(
        ma.AddColumnMigrationAction(
            table_identifier=target_table_identifier,
            column=new,
        ),
        foreign_key,
    )

    new_column_name = new.name
    if new.unique:
        constraint_name = get_pg_constraint_name(target_table_name, [new_column_name])
        unique_constraints = inspector.get_unique_constraints(
            table_name=target_table_name, schema=target_table_identifier.pg_schema_name
        )
        property_ctx.model_context.mark_unique_constraint_handled(
            source_table_identifier.logical_qualified_name, constraint_name
        )
        if not contains_unique_constraint(unique_constraints, new_column_name):
            handler.add_action(
                ma.CreateUniqueConstraintMigrationAction(
                    constraint_name=constraint_name, table_identifier=target_table_identifier, columns=[new_column_name]
                )
            )
        elif not contains_constraint_name(unique_constraints, constraint_name):
            constraint = constraint_with_columns(unique_constraints, [new_column_name])
            if constraint:
                property_ctx.model_context.mark_unique_constraint_handled(
                    source_table_identifier.logical_qualified_name, constraint["name"]
                )
                handler.add_action(
                    ma.RenameConstraintMigrationAction(
                        table_identifier=target_table_identifier,
                        old_constraint_name=constraint["name"],
                        new_constraint_name=constraint_name,
                    )
                )

    index_required = isinstance(new.type, geoalchemy2.types.Geometry)
    if index_required:
        index_name = get_pg_index_name(target_table_name, new_column_name)
        property_ctx.model_context.mark_index_handled(source_table_identifier.logical_qualified_name, index_name)
        handler.add_action(
            ma.CreateIndexMigrationAction(
                table_identifier=target_table_identifier, columns=[new_column_name], index_name=index_name, using="GIST"
            )
        )


@commands.migrate.register(
    Context,
    PostgreSQL,
    PostgresqlMigrationContext,
    (PropertyMigrationContext, ModelMigrationContext),
    list,
    NotAvailable,
)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    node_ctx: PropertyMigrationContext | ModelMigrationContext,
    old: list,
    new: NotAvailable,
    foreign_key: bool = False,
    **kwargs,
):
    for item in old:
        commands.migrate(
            context,
            backend,
            migration_ctx,
            node_ctx,
            item,
            new,
            **adjust_kwargs(kwargs, {"foreign_key": foreign_key}),
        )


@commands.migrate.register(
    Context,
    PostgreSQL,
    PostgresqlMigrationContext,
    (PropertyMigrationContext, ModelMigrationContext),
    sa.Column,
    NotAvailable,
)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    node_ctx: PropertyMigrationContext | ModelMigrationContext,
    old: sa.Column,
    new: NotAvailable,
    foreign_key: bool = False,
    **kwargs,
):
    model_ctx = node_ctx
    node = None
    if isinstance(node_ctx, PropertyMigrationContext):
        model_ctx = node_ctx.model_context
        node = node_ctx.prop

    if node is None:
        node = model_ctx.model

    if old.name.startswith("_"):
        return

    inspector = migration_ctx.inspector
    handler = migration_ctx.handler

    source_table = get_source_table(node_ctx, old)
    source_table_identifier = migration_ctx.get_table_identifier(source_table)
    source_table_name = source_table_identifier.pg_table_name
    source_logical_name = source_table_identifier.logical_qualified_name
    target_table_identifier = migration_ctx.get_table_identifier(node)
    columns = inspector.get_columns(source_table_name, schema=source_table.schema)
    remove_name = get_pg_removed_name(old.name)

    if any(remove_name == column["name"] for column in columns):
        handler.add_action(
            ma.DropColumnMigrationAction(table_identifier=target_table_identifier, column_name=remove_name), foreign_key
        )
    handler.add_action(
        ma.AlterColumnMigrationAction(
            table_identifier=target_table_identifier,
            column_name=old.name,
            new_column_name=remove_name,
            comment=get_removed_name(old.comment or old.name),
        ),
        foreign_key,
    )
    indexes = inspector.get_indexes(table_name=source_table_name, schema=source_table.schema)
    for index in indexes:
        if index["column_names"] == [old.name]:
            model_ctx.mark_index_handled(source_logical_name, index["name"])
            handler.add_action(
                ma.DropIndexMigrationAction(
                    table_identifier=target_table_identifier,
                    index_name=index["name"],
                ),
                foreign_key,
            )
    if old.unique:
        unique_constraints = inspector.get_unique_constraints(table_name=source_table_name, schema=source_table.schema)
        for constraint in unique_constraints:
            if old.name in constraint["column_names"]:
                model_ctx.mark_unique_constraint_handled(source_logical_name, constraint["name"])
                handler.add_action(
                    ma.DropConstraintMigrationAction(
                        table_identifier=target_table_identifier,
                        constraint_name=constraint["name"],
                    ),
                    foreign_key,
                )


@commands.migrate.register(
    Context, PostgreSQL, PostgresqlMigrationContext, PropertyMigrationContext, sa.Table, NotAvailable, list
)
def migrate(
    context: Context,
    backend: PostgreSQL,
    migration_ctx: PostgresqlMigrationContext,
    property_ctx: PropertyMigrationContext,
    old: NotAvailable,
    new: List[sa.Column],
    foreign_key: bool = False,
    **kwargs,
):
    for column in new:
        commands.migrate(context, backend, migration_ctx, property_ctx, old, column, foreign_key=foreign_key, **kwargs)
