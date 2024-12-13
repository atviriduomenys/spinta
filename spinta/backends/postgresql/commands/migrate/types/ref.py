from typing import List

import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_column_name
from spinta.backends.postgresql.helpers.migrate.actions import MigrationHandler
from spinta.backends.postgresql.helpers.migrate.migrate import name_key, MigratePostgresMeta, adjust_kwargs, \
    is_name_complex, extract_literal_name_from_column, generate_type_missmatch_exception_details, \
    extract_sqlalchemy_columns, is_internal, split_columns, get_spinta_primary_keys, remap_and_rename_columns, \
    remove_property_prefix_from_column_name, zip_and_migrate_properties, contains_constraint_name, MigrateModelMeta, \
    constraint_with_name
from spinta.backends.postgresql.helpers.name import get_pg_column_name, get_pg_table_name, get_pg_foreign_key_name
from spinta.cli.helpers.migrate import MigrateRename
from spinta.components import Context
from spinta.datasets.inspect.helpers import zipitems
from spinta.exceptions import MigrateScalarToRefTooManyKeys, MigrateScalarToRefTypeMissmatch
from spinta.types.datatype import Ref, ExternalRef
from spinta.utils.itertools import ensure_list
from spinta.utils.schema import NotAvailable, NA


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, NotAvailable, Ref)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: NotAvailable, new: Ref, model_meta: MigrateModelMeta, **kwargs):
    new_primary_columns = commands.prepare(context, backend, new.prop, propagate=False)
    new_primary_columns = ensure_list(new_primary_columns)
    new_primary_columns = extract_sqlalchemy_columns(new_primary_columns)
    # Since its `Ref` type, it should only generate 1 columns 'column._id'
    primary_column = new_primary_columns[0]

    columns = commands.prepare(context, backend, new.prop)
    if not isinstance(columns, list):
        columns = [columns]
    for column in columns:
        if isinstance(column, sa.Column):
            commands.migrate(context, backend, meta, table, old, column, **adjust_kwargs(kwargs, {
                'foreign_key': True,
                'model_meta': model_meta
            }))

    table_name = get_pg_table_name(get_table_name(new.prop.model))
    _handle_property_foreign_key_constraint(
        table_name=table_name,
        table=table,
        primary_column=primary_column,
        ref=new,
        handler=meta.handler,
        inspector=meta.inspector,
        rename=meta.rename,
        model_meta=model_meta
    )


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, NotAvailable, ExternalRef)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: NotAvailable, new: Ref, **kwargs):
    columns = commands.prepare(context, backend, new.prop)
    if not isinstance(columns, list):
        columns = [columns]
    for column in columns:
        if isinstance(column, sa.Column):
            commands.migrate(context, backend, meta, table, old, column, **adjust_kwargs(kwargs, {
                'foreign_key': True
            }))


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, sa.Column, Ref)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: sa.Column, new: Ref, **kwargs):
    commands.migrate(context, backend, meta, table, [old], new, **kwargs)


def _migrate_scalar_to_ref_4(
    context: Context,
    backend: PostgreSQL,
    table: sa.Table,
    columns: List[sa.Column],
    ref: Ref,
    ref_column: sa.Column,
    meta: MigratePostgresMeta,
    handler: MigrationHandler,
    rename: MigrateRename,
    **kwargs
) -> bool:
    """Checks and converts scalar to internal ref

    Args:
        table: old table
        columns: list of old columns
        ref: new Ref property
        ref_column: new Ref converted to column

    Returns:
        bool: Applied scalar to ref conversion or not

    In order to do scalar to ref conversion, there are strict requirements
    otherwise it would be nearly impossible to guess primary key and foreign key mapping
        1. refprops size has to be 1 (since you try to map single column, cannot map to more than that)
        2. column type has to match ref tables primary key type
    This triggers only if column name (after rename) matches ref property name_key
    """

    # Check if there is only 1 primary key to match
    if not len(columns) == 1:
        return False

    table_name = get_pg_table_name(rename.get_table_name(table.name))

    column = columns[0]
    new_name = rename.get_column_name(table.name, column.name)

    # Check if after rename column becomes ref itself, or only part of it (can check if name contains special characters)
    if is_name_complex(new_name):
        return False

    # Check if refprops is size of 1
    if len(ref.refprops) > 1:
        raise MigrateScalarToRefTooManyKeys(ref, primary_keys=[pkey.name for pkey in ref.refprops])

    target = ref.refprops[0]
    target_column = commands.prepare(context, backend, target)
    key = target.name
    old_type = extract_literal_name_from_column(column)
    new_type = extract_literal_name_from_column(target_column)

    # Check if types match
    if old_type != new_type:
        raise MigrateScalarToRefTypeMissmatch(
            ref,
            details=generate_type_missmatch_exception_details(
                [
                    (
                        (column.name, old_type),
                        (key, new_type)
                    )
                ]
            ))
    # Create new empty ref column
    commands.migrate(context, backend, meta, table, NA, ref_column, **kwargs)

    # Apply conversion from scalar to ref column
    handler.add_action(ma.UpgradeTransferDataMigrationAction(
        table_name=table_name,
        referenced_table_name=get_pg_table_name(get_table_name(ref.model)),
        ref_column=ref_column,
        columns={
            key: column
        }
    ), True)

    # Drop old column after migration
    commands.migrate(context, backend, meta, table, column, NA, **kwargs)
    return True


def _migrate_scalar_to_ref_3(
    context: Context,
    backend: PostgreSQL,
    table: sa.Table,
    columns: List[sa.Column],
    ref: ExternalRef,
    ref_columns: List[sa.Column],
    meta: MigratePostgresMeta,
    handler: MigrationHandler,
    rename: MigrateRename,
    **kwargs
) -> bool:
    """Checks and converts scalar to external ref

    Args:
        table: old table
        columns: list of old columns
        ref: new ExternalRef property
        ref_columns: new ExternalRef converted to column

    Returns:
        bool: Applied scalar to ref conversion or not

    In order to do scalar to ref conversion, there are strict requirements
    otherwise it would be nearly impossible to guess primary key and foreign key mapping
        1. refprops size has to be 1 (since you try to map single column, cannot map to more than that)
        2. column type has to match ref tables primary key type
    This triggers only if column name (after rename) matches ref property name_key
    """

    # Check if there is only 1 primary key to match
    if not len(columns) == 1:
        return False

    table_name = get_pg_table_name(rename.get_table_name(table.name))

    column = columns[0]
    new_name = rename.get_column_name(table.name, column.name)

    # Check if after rename column becomes ref itself, or only part of it (can check if name contains special characters)
    if is_name_complex(new_name):
        return False

    # Check if refprops is size of 1
    if len(ref.refprops) > 1 or len(ref_columns) > 1:
        raise MigrateScalarToRefTooManyKeys(ref, primary_keys=[pkey.name for pkey in ref.refprops])

    ref_column = ref_columns[0]
    old_type = extract_literal_name_from_column(column)
    new_type = extract_literal_name_from_column(ref_column)

    # Check if types match
    if old_type != new_type:
        raise MigrateScalarToRefTypeMissmatch(
            ref,
            details=generate_type_missmatch_exception_details(
                [
                    (
                        (column.name, old_type),
                        (ref_column.name, new_type)
                    )
                ]
            ))
    # Create new empty ref column
    commands.migrate(context, backend, meta, table, NA, ref_column, **kwargs)

    # Apply conversion from scalar to ref column
    target = remove_property_prefix_from_column_name(
        ref_column.name,
        ref.prop
    )
    handler.add_action(
        ma.DowngradeTransferDataMigrationAction(
            table_name=table_name,
            referenced_table_name=get_pg_table_name(get_table_name(ref.model)),
            source_column=column,
            columns={
                ref_column.name: sa.Column(target, type_=ref_column.type)
            },
            target=target
        ), True
    )

    # Drop old column after migration
    commands.migrate(context, backend, meta, table, column, NA, **kwargs)
    return True


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, list, Ref)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: List[sa.Column], new: Ref, model_meta: MigrateModelMeta, **kwargs):
    rename = meta.rename
    inspector = meta.inspector
    handler = meta.handler
    adjusted_kwargs = adjust_kwargs(kwargs, {
        'foreign_key': True,
        'model_meta': model_meta
    })

    new_primary_columns = commands.prepare(context, backend, new.prop, propagate=False)
    new_primary_columns = ensure_list(new_primary_columns)
    new_primary_columns = extract_sqlalchemy_columns(new_primary_columns)
    new_primary_column_names = [column.name for column in new_primary_columns]

    # Since its `Ref` type, it should only generate 1 columns 'column._id'
    primary_column = new_primary_columns[0]

    new_all_columns = commands.prepare(context, backend, new.prop)
    new_all_columns = ensure_list(new_all_columns)
    new_all_columns = extract_sqlalchemy_columns(new_all_columns)

    new_children_columns = [column for column in new_all_columns if column.name not in new_primary_column_names]
    new_children_column_names = [column.name for column in new_children_columns]

    table_name = get_pg_table_name(rename.get_table_name(table.name))
    old_ref_table = get_pg_table_name(rename.get_old_table_name(get_table_name(new.model)))
    old_prop_name = get_pg_column_name(rename.get_old_column_name(table.name, get_column_name(new.prop)))

    new_name = get_pg_column_name(new.prop.place)
    ref_model_primary_keys = get_spinta_primary_keys(
        table_name=old_ref_table,
        model=new.model,
        inspector=inspector
    )
    old_columns_internal = is_internal(
        columns=old,
        base_name=old_prop_name,
        table_name=table.name,
        ref_table_name=old_ref_table,
        inspector=inspector
    )
    old_primary_columns, old_children_columns = split_columns(
        columns=old,
        base_name=old_prop_name,
        target_base_name=new_name,
        ref_table_primary_key_names=ref_model_primary_keys,
        target_primary_column_names=new_primary_column_names,
        target_children_column_names=new_children_column_names,
        internal=old_columns_internal
    )

    if old_columns_internal:
        # Handle internal ref migration
        commands.migrate(
            context,
            backend,
            meta,
            table,
            old_primary_columns[0],
            primary_column,
            **adjusted_kwargs
        )
    else:
        # Try to do scalar to ref migration
        migrated = _migrate_scalar_to_ref_4(
            context=context,
            backend=backend,
            table=table,
            columns=old_primary_columns,
            ref=new,
            ref_column=primary_column,
            meta=meta,
            handler=handler,
            rename=rename,
            **adjusted_kwargs
        )

        # Scalar did not pass
        if not migrated:
            column_mapping = dict()
            # Ref level 3 (no pkeys) -> ref level 4
            if len(old_primary_columns) == 1 and old_primary_columns[0].name.endswith('._id'):
                commands.migrate(
                    context,
                    backend,
                    meta,
                    table,
                    old_primary_columns[0],
                    primary_column,
                    **adjusted_kwargs
                )
            else:
                for column in old_primary_columns:
                    new_name = rename.get_column_name(table.name, column.name)
                    key = new_name.split('.')[-1]
                    column_mapping[key] = column

                # Create empty ref column
                commands.migrate(context, backend, meta, table, NA, primary_column, **adjusted_kwargs)

                # Migrate from level 3 to level 4 ref
                handler.add_action(ma.UpgradeTransferDataMigrationAction(
                    table_name=table_name,
                    referenced_table_name=get_pg_table_name(get_table_name(new.model)),
                    ref_column=primary_column,
                    columns=column_mapping
                ), True)

                # Drop old columns
                for column in column_mapping.values():
                    commands.migrate(context, backend, meta, table, column, NA, **adjusted_kwargs)

    _handle_property_foreign_key_constraint(
        table_name=table_name,
        table=table,
        primary_column=primary_column,
        ref=new,
        rename=rename,
        inspector=inspector,
        model_meta=model_meta,
        handler=handler,
    )

    zip_and_migrate_properties(
        context=context,
        backend=backend,
        old_table=table,
        new_model=new.prop.model,
        old_columns=old_children_columns,
        new_properties=list(new.properties.values()),
        meta=meta,
        rename=rename,
        root_name=new.prop.place,
        **adjusted_kwargs
    )


def _handle_property_foreign_key_constraint(
    table_name: str,
    table: sa.Table,
    primary_column: sa.Column,
    ref: Ref,
    handler: MigrationHandler,
    inspector: Inspector,
    rename: MigrateRename,
    model_meta: MigrateModelMeta
):
    foreign_keys = inspector.get_foreign_keys(table.name)
    foreign_key_name = get_pg_foreign_key_name(
        table_name=table_name,
        column_name=primary_column.name
    )
    model_meta.handle_foreign_constraint(foreign_key_name)
    referent_table = get_pg_table_name(get_table_name(ref.model))

    old_prop_name = get_pg_column_name(f'{rename.get_old_column_name(table.name, get_column_name(ref.prop))}._id')
    old_referent_table = get_pg_table_name(rename.get_old_table_name(get_table_name(ref.model)))
    if not contains_constraint_name(foreign_keys, foreign_key_name):
        for foreign_key in foreign_keys:
            if foreign_key["constrained_columns"] == [old_prop_name] and foreign_key["referred_table"] == old_referent_table:
                model_meta.handle_foreign_constraint(foreign_key["name"])
                handler.add_action(ma.RenameConstraintMigrationAction(
                    table_name=table_name,
                    old_constraint_name=foreign_key["name"],
                    new_constraint_name=foreign_key_name
                ), foreign_key=True)
                return

        handler.add_action(
            ma.CreateForeignKeyMigrationAction(
                source_table=table_name,
                referent_table=referent_table,
                constraint_name=foreign_key_name,
                local_cols=[primary_column.name],
                remote_cols=["_id"]
            ), foreign_key=True
        )
        return

    constraint = constraint_with_name(foreign_keys, foreign_key_name)
    if constraint["constrained_columns"] != [old_prop_name] or constraint["referred_table"] != old_referent_table:
        model_meta.handle_foreign_constraint(constraint["name"])
        handler.add_action(ma.DropConstraintMigrationAction(
            table_name=table_name,
            constraint_name=constraint["name"]
        ), foreign_key=True)
        handler.add_action(
            ma.CreateForeignKeyMigrationAction(
                source_table=table_name,
                referent_table=referent_table,
                constraint_name=foreign_key_name,
                local_cols=[primary_column.name],
                remote_cols=["_id"]
            ), foreign_key=True
        )


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, list, ExternalRef)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: List[sa.Column], new: ExternalRef, **kwargs):
    rename = meta.rename
    inspector = meta.inspector
    handler = meta.handler

    adjusted_kwargs = adjust_kwargs(kwargs, {
        'foreign_key': True
    })

    table_name = get_pg_table_name(rename.get_table_name(table.name))
    old_ref_table = get_pg_table_name(rename.get_old_table_name(get_table_name(new.model)))
    old_prop_name = rename.get_old_column_name(table.name, get_column_name(new.prop))

    new_primary_columns = commands.prepare(context, backend, new.prop, propagate=False)
    new_primary_columns = ensure_list(new_primary_columns)
    new_primary_columns = extract_sqlalchemy_columns(new_primary_columns)
    new_primary_column_name_mapping = {column.name: column for column in new_primary_columns}

    new_all_columns = commands.prepare(context, backend, new.prop)
    new_all_columns = ensure_list(new_all_columns)
    new_all_columns = extract_sqlalchemy_columns(new_all_columns)

    new_children_columns = [column for column in new_all_columns if
                            column.name not in new_primary_column_name_mapping]
    new_children_column_names = [column.name for column in new_children_columns]

    new_name = get_pg_column_name(new.prop.place)
    ref_model_primary_keys = get_spinta_primary_keys(
        table_name=old_ref_table,
        model=new.model,
        inspector=inspector
    )
    old_columns_internal = is_internal(
        columns=old,
        base_name=old_prop_name,
        table_name=table.name,
        ref_table_name=old_ref_table,
        inspector=inspector
    )
    old_primary_columns, old_children_columns = split_columns(
        columns=old,
        base_name=old_prop_name,
        target_base_name=new_name,
        ref_table_primary_key_names=ref_model_primary_keys,
        target_primary_column_names=list(new_primary_column_name_mapping.keys()),
        target_children_column_names=new_children_column_names,
        internal=old_columns_internal
    )

    # Check to see if migration has already been achieved
    migrated = False
    if len(old_primary_columns) == 1:
        old_primary_column = old_primary_columns[0]

        if old_columns_internal:
            # Ref 4 -> ref 3 (no pkeys)
            if len(new_primary_columns) == 1 and new_primary_columns[0].name.endswith('._id'):
                commands.migrate(
                    context,
                    backend,
                    meta,
                    table,
                    old_primary_column,
                    new_primary_columns[0],
                    **adjusted_kwargs
                )
            else:
                # Handle Internal ref mapping
                # Ensure columns exist
                column_mapping = {}
                for column in new_primary_columns:
                    column_mapping[column.name] = sa.Column(
                        remove_property_prefix_from_column_name(
                            column.name,
                            new.prop
                        ),
                        type_=column.type
                    )
                    commands.migrate(context, backend, meta, table, NA, column, **adjusted_kwargs)

                # Downgrade ref column
                handler.add_action(
                    ma.DowngradeTransferDataMigrationAction(
                        table_name=table_name,
                        referenced_table_name=get_pg_table_name(get_table_name(new.model)),
                        source_column=old_primary_column,
                        columns=column_mapping,
                        target='_id'
                    ), True
                )

                # Drop old column
                commands.migrate(context, backend, meta, table, old_primary_column, NA, **adjusted_kwargs)
            migrated = True
        else:
            migrated = _migrate_scalar_to_ref_3(
                context=context,
                backend=backend,
                table=table,
                columns=old_primary_columns,
                ref=new,
                ref_columns=new_primary_columns,
                meta=meta,
                handler=handler,
                rename=rename,
                **adjusted_kwargs
            )

    # If no ref migrations were done, try to zip the primary columns and migrate them
    if not migrated:
        renamed_old_primary_columns = remap_and_rename_columns(
            base_name=old_prop_name,
            columns=old_primary_columns,
            table_name=table.name,
            ref_table_name=old_ref_table,
            rename=rename
        )

        zipped_items = zipitems(
            renamed_old_primary_columns.keys(),
            new_primary_column_name_mapping.keys(),
            name_key
        )
        for zipped_item in zipped_items:
            for old_column, new_column in zipped_item:
                if old_column is not NA:
                    old_column = renamed_old_primary_columns[old_column]
                if new_column is not NA:
                    new_column = new_primary_column_name_mapping[new_column]

                if old_column is not None and new_column is None:
                    commands.migrate(context, backend, meta, table, old_column, NA, **adjusted_kwargs)
                else:
                    commands.migrate(context, backend, meta, table, old_column, new_column, **adjusted_kwargs)

    zip_and_migrate_properties(
        context=context,
        backend=backend,
        old_table=table,
        new_model=new.prop.model,
        old_columns=old_children_columns,
        new_properties=list(new.properties.values()),
        meta=meta,
        rename=rename,
        root_name=new.prop.place,
        **kwargs
    )
