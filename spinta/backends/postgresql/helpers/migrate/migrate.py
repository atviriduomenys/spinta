import dataclasses
from typing import Any, List, Union, Dict, Tuple

import sqlalchemy as sa
import geoalchemy2.types
from sqlalchemy.dialects.postgresql import JSONB, BIGINT, ARRAY, JSON
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.dialects import postgresql

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.components import BackendFeatures
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_name, get_column_name
from spinta.backends.postgresql.helpers.migrate.actions import MigrationHandler
from spinta.cli.migrate import MigrateRename
from spinta.components import Context, Model, Property
from spinta.datasets.enums import Level
from spinta.exceptions import MigrateScalarToRefTooManyKeys
from spinta.types.datatype import Ref, File, Array, Object
from spinta.types.text.components import Text
from spinta.utils.nestedstruct import get_root_attr
from spinta.utils.schema import NA


def check_if_renamed(old_table: str, new_table: str, old_property: str, new_property: str):
    return old_table != new_table or old_property != new_property


def drop_all_indexes_and_constraints(inspector: Inspector, table: str, new_table: str, handler: MigrationHandler):
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


def create_changelog_table(context: Context, new: Model, handler: MigrationHandler, rename: MigrateRename):
    table_name = get_pg_name(get_table_name(new, TableType.CHANGELOG))
    pkey_type = commands.get_primary_key_type(context, new.backend)
    handler.add_action(ma.CreateTableMigrationAction(
        table_name=table_name,
        columns=[
            sa.Column('_id', BIGINT, primary_key=True, autoincrement=True),
            sa.Column('_revision', sa.String),
            sa.Column('_txn', pkey_type, index=True),
            sa.Column('_rid', pkey_type),
            sa.Column('datetime', sa.DateTime),
            sa.Column('action', sa.String(8)),
            sa.Column('data', JSONB)
        ]
    ))


def handle_new_file_type(context: Context, backend: PostgreSQL, inspector: Inspector, prop: Property, pkey_type: Any,
                         handler: MigrationHandler) -> list:
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


def handle_new_array_type(context: Context, backend: PostgreSQL, inspector: Inspector, prop: Property, pkey_type: Any,
                          handler: MigrationHandler):
    columns = []
    if isinstance(prop.dtype, Array) and prop.dtype.items:
        if prop.list is None:
            columns.append(sa.Column(prop.place, JSONB))

        if isinstance(prop.dtype.items.dtype, File):
            new_columns = handle_new_file_type(context, backend, inspector, prop.dtype.items, pkey_type, handler)
        elif isinstance(prop.dtype.items.dtype, Array):
            new_columns = handle_new_array_type(context, backend, inspector, prop.dtype.items, pkey_type, handler)
        elif isinstance(prop.dtype.items.dtype, Object):
            new_columns = handle_new_object_type(context, backend, inspector, prop.dtype.items, pkey_type, handler)
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


def handle_new_object_type(context: Context, backend: PostgreSQL, inspector: Inspector, prop: Property, pkey_type: Any,
                           handler: MigrationHandler):
    columns = []
    if isinstance(prop.dtype, Object) and prop.dtype.properties:
        for new_prop in prop.dtype.properties.values():
            if prop.name.startswith('_') and prop.name not in ('_revision',):
                continue
            if isinstance(new_prop.dtype, File):
                columns = handle_new_file_type(context, backend, inspector, new_prop, pkey_type, handler)
            elif isinstance(new_prop.dtype, Array):
                columns = handle_new_array_type(context, backend, inspector, new_prop, pkey_type, handler)
            elif isinstance(new_prop.dtype, Object):
                columns = handle_new_object_type(context, backend, inspector, new_prop, pkey_type, handler)
            else:
                columns = commands.prepare(context, backend, new_prop)

            if not isinstance(columns, list):
                columns = [columns]
            for column in columns:
                if not isinstance(column, sa.Column):
                    columns.remove(column)
    return columns


def rename_index_name(index: str, old_table: str, new_table: str, old_property: str, new_property: str):
    new = f'ix_{new_table}_{new_property}'
    return new


def get_remove_name(name: str) -> str:
    new_name = name.split("/")
    if not new_name[-1].startswith("__"):
        new_name[-1] = f'__{new_name[-1]}'
    new_name = '/'.join(new_name)
    new_name = get_pg_name(new_name)
    return new_name


def get_prop_names(prop: Property):
    name = prop.name
    if isinstance(prop.dtype, Ref):
        if not prop.level or prop.level > Level.open:
            name = f'{name}._id'
        else:
            for refprop in prop.dtype.refprops:
                yield f'{name}.{refprop.name}'
    yield name


def json_has_key(backend: PostgreSQL, column: sa.Column, table: sa.Table, key: str):
    with backend.engine.begin() as connection:
        query = sa.select(table.select().where(column.has_key(key)).exists())
        return connection.execute(query).scalar()


def jsonb_keys(backend: PostgreSQL, column: sa.Column, table: sa.Table):
    with backend.engine.begin() as connection:
        keys = sa.func.jsonb_object_keys(
            column
        )
        query = sa.select(
            [keys]
        ).select_from(table).group_by(keys)
        return [result[0] for result in connection.execute(query)]


def name_key(name: str):
    return name


def model_name_key(model: str) -> str:
    return get_pg_name(model)


def is_name_complex(name: str):
    return '.' in name or '@' in name


def is_prop_complex(prop: Property):
    return isinstance(prop.dtype, (Text, File, Ref))


def is_name_or_property_complex(name: str, prop: Property):
    is_complex = is_name_complex(name)
    if not is_complex:
        return is_prop_complex(prop)
    return is_complex


def is_name_or_column_complex(name: str, col: sa.Column):
    is_complex = is_name_complex(name)
    if not is_complex:
        return is_column_complex(col)
    return is_complex


def is_column_complex(col: sa.Column):
    return isinstance(col.type, (JSONB, JSON))


def property_and_column_name_key(item: Union[sa.Column, Property], rename, table: sa.Table, model: Model) -> str:
    # Mapping concept is to prioritize complex types over simple
    # new types take priority over old
    # Column is always old, Property is always new

    if isinstance(item, sa.Column):
        name = item.name
        is_complex = is_name_or_column_complex(name, item)
        new_name = rename.get_column_name(table.name, name, True)
        full_name = rename.get_column_name(table.name, name)

        root_changed = has_been_renamed(name, new_name)
        full_changed = has_been_renamed(name, full_name)

        # Check for edge case when you have old columns: column_one, column_two
        # new manifest only hase column_one, but
        # rename provides "column_two": "column_one"
        # meaning, you need to remove old "column_one" and rename old "column_two" to "column_one"
        if not root_changed:
            old_name = rename.get_old_column_name(table.name, name)
            if has_been_renamed(name, old_name):
                return get_root_attr(old_name)

        if full_changed:
            new_prop = model.flatprops[full_name]
            if is_name_or_property_complex(full_name, new_prop) or is_complex:
                return get_root_attr(full_name)

        if root_changed:
            new_prop = model.flatprops[new_name]
            if is_name_or_property_complex(new_name, new_prop):
                return get_root_attr(new_name)
            elif not full_changed:
                return name

        return get_root_attr(name)
    elif isinstance(item, Property):
        name = get_column_name(item)
        old_name = rename.get_old_column_name(table.name, name, True)
        old_full_name = rename.get_old_column_name(table.name, name)

        if is_name_or_property_complex(name, item):
            return get_root_attr(name)

        if has_been_renamed(name, old_full_name):
            if old_full_name in table.columns:
                col = table.columns[old_full_name]
                if is_name_or_column_complex(old_full_name, col):
                    return get_root_attr(name)

        return get_root_attr(old_name)


@dataclasses.dataclass
class MigratePostgresMeta:
    inspector: Inspector
    rename: MigrateRename
    handler: MigrationHandler


@dataclasses.dataclass
class JSONColumnMigrateMeta:
    column: sa.Column
    keys: List[str] = dataclasses.field(default_factory=list)
    new_keys: Dict[str, str] = dataclasses.field(default_factory=dict)
    full_remove: bool = dataclasses.field(default=True)
    cast_to: Tuple[sa.Column, str] = dataclasses.field(default=None)
    new_name: str = dataclasses.field(default=None)

    def initialize(self, backend, table):
        self.keys = jsonb_keys(backend, self.column, table)

    def add_new_key(self, old_key: str, new_key: str):
        if old_key in self.keys and old_key not in self.new_keys:
            self.new_keys[old_key] = new_key


@dataclasses.dataclass
class MigrateModelMeta:
    json_columns: Dict[str, JSONColumnMigrateMeta] = dataclasses.field(default_factory=dict)

    def add_json_column(self, backend: PostgreSQL, table: sa.Table, column: sa.Column):
        meta = JSONColumnMigrateMeta(
            column=column,
        )
        meta.initialize(backend, table)
        self.json_columns[column.name] = meta


def has_been_renamed(old: str, new: str):
    return old != new


def is_internal_ref(dtype: Ref):
    prop = dtype.prop
    return prop.level is None or prop.level > Level.open


def handle_internal_ref_to_scalar_conversion(
    context: Context,
    backend: PostgreSQL,
    meta: MigratePostgresMeta,
    table: sa.Table,
    old_columns: List[sa.Column],
    new_property: Property,
    **kwargs
) -> bool:
    if isinstance(old_columns, sa.Column):
        old_columns = [old_columns]

    if not old_columns or not new_property:
        return False

    # Check if columns are from ref 4 (can only have 1 column)
    if not (len(old_columns) == 1 and isinstance(old_columns[0], sa.Column)):
        return False

    inspector = meta.inspector
    handler = meta.handler
    rename = meta.rename

    ref_col = old_columns[0]
    manifest = new_property.model.manifest

    # Skip ref 4 -> ref 3
    if isinstance(new_property.dtype, Ref):
        return False

    if ref_col.name.endswith('._id'):
        constraints = inspector.get_foreign_keys(table.name)
        ref_model = None
        for constraint in constraints:
            if constraint['constrained_columns'] == [ref_col.name]:
                table_name = constraint['referred_table']
                if commands.has_model(context, manifest, table_name):
                    ref_model = commands.get_model(context, manifest, table_name)
                else:
                    # In case table name has been truncated, need to loop through all models and convert their names to pg
                    all_models = commands.get_models(context, manifest)
                    for model in all_models.values():
                        if get_pg_name(model.name) == table_name:
                            ref_model = model
                            break
                break

        if isinstance(ref_model, Model):
            if ref_model.external and not ref_model.external.unknown_primary_key:
                pkeys = ref_model.external.pkeys
                mapped_data: dict = dict()
                if len(pkeys) > 1:
                    raise MigrateScalarToRefTooManyKeys(new_property.dtype, primary_keys=[key.name for key in pkeys])

                target_key = pkeys[0]
                prop_col = commands.prepare(context, backend, target_key)
                mapped_data[get_pg_name(new_property.name)] = prop_col
                updated_kwargs = adjust_kwargs(kwargs, 'foreign_key', True)

                commands.migrate(context, backend, meta, table, NA, new_property, **updated_kwargs)
                table_name = rename.get_table_name(table.name)
                foreign_table_name = get_table_name(ref_model)
                handler.add_action(
                    ma.DowngradeTransferDataMigrationAction(
                        table_name,
                        foreign_table_name,
                        ref_col,
                        mapped_data
                    ),
                    foreign_key=True
                )
                commands.migrate(context, backend, meta, table, ref_col, NA, **updated_kwargs)
                return True

    return False


def extract_target_column(rename: MigrateRename, columns: list, table: sa.Table, prop: Property):
    full_name = rename.get_old_column_name(table.name, prop.name)
    if isinstance(columns, list):
        for col in columns:
            if isinstance(col, sa.Column) and col.name == full_name:
                return [col]
    return columns


def adjust_kwargs(kwargs: dict, key: str, value: Any) -> dict:
    copied = kwargs.copy()
    copied[key] = value
    return copied


def extract_literal_name_from_column(
    column: sa.Column,
) -> str:
    type_ = column.type.compile(dialect=postgresql.dialect())

    # Convert sa.Float, to postgresql DOUBLE PRECISION type
    if isinstance(column.type, sa.Float):
        type_ = 'DOUBLE PRECISION'

    return type_


# Match [
#   (
#       (old_column_name, old_type),
#       (new_column_name, new_type)
#   )
# ]
def generate_type_missmatch_exception_details(
    columns: list
):
    result = ''
    for pair in columns:
        old_data = pair[0]
        new_data = pair[1]
        result += f'\t\'{old_data[0]}\' [{old_data[1]}] -> \'{new_data[0]}\' [{new_data[1]}]\t'
        if old_data[1] == new_data[1]:
            result += f'\'{old_data[1]}\' == \'{new_data[1]}\'\n'
        else:
            result += f'\'{old_data[1]}\' != \'{new_data[1]}\'\t<= Incorrect\n'
    return result


def contains_unique_constraint(inspector: Inspector, table: Union[sa.Table, str], column_name: str):
    table_name = table.name if isinstance(table, sa.Table) else table
    return any(constraint["column_names"] == [column_name] for constraint in
               inspector.get_unique_constraints(table_name=table_name)) or any(
        index["column_names"] == [column_name] and index["unique"] for index in
        inspector.get_indexes(table_name=table_name))


def handle_unique_constraint_migration(
    table: sa.Table,
    table_name: str,
    old: sa.Column,
    new: sa.Column,
    column_name: str,
    handler: MigrationHandler,
    inspector: Inspector,
    foreign_key: bool,
    removed: list,
    renamed: bool
):
    unique_name = get_pg_name(f'{table_name}_{column_name}_key')

    unique_constraints = inspector.get_unique_constraints(table_name=table.name)
    constraint_column = old.name if renamed else column_name

    if new.unique and not renamed and not contains_unique_constraint(inspector, table, constraint_column):
        handler.add_action(ma.CreateUniqueConstraintMigrationAction(
            constraint_name=unique_name,
            table_name=table_name,
            columns=[constraint_column]
        ), foreign_key)
    else:
        for constraint in unique_constraints:
            dropped = False
            if constraint["column_names"] == [constraint_column]:
                removed.append(constraint["name"])
                if renamed or old.unique:
                    dropped = True
                    handler.add_action(ma.DropConstraintMigrationAction(
                        constraint_name=constraint["name"],
                        table_name=table_name
                    ), foreign_key)
                unique_name = get_pg_name(
                    rename_index_name(constraint["name"], table.name, table_name, old.name, new.name))
                if new.unique and (dropped or not contains_unique_constraint(inspector, table, constraint_column)):
                    handler.add_action(ma.CreateUniqueConstraintMigrationAction(
                        constraint_name=unique_name,
                        table_name=table_name,
                        columns=[constraint_column]
                    ), foreign_key)
        if not new.unique:
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
                        columns=[constraint_column]
                    ), foreign_key)


def contains_index(inspector: Inspector, table: Union[sa.Table, str], column_name: str):
    table_name = table.name if isinstance(table, sa.Table) else table
    return any(index["column_names"] == [column_name] for index in inspector.get_indexes(table_name=table_name))


def handle_index_migration(
    table: sa.Table,
    table_name: str,
    old: sa.Column,
    new: sa.Column,
    column_name: str,
    handler: MigrationHandler,
    inspector: Inspector,
    foreign_key: bool,
    removed: list,
    renamed: bool
):
    index_name = get_pg_name(f'ix_{table_name}_{column_name}')

    constraint_column = old.name if renamed else column_name
    indexes = inspector.get_indexes(table_name=table.name)
    if (new.index or isinstance(new.type, geoalchemy2.types.Geometry)) and not renamed and not contains_index(inspector, table, constraint_column):
        handler.add_action(ma.CreateIndexMigrationAction(
            index_name=index_name,
            table_name=table_name,
            columns=[constraint_column],
            using="gist" if isinstance(new.type, geoalchemy2.types.Geometry) else None
        ), foreign_key)
    else:
        for index in indexes:
            dropped = False
            if index["column_names"] == [constraint_column] and index["name"] not in removed:
                index_name = get_pg_name(rename_index_name(index["name"], table.name, table_name, old.name, new.name))
                removed.append(index["name"])
                if renamed or (not new.index and not isinstance(new.type, geoalchemy2.types.Geometry)):
                    dropped = True
                    handler.add_action(
                        ma.DropIndexMigrationAction(
                            index_name=index["name"],
                            table_name=table_name
                        ), foreign_key)
                if (new.index or isinstance(new.type, geoalchemy2.types.Geometry)) and (dropped or not contains_index(inspector, table, constraint_column)):
                    handler.add_action(ma.CreateIndexMigrationAction(
                        index_name=index_name,
                        table_name=table_name,
                        columns=[column_name],
                        using="gist" if isinstance(new.type, geoalchemy2.types.Geometry) else None
                    ), foreign_key)
