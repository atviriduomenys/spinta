import dataclasses
from collections import defaultdict
from typing import Any, List, Union, Dict, Tuple, Callable

import sqlalchemy as sa
import geoalchemy2.types
from spinta.datasets.inspect.helpers import zipitems
from spinta.utils.itertools import ensure_list
from sqlalchemy.dialects.postgresql import JSONB, BIGINT, ARRAY, JSON
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.dialects import postgresql

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.constants import TableType, BackendFeatures
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_name, get_column_name
from spinta.backends.postgresql.helpers.migrate.actions import MigrationHandler
from spinta.backends.postgresql.helpers.name import name_changed, get_pg_constraint_name, get_pg_index_name, \
    nested_column_rename, get_pg_table_name, get_pg_column_name
from spinta.cli.helpers.migrate import MigrateRename
from spinta.components import Context, Model, Property
from spinta.exceptions import MigrateScalarToRefTooManyKeys
from spinta.types.datatype import Ref, File, Array, Object
from spinta.types.text.components import Text
from spinta.utils.nestedstruct import get_root_attr
from spinta.utils.schema import NA


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


def get_prop_names(prop: Property):
    name = prop.name
    if isinstance(prop.dtype, Ref):
        if commands.identifiable(prop):
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


def empty_table(backend: PostgreSQL, table: sa.Table) -> bool:
    with backend.engine.begin() as connection:
        # Fetching one row with limit is more efficient
        query = sa.select(1).select_from(table).limit(1)
        result = connection.execute(query)
        return result.fetchone() is None


def name_key(name: str):
    return name


def model_name_key(model: str) -> str:
    return get_pg_table_name(model)


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


def property_and_column_name_key(
    item: Union[sa.Column, Property],
    rename,
    table: sa.Table,
    model: Model,
    root_name: str = ""
) -> str:
    # Mapping concept is to prioritize complex types over simple
    # new types take priority over old
    # Column is always old, Property is always new

    if isinstance(item, sa.Column):
        # Mapping order
        # Replace existing edge case -> Indirect renaming / removal edge case -> New name -> Old name

        name = item.name
        new_name = rename.get_column_name(table.name, name, True, root_value=root_name)
        full_name = rename.get_column_name(table.name, name)

        column_renamed = name_changed(name, new_name)
        column_directly_renamed = name_changed(name, full_name)

        # Check for edge case when you have old columns: column_one, column_two
        # new manifest only hase column_one, but
        # rename provides "column_two": "column_one"
        # meaning, you need to remove old "column_one" and rename old "column_two" to "column_one"
        if not column_renamed:
            old_name = rename.get_old_column_name(table.name, name)
            if name_changed(name, old_name):
                return name

        if column_renamed and not column_directly_renamed:
            new_prop = model.flatprops[new_name]
            if not is_name_or_property_complex(new_name, new_prop):
                return name

        return get_root_attr(new_name, initial_root=root_name)
    elif isinstance(item, Property):
        # Mapping order
        # New Property (complex) -> Old column (complex) -> New Property

        name = get_column_name(item)
        old_name = rename.get_old_column_name(table.name, name, True, root_value=root_name)
        old_full_name = rename.get_old_column_name(table.name, name)

        property_directly_renamed = name_changed(name, old_full_name)

        if is_name_or_property_complex(name, item):
            return get_root_attr(name, initial_root=root_name)

        if property_directly_renamed:
            if old_full_name in table.columns:
                col = table.columns[old_full_name]
                if is_column_complex(col):
                    return get_root_attr(old_full_name, initial_root=root_name)
            elif old_name in table.columns:
                col = table.columns[old_name]
                if is_column_complex(col):
                    return get_root_attr(old_name, initial_root=root_name)

        return get_root_attr(name, initial_root=root_name)


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
    empty: bool = False

    def initialize(self, backend, table):
        self.keys = jsonb_keys(backend, self.column, table)
        self.empty = empty_table(backend, table)

    def add_new_key(self, old_key: str, new_key: str):
        if old_key in self.keys and old_key not in self.new_keys:
            self.new_keys[old_key] = new_key


@dataclasses.dataclass
class MigrateModelMeta:
    json_columns: Dict[str, JSONColumnMigrateMeta] = dataclasses.field(default_factory=dict)
    unique_constraint_states: Dict[str, bool] = dataclasses.field(default_factory=lambda: defaultdict(lambda: False))
    foreign_constraint_states: Dict[str, bool] = dataclasses.field(default_factory=lambda: defaultdict(lambda: False))
    index_states: Dict[str, bool] = dataclasses.field(default_factory=lambda: defaultdict(lambda: False))

    def initialize(
        self,
        backend: PostgreSQL,
        table: sa.Table,
        columns: List[sa.Column],
        inspector: Inspector
    ):
        for column in columns:
            # Add JSONB to meta (JSONB has different handle system)
            if isinstance(column.type, JSONB):
                self.__add_json_column(
                    backend,
                    table,
                    column
                )

        constraints = inspector.get_unique_constraints(table.name)
        for constraint in constraints:
            if not _reserved_constraint(constraint):
                self.unique_constraint_states[constraint['name']] = False

        constraints = inspector.get_foreign_keys(table.name)
        for constraint in constraints:
            self.foreign_constraint_states[constraint['name']] = False

        indexes = inspector.get_indexes(table.name)
        for index in indexes:
            if not _reserved_constraint(index):
                self.index_states[index['name']] = False

    def handle_unique_constraint(self, constraint: str):
        self.unique_constraint_states[constraint] = True
        self.index_states[constraint] = True

    def handle_foreign_constraint(self, constraint: str):
        self.foreign_constraint_states[constraint] = True

    def handle_index(self, constraint: str):
        self.index_states[constraint] = True

    def __add_json_column(self, backend: PostgreSQL, table: sa.Table, column: sa.Column):
        meta = JSONColumnMigrateMeta(
            column=column,
        )
        meta.initialize(backend, table)
        self.json_columns[column.name] = meta


def _reserved_constraint(constraint: dict) -> bool:
    return all(column_name.startswith('_') for column_name in constraint["column_names"])


def is_internal_ref(dtype: Ref):
    prop = dtype.prop
    return commands.identifiable(prop)


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
        old_columns = ensure_list(old_columns)

    if not old_columns or not new_property:
        return False

    # Skip ref 4 -> ref 3
    if isinstance(new_property.dtype, Ref):
        return False

    # Check if columns are from ref 4 (can only have 1 column)
    if not (len(old_columns) == 1 and isinstance(old_columns[0], sa.Column)):
        return False

    ref_col = old_columns[0]
    # Check if it is internal (should end with '._id'
    if not ref_col.name.endswith('._id'):
        return False

    inspector = meta.inspector
    handler = meta.handler
    rename = meta.rename

    manifest = new_property.model.manifest

    constraints = inspector.get_foreign_keys(table.name)
    ref_model = None
    table_name = None
    # Try to find referred table's matching model
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

    if not ref_model:
        return False

    ref_primary_keys = get_spinta_primary_keys(
        table_name=table_name,
        model=ref_model,
        inspector=inspector
    )

    if len(ref_primary_keys) > 1:
        raise MigrateScalarToRefTooManyKeys(new_property.dtype, primary_keys=[key for key in ref_primary_keys])

    ref_primary_property = ref_model.flatprops[ref_primary_keys[0]]
    ref_primary_column = commands.prepare(context, backend, ref_primary_property)
    column_name = get_pg_name(get_column_name(new_property))
    updated_kwargs = adjust_kwargs(kwargs, {
        'foreign_key': True
    })

    commands.migrate(context, backend, meta, table, NA, new_property, **updated_kwargs)
    table_name = get_pg_table_name(rename.get_table_name(table.name))
    foreign_table_name = get_pg_table_name(get_table_name(ref_model))
    handler.add_action(
        ma.DowngradeTransferDataMigrationAction(
            table_name,
            foreign_table_name,
            ref_col,
            {
                column_name: ref_primary_column
            },
            '_id'
        ),
        foreign_key=True
    )
    commands.migrate(context, backend, meta, table, ref_col, NA, **updated_kwargs)
    return True


def extract_target_column(rename: MigrateRename, columns: list, table: sa.Table, prop: Property):
    full_name = rename.get_old_column_name(table.name, prop.name)
    if isinstance(columns, list):
        for col in columns:
            if isinstance(col, sa.Column) and col.name == full_name:
                return [col]
    return columns


def adjust_kwargs(kwargs: dict, new: dict) -> dict:
    copied = kwargs.copy()
    copied.update(new)
    return copied


def extract_literal_name_from_column(
    column: sa.Column,
) -> str:
    type_ = column.type.compile(dialect=postgresql.dialect())

    # Convert sa.Float, to postgresql DOUBLE PRECISION type
    if isinstance(column.type, sa.Float):
        type_ = 'DOUBLE PRECISION'

    return type_


def extract_using_from_columns(
    old_column: sa.Column,
    new_column: sa.Column,
    type_
):
    using = None
    if (isinstance(old_column.type, geoalchemy2.types.Geometry)
        and isinstance(new_column.type, geoalchemy2.types.Geometry)
        and old_column.type.srid != new_column.type.srid
    ):
        srid_name = old_column
        srid = new_column.type.srid
        if old_column.type.srid == -1:
            srid_name = sa.func.ST_SetSRID(old_column, 4326)
        if new_column.type.srid == -1:
            srid = 4326
        using = sa.func.ST_Transform(srid_name, srid).compile(compile_kwargs={"literal_binds": True})
    elif type_ is not None:
        using = sa.func.cast(old_column, type_).compile(compile_kwargs={"literal_binds": True})

    return using


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


def constraint_with_name(constraints: list, constraint_name: str):
    try:
        return next(constraint for constraint in constraints if constraint["name"] == constraint_name)
    except StopIteration:
        return None


def constraint_with_columns(constraints: list, column_names: list[str]):
    try:
        return next(constraint for constraint in constraints if constraint["column_names"] == column_names)
    except StopIteration:
        return None


def index_with_columns(indexes: list, column_names: list[str], condition: Callable[[dict], bool] = lambda index: True):
    try:
        return next(index for index in indexes if index["column_names"] == column_names and condition(index))
    except StopIteration:
        return None


def index_with_name(indexes: list, index_name: str, condition: Callable[[dict], bool] = lambda index: True):
    try:
        return next(index for index in indexes if index["name"] == index_name and condition(index))
    except StopIteration:
        return None


def index_not_handled_condition(meta: MigrateModelMeta):
    return lambda index: not meta.index_states[index['name']]


def contains_unique_constraint(constraints: list, column_name: str):
    return any(constraint["column_names"] == [column_name] for constraint in constraints)


def contains_constraint_name(constraints: list, constraint_name: str):
    return any(constraint["name"] == constraint_name for constraint in constraints)


def contains_index(indexes: list, column_name: str):
    return any(index["column_names"] == [column_name] for index in indexes)


def contains_foreign_key_with_table_columns(constraints: list, table_name: str, column_names: list[str]):
    return any(
        constraint["constrained_columns"] == column_names and constraint["referred_table"] == table_name for constraint
        in constraints)


def handle_unique_constraint_migration(
    table: sa.Table,
    table_name: str,
    old: sa.Column,
    new: sa.Column,
    column_name: str,
    handler: MigrationHandler,
    inspector: Inspector,
    foreign_key: bool,
    renamed: bool,
    meta: MigrateModelMeta
):
    if not new.unique:
        return

    unique_name = get_pg_constraint_name(table_name, column_name)

    if meta.unique_constraint_states[unique_name]:
        return

    unique_constraints = inspector.get_unique_constraints(table_name=table.name)
    constraint_column = old.name if renamed else column_name

    meta.handle_unique_constraint(unique_name)
    old_constraint = constraint_with_columns(unique_constraints, [constraint_column])
    if old_constraint and old_constraint['name'] == unique_name:
        return

    if not contains_constraint_name(unique_constraints, unique_name):
        if old_constraint:
            meta.handle_unique_constraint(old_constraint['name'])
            handler.add_action(ma.RenameConstraintMigrationAction(
                table_name=table_name,
                old_constraint_name=old_constraint['name'],
                new_constraint_name=unique_name
            ))
            return

        handler.add_action(ma.CreateUniqueConstraintMigrationAction(
            constraint_name=unique_name,
            table_name=table_name,
            columns=[column_name]
        ), foreign_key)
        return

    if not contains_unique_constraint(unique_constraints, constraint_column):
        handler.add_action(ma.DropConstraintMigrationAction(
            constraint_name=unique_name,
            table_name=table_name,
        ), foreign_key)

        handler.add_action(ma.CreateUniqueConstraintMigrationAction(
            constraint_name=unique_name,
            table_name=table_name,
            columns=[column_name]
        ), foreign_key)


def _index_using_suffix(
    column: sa.Column
):
    if isinstance(column.type, geoalchemy2.types.Geometry):
        return "GIST"

    return None


def _requires_index(
    column: sa.Column,
    skip_unique: bool = True
) -> bool:
    if isinstance(column.type, geoalchemy2.types.Geometry):
        return True

    # This is handled with UniqueConstraint handler
    if skip_unique and column.unique:
        return False

    return column.index


def handle_index_migration(
    table: sa.Table,
    table_name: str,
    old: sa.Column,
    new: sa.Column,
    column_name: str,
    handler: MigrationHandler,
    inspector: Inspector,
    foreign_key: bool,
    renamed: bool,
    meta: MigrateModelMeta
):
    if not _requires_index(new):
        return

    index_name = get_pg_index_name(table_name=table_name, columns=[column_name])
    if meta.index_states[index_name]:
        return

    constraint_column = old.name if renamed else column_name
    indexes = inspector.get_indexes(table_name=table.name)
    using = _index_using_suffix(new)

    # Check unhandled index with same columns
    existing_index = index_with_columns(indexes, [constraint_column], condition=index_not_handled_condition(meta))
    meta.handle_index(index_name)
    if existing_index is not None:
        if existing_index['name'] == index_name:
            return

        meta.handle_index(existing_index["name"])
        handler.add_action(ma.RenameIndexMigrationAction(
            old_index_name=existing_index["name"],
            new_index_name=index_name
        ))
        return

    # Check index with existing name
    if contains_index(indexes, index_name):
        existing_index = index_with_name(indexes, index_name)
        if existing_index['column_names'] == [constraint_column]:
            return

        handler.add_action(
            ma.DropIndexMigrationAction(
                index_name=index_name,
                table_name=table_name
            ), foreign_key)
        handler.add_action(
            ma.CreateIndexMigrationAction(
                index_name=index_name,
                table_name=table_name,
                columns=[constraint_column],
                using=using
            ), foreign_key)
        return

    handler.add_action(
        ma.CreateIndexMigrationAction(
            index_name=index_name,
            table_name=table_name,
            columns=[constraint_column],
            using=using
        ), foreign_key)


def extract_sqlalchemy_columns(data: list) -> List[sa.Column]:
    return [item for item in data if isinstance(item, sa.Column)]


def reduce_columns(data: list) -> Union[sa.Column, list[sa.Column]]:
    return data[0] if len(data) == 1 else data


def is_internal(
    columns: List[sa.Column],
    base_name: str,
    table_name: str,
    ref_table_name: str,
    inspector: Inspector
) -> bool:
    column_name = get_pg_column_name(f'{base_name}._id')
    contains_column = any(column.name == column_name for column in columns)

    if not contains_column:
        return False

    foreign_keys = inspector.get_foreign_keys(table_name)
    return contains_foreign_key_with_table_columns(foreign_keys, ref_table_name, [column_name])


def split_columns(
    columns: List[sa.Column],
    base_name: str,
    target_base_name: str,
    internal: bool,
    ref_table_primary_key_names: List[str],
    target_primary_column_names: List[str],
    target_children_column_names: List[str]
):
    all_column_names = [column.name for column in columns]
    primary_columns = []
    children_columns = []

    # If we know that old columns contain internal ref key, that means we can extract it and everything else are children
    if internal:
        for column in columns:
            column_name = column.name
            if column_name.endswith('_id') and column_name.replace(f'{base_name}.', '') == '_id':
                primary_columns = [column]
                continue

            children_columns.append(column)
        return primary_columns, children_columns

    # Check if all old columns contain all target children columns
    all_children_match = all(column_name.replace(target_base_name, base_name) in all_column_names for column_name in
                             target_children_column_names)

    # Check if all old columns contain all target primary key columns
    all_primary_match = all(column_name.replace(target_base_name, base_name) in all_column_names for column_name in
                            target_primary_column_names)

    if all_primary_match:
        # Primary keys are priority so if they all match we assume:
        # primary_columns = all target_primary_columns
        # children_columns = everything else
        for column in columns:
            column_name = column.name.replace(base_name, target_base_name)
            if column_name in target_primary_column_names:
                primary_columns.append(column)
                continue

            children_columns.append(column)
        return primary_columns, children_columns

    elif all_children_match:
        # If all primary keys do not match, but children do, we need to figure out what is the foreign key
        remaining_column_names = [column.name for column in columns if
                                  column.name.replace(base_name, target_base_name) not in target_children_column_names]
        all_remaining_match = all(
            f'{base_name}.{column_name}' in remaining_column_names for column_name in ref_table_primary_key_names)

        if all_remaining_match:
            # If all renaming columns, after children, match ref tables primary keys, we assume that foreign keys
            # are ref table primary keys
            # primary_columns = all ref primary keys
            # children_columns = everything else
            for column in columns:
                column_name = column.name.replace(f'{base_name}.', '')
                if column_name in ref_table_primary_key_names:
                    primary_columns.append(column)
                    continue

                children_columns.append(column)
            return primary_columns, children_columns

        # We could not find primary key source, so we assume that everything that remains is primary key
        for column in columns:
            column_name = column.name
            if column_name in remaining_column_names:
                primary_columns.append(column)
                continue

            children_columns.append(column)
        return primary_columns, children_columns

    else:
        # Nothing matches we can only try to guess
        converted_ref_names = [f'{base_name}.{column_name}' for column_name in ref_table_primary_key_names]
        all_ref_primary_keys_match = all(
            column_name in all_column_names for column_name in converted_ref_names)

        if all_ref_primary_keys_match:
            # Since all ref model's primary keys are in the list, we can assume, that this is the foreign key
            for column in columns:
                column_name = column.name
                if column_name in converted_ref_names:
                    primary_columns.append(column)
                    continue

                children_columns.append(column)
            return primary_columns, children_columns

    # If nothing worked then we assume that all columns are foreign keys
    for column in columns:
        primary_columns.append(column.name)
    return primary_columns, children_columns


def get_spinta_primary_keys(
    table_name: str,
    model: Model,
    inspector: Inspector
) -> List[str]:
    """Extracts `manifest` declared primary keys (from internal PostgresSql)

    Args:
        table_name: old table's name
        model: new table's model
        inspector: SQLAlchemy Inspector object

    Since spinta on internal backend does not actually store primary keys as `PrimaryKey`
    You can only know if primary key was set in manifest through `UniqueConstraint`
    `PrimaryKey` is reserved for `_id` property
    Issue is that it is not mandatory to set primary key
    Also you can explicitly create unique constraints through manifest, meaning you cannot be certain
    that the `UniqueConstraint` you find is actually primary key
    """

    unique_constraints = inspector.get_unique_constraints(table_name)
    unique_constraint_columns = [constraint['column_names'] for constraint in unique_constraints]

    if not unique_constraint_columns:
        return []

    if not model.external.unknown_primary_key:
        # If model contains primary key, we might be able to find column combination
        # which would take priority
        primary_property_names = [prop.place for prop in model.external.pkeys]
        for constraint in unique_constraints:
            if set(constraint) == set(primary_property_names):
                return constraint

    # New model does have declared primary key, making it hard to predict if table had it set before
    if len(unique_constraint_columns) == 1:
        # If there is only 1 combination
        return unique_constraint_columns[0]
    return []


def remap_and_rename_columns(
    base_name: str,
    columns: List[sa.Column],
    table_name: str,
    ref_table_name: str,
    rename: MigrateRename
) -> dict:
    result = {}
    for column in columns:
        name = rename.get_column_name(table_name, column.name)
        # Handle nested renaming from 2 tables
        if column.name.startswith(base_name):
            leaf_name = column.name.removeprefix(base_name)
            if leaf_name.startswith('.'):
                leaf_name = leaf_name[1:]

            base_renamed = nested_column_rename(base_name, table_name, rename)
            leaf_renamed = nested_column_rename(leaf_name, ref_table_name, rename)
            name = f"{base_renamed}.{leaf_renamed}"
        result[name] = column
    return result


def remove_property_prefix_from_column_name(
    column_name: str,
    prop: Property,
) -> str:
    return column_name.replace(f'{prop.place}.', '', 1)


def zip_and_migrate_properties(
    context: Context,
    backend: PostgreSQL,
    old_table: sa.Table,
    new_model: Model,
    old_columns: List[sa.Column],
    new_properties: List[Property],
    meta: MigratePostgresMeta,
    rename: MigrateRename,
    root_name: str = "",
    **kwargs
):
    zipped_items = zipitems(
        old_columns,
        new_properties,
        lambda x: property_and_column_name_key(x, rename, old_table, new_model, root_name=root_name)
    )
    for zipped_item in zipped_items:
        old_columns = []
        new_properties = []
        for old_column, new_property in zipped_item:
            # Ignore deleted / reserved properties
            if new_property and new_property.name.startswith('_'):
                continue

            if old_column not in old_columns:
                old_columns.append(old_column)
            if new_property and new_property not in new_properties:
                new_properties.append(new_property)

        if len(old_columns) == 1:
            old_columns = old_columns[0]
        elif not old_columns:
            old_columns = NA
            # If neither column nor property is matched skip it
            if not new_properties:
                continue
        if new_properties:
            for new_property in new_properties:
                handled = handle_internal_ref_to_scalar_conversion(
                    context,
                    backend,
                    meta,
                    old_table,
                    old_columns,
                    new_property,
                    **kwargs
                )

                if not handled:
                    commands.migrate(context, backend, meta, old_table, old_columns, new_property,
                                     **kwargs)
        else:
            commands.migrate(context, backend, meta, old_table, old_columns, NA,
                             **kwargs)
