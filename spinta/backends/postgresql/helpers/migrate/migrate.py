from __future__ import annotations

import dataclasses
import enum
import json
import os
from collections import defaultdict
from typing import Any, List, Union, Dict, Tuple, Callable

import geoalchemy2.types
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB, BIGINT, ARRAY, JSON
from sqlalchemy.engine.reflection import Inspector

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.constants import TableType, BackendFeatures
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_name, get_column_name
from spinta.backends.postgresql.helpers.migrate.actions import MigrationHandler
from spinta.backends.postgresql.helpers.name import (
    name_changed,
    get_pg_constraint_name,
    get_pg_index_name,
    get_pg_table_name,
    get_pg_column_name,
)
from spinta.cli.helpers.migrate import MigrationContext
from spinta.components import Context, Model, Property
from spinta.datasets.inspect.helpers import zipitems
from spinta.exceptions import (
    MigrateScalarToRefTooManyKeys,
    UnableToFindPrimaryKeysNoUniqueConstraints,
    UnableToFindPrimaryKeysMultipleUniqueConstraints,
    ModelNotFound,
    PropertyNotFound,
    FileNotFound,
)
from spinta.manifests.components import Manifest
from spinta.types.datatype import Ref, File, Array, Object, DataType
from spinta.types.text.components import Text
from spinta.utils.itertools import ensure_list
from spinta.utils.nestedstruct import get_root_attr
from spinta.utils.schema import NA


class CastSupport(enum.Enum):
    # Doest not support casting
    INVALID = 0
    # Supports based on context (can only be resolved runtime, which can cause unexpected errors)
    UNSAFE = 1
    # Has direct support from backend
    VALID = 2


class CastMatrix:
    _cache: dict[tuple[str, str], CastSupport]
    engine: sa.engine.Engine

    def __init__(self, engine: sa.engine.Engine):
        self._cache = {}
        self.engine = engine

    def supports(self, from_type: str, to_type: str) -> CastSupport:
        key = (from_type, to_type)

        if key in self._cache:
            return self._cache[key]

        self._cache[key] = self.__supports_exec(from_type, to_type)
        return self._cache[key]

    def __supports_exec(self, from_type: str, to_type: str) -> CastSupport:
        """
        Checks postgresql cast table between given type strings
        """

        with self.engine.connect() as conn:
            result = conn.execute(
                sa.text("""
            SELECT 1
            FROM pg_cast
            WHERE castsource = CAST(:source AS regtype)
              AND casttarget = CAST(:target AS regtype)
            LIMIT 1
            """),
                {"source": from_type, "target": to_type},
            ).scalar()

        result = result is not None
        if result:
            return CastSupport.VALID

        result = self.__runtime_cast_exec(from_type, to_type)
        return result

    def __runtime_cast_exec(self, from_type: str, to_type: str) -> CastSupport:
        """
        Checks for unsafe casting between 2 types using runtime
        """
        with self.engine.connect() as conn:
            try:
                conn.execute(
                    sa.text("SELECT NULL::" + from_type + "::" + to_type),
                ).scalar()
                return CastSupport.UNSAFE
            except Exception as _:
                return CastSupport.INVALID


class RenameMap:
    @dataclasses.dataclass
    class _Name:
        normal: str
        compressed: str

    @dataclasses.dataclass
    class _TableRename:
        old: "RenameMap._Name"
        new: "RenameMap._Name" | None
        columns: Dict[str, str]

        def get_new_name(self, fallback: bool = False) -> str | None:
            if self.new is None:
                if fallback:
                    return self.get_old_name()

                return None

            return self.new.normal

        def get_old_name(self) -> str:
            return self.old.normal

    tables: Dict[str, _TableRename]

    def __init__(self, rename_src: str | dict):
        self.tables = {}
        self.parse_rename_src(rename_src)

    def _find_new_table(self, name: str, compressed: bool) -> _TableRename | None:
        if name in self.tables:
            return self.tables[name]

        for table in self.tables.values():
            table_name = table.old.compressed if compressed else table.old.normal
            if table_name == name:
                return table

        return None

    def _find_old_table(self, name: str, compressed: bool) -> _TableRename | None:
        for table in self.tables.values():
            if table.new is None:
                continue

            table_name = table.new.compressed if compressed else table.new.normal
            if table_name == name:
                return table

        return None

    def insert_table(self, old_name: str, new_name: str | None = None):
        self.tables[old_name] = self._TableRename(
            old=self._Name(normal=old_name, compressed=get_pg_table_name(old_name)),
            new=self._Name(normal=new_name, compressed=get_pg_table_name(new_name)) if new_name else None,
            columns={},
        )

    def insert_column(self, table_name: str, column_name: str, new_column_name: str):
        if table_name not in self.tables.keys():
            self.insert_table(table_name)
        if column_name == "":
            self.tables[table_name].new = self._Name(
                normal=new_column_name, compressed=get_pg_table_name(new_column_name)
            )
            return

        self.tables[table_name].columns[column_name] = new_column_name

    def get_column_name(self, table_name: str, column_name: str, root_only: bool = False, root_value: str = ""):
        # If table does not have renamed, return given column
        table = self._find_new_table(table_name, compressed=True)
        if table is None:
            return column_name

        columns = table.columns

        if column_name in columns:
            return columns[column_name]

        # If column was not directly set, and it cannot be mapped through root node, return it
        if not root_only:
            return column_name

        root_attr = get_root_attr(column_name, initial_root=root_value)
        for old_column_name, new_column_name in columns.items():
            target_root_attr = get_root_attr(old_column_name, initial_root=root_value)
            if root_attr == target_root_attr:
                new_name = get_root_attr(new_column_name, initial_root=root_value)
                return new_name
        return column_name

    def get_old_column_name(self, table_name: str, column_name: str, root_only: bool = False, root_value: str = ""):
        table = self._find_new_table(table_name, compressed=True)
        if table is None:
            return column_name

        given_name = get_root_attr(column_name, initial_root=root_value) if root_only else column_name
        for old_column_column, new_column_name in table.columns.items():
            target_name = get_root_attr(new_column_name, initial_root=root_value) if root_only else new_column_name

            if target_name == given_name:
                old_name = get_root_attr(old_column_column, initial_root=root_value) if root_only else old_column_column
                return old_name
        return column_name

    # Compressed default True, because in most cases we want new name from old tables, which are compressed
    def get_table_name(self, table_name: str, compressed: bool = True) -> str:
        table = self._find_new_table(table_name, compressed=compressed)
        if table is None:
            return table_name

        name = table.get_new_name()
        if name is not None:
            table_name = name

        return table_name

    # Compressed default False, because in most cases we want old name from model name, which is not compressed
    def get_old_table_name(self, table_name: str, compressed: bool = False) -> str:
        table = self._find_old_table(table_name, compressed=compressed)
        if table is None:
            return table_name

        return table.get_old_name()

    def parse_rename_src(self, rename_src: str | dict):
        def _parse_dict(src: dict):
            for table, table_data in src.items():
                table_rename = table_data.pop("", None)
                self.insert_table(table, table_rename)
                for column, column_data in table_data.items():
                    self.insert_column(table, column, column_data)

        if rename_src:
            if isinstance(rename_src, str):
                if os.path.exists(rename_src):
                    with open(rename_src, "r") as f:
                        data = json.loads(f.read())
                        _parse_dict(data)
                else:
                    raise FileNotFound(file=rename_src)
            else:
                _parse_dict(rename_src)


@dataclasses.dataclass
class PostgresqlMigrationContext(MigrationContext):
    inspector: Inspector
    rename: RenameMap
    handler: MigrationHandler
    cast_matrix: CastMatrix


@dataclasses.dataclass
class JSONMigrationContext:
    column: sa.Column
    prop: Property

    # Currently defined keys
    keys: List[str] = dataclasses.field(default_factory=list)
    # Added new keys
    new_keys: Dict[str, str] = dataclasses.field(default_factory=dict)
    cast_to: Tuple[sa.Column, str] = dataclasses.field(default=None)
    new_name: str = dataclasses.field(default=None)
    empty: bool = False

    # This could be considered a hack, but by default whenever json migration context is create
    # we assume that it's going to be deleted (since it requires old json column).
    # Main reason for this logic, is that normal property zip no longer properly works on json columns
    # since it cannot split mapping by keys.
    full_remove: bool = dataclasses.field(default=True)

    def initialize(self, backend, table):
        self.keys = jsonb_keys(backend, self.column, table)
        self.empty = empty_table(backend, table)

    def add_new_key(self, old_key: str, new_key: str):
        if old_key in self.keys and old_key not in self.new_keys:
            self.new_keys[old_key] = new_key


@dataclasses.dataclass
class PropertyMigrationContext:
    prop: Property
    model_context: ModelMigrationContext


@dataclasses.dataclass
class ModelMigrationContext:
    model: Model
    table: sa.Table

    json_columns: Dict[str, JSONMigrationContext] = dataclasses.field(default_factory=dict)
    unique_constraint_states: Dict[str, bool] = dataclasses.field(default_factory=lambda: defaultdict(lambda: False))
    foreign_constraint_states: Dict[str, bool] = dataclasses.field(default_factory=lambda: defaultdict(lambda: False))
    index_states: Dict[str, bool] = dataclasses.field(default_factory=lambda: defaultdict(lambda: False))

    def initialize(self, inspector: Inspector):
        constraints = inspector.get_unique_constraints(self.table.name)
        for constraint in constraints:
            if not _reserved_constraint(constraint):
                self.unique_constraint_states[constraint["name"]] = False

        constraints = inspector.get_foreign_keys(self.table.name)
        for constraint in constraints:
            self.foreign_constraint_states[constraint["name"]] = False

        indexes = inspector.get_indexes(self.table.name)
        for index in indexes:
            if not _reserved_constraint(index):
                self.index_states[index["name"]] = False

    def mark_unique_constraint_handled(self, constraint: str):
        self.unique_constraint_states[constraint] = True
        self.index_states[constraint] = True

    def mark_foreign_constraint_handled(self, constraint: str):
        self.foreign_constraint_states[constraint] = True

    def mark_index_handled(self, index: str):
        self.index_states[index] = True

    def create_json_context(
        self, backend: PostgreSQL, column: sa.Column, prop: Property, remove: bool = True
    ) -> JSONMigrationContext:
        meta = JSONMigrationContext(column=column, prop=prop, full_remove=remove)
        meta.initialize(backend, self.table)
        self.json_columns[column.name] = meta
        return meta


def drop_all_indexes_and_constraints(inspector: Inspector, table: str, new_table: str, handler: MigrationHandler):
    constraints = inspector.get_unique_constraints(table)
    removed = []
    foreign_keys = inspector.get_foreign_keys(table)
    for key in foreign_keys:
        handler.add_action(ma.DropConstraintMigrationAction(table_name=new_table, constraint_name=key["name"]), True)

    for constraint in constraints:
        removed.append(constraint["name"])
        handler.add_action(ma.DropConstraintMigrationAction(table_name=new_table, constraint_name=constraint["name"]))
    indexes = inspector.get_indexes(table)
    for index in indexes:
        if index["name"] not in removed:
            handler.add_action(ma.DropIndexMigrationAction(table_name=new_table, index_name=index["name"]))


def create_changelog_table(context: Context, new: Model, handler: MigrationHandler):
    table_name = get_pg_name(get_table_name(new, TableType.CHANGELOG))
    pkey_type = commands.get_primary_key_type(context, new.backend)
    handler.add_action(
        ma.CreateTableMigrationAction(
            table_name=table_name,
            columns=[
                sa.Column("_id", BIGINT, primary_key=True, autoincrement=True),
                sa.Column("_revision", sa.String),
                sa.Column("_txn", pkey_type, index=True),
                sa.Column("_rid", pkey_type),
                sa.Column("datetime", sa.DateTime),
                sa.Column("action", sa.String(8)),
                sa.Column("data", JSONB),
            ],
        )
    )


def create_redirect_table(context: Context, new: Model, handler: MigrationHandler):
    table_name = get_pg_name(get_table_name(new, TableType.REDIRECT))
    pkey_type = commands.get_primary_key_type(context, new.backend)
    handler.add_action(
        ma.CreateTableMigrationAction(
            table_name=table_name,
            columns=[
                sa.Column("_id", pkey_type, primary_key=True),
                sa.Column("redirect", pkey_type, index=True),
            ],
        )
    )


def handle_new_file_type(
    context: Context,
    backend: PostgreSQL,
    inspector: Inspector,
    prop: Property,
    pkey_type: Any,
    handler: MigrationHandler,
) -> list:
    name = get_column_name(prop)
    nullable = not prop.dtype.required
    columns = []
    columns += [
        sa.Column(f"{name}._id", sa.String, nullable=nullable),
        sa.Column(f"{name}._content_type", sa.String, nullable=nullable),
        sa.Column(f"{name}._size", BIGINT, nullable=nullable),
    ]
    if BackendFeatures.FILE_BLOCKS in prop.dtype.backend.features:
        columns += [
            sa.Column(f"{name}._bsize", sa.Integer, nullable=nullable),
            sa.Column(
                f"{name}._blocks",
                ARRAY(
                    pkey_type,
                ),
                nullable=nullable,
            ),
        ]
    new_table = get_pg_name(get_table_name(prop, TableType.FILE))
    if not inspector.has_table(new_table):
        handler.add_action(
            ma.CreateTableMigrationAction(
                table_name=new_table,
                columns=[sa.Column("_id", pkey_type, primary_key=True), sa.Column("_block", sa.LargeBinary)],
            )
        )
    return columns


def handle_new_array_type(
    context: Context,
    backend: PostgreSQL,
    inspector: Inspector,
    prop: Property,
    pkey_type: Any,
    handler: MigrationHandler,
):
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
            handler.add_action(
                ma.CreateTableMigrationAction(
                    table_name=new_table,
                    columns=[
                        sa.Column("_txn", pkey_type, index=True),
                        sa.Column(
                            "_rid",
                            pkey_type,
                            sa.ForeignKey(
                                f"{main_table_name}._id",
                                ondelete="CASCADE",
                            ),
                            index=True,
                        ),
                        *new_columns,
                    ],
                )
            )
    return columns


def handle_new_object_type(
    context: Context,
    backend: PostgreSQL,
    inspector: Inspector,
    prop: Property,
    pkey_type: Any,
    handler: MigrationHandler,
):
    columns = []
    if isinstance(prop.dtype, Object) and prop.dtype.properties:
        for new_prop in prop.dtype.properties.values():
            if prop.name.startswith("_") and prop.name not in ("_revision",):
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
            name = f"{name}._id"
        else:
            for refprop in prop.dtype.refprops:
                yield f"{name}.{refprop.name}"
    yield name


def json_has_key(backend: PostgreSQL, column: sa.Column, table: sa.Table, key: str):
    with backend.engine.begin() as connection:
        query = sa.select(table.select().where(column.has_key(key)).exists())
        return connection.execute(query).scalar()


def jsonb_keys(backend: PostgreSQL, column: sa.Column, table: sa.Table):
    with backend.engine.begin() as connection:
        keys = sa.func.jsonb_object_keys(column)
        query = sa.select([keys]).select_from(table).group_by(keys)
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
    return "." in name or "@" in name


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
    item: Union[sa.Column, Property], rename, table: sa.Table, model: Model, root_name: str = ""
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
    return None


def _reserved_constraint(constraint: dict) -> bool:
    return all(column_name.startswith("_") for column_name in constraint["column_names"])


def is_internal_ref(dtype: Ref):
    prop = dtype.prop
    return commands.identifiable(prop)


def handle_internal_ref_to_scalar_conversion(
    context: Context,
    backend: PostgreSQL,
    migration_context: PostgresqlMigrationContext,
    model_context: ModelMigrationContext,
    table: sa.Table,
    old_columns: List[sa.Column],
    new_property: Property,
    **kwargs,
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
    if not ref_col.name.endswith("._id"):
        return False

    inspector = migration_context.inspector
    handler = migration_context.handler
    rename = migration_context.rename

    manifest = new_property.model.manifest

    constraints = inspector.get_foreign_keys(table.name)
    ref_model = None
    table_name = None
    # Try to find referred table's matching model
    for constraint in constraints:
        if constraint["constrained_columns"] == [ref_col.name]:
            table_name = constraint["referred_table"]
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

    ref_primary_keys = get_spinta_primary_keys(table_name=table_name, model=ref_model, inspector=inspector, error=True)

    if len(ref_primary_keys) > 1:
        raise MigrateScalarToRefTooManyKeys(new_property.dtype, primary_keys=[key for key in ref_primary_keys])

    ref_primary_property = ref_model.flatprops[ref_primary_keys[0]]
    ref_primary_column = commands.prepare(context, backend, ref_primary_property)
    column_name = get_pg_name(get_column_name(new_property))
    updated_kwargs = adjust_kwargs(kwargs, {"foreign_key": True})

    commands.migrate(context, backend, migration_context, model_context, table, NA, new_property, **updated_kwargs)
    table_name = get_pg_table_name(rename.get_table_name(table.name))
    foreign_table_name = get_pg_table_name(get_table_name(ref_model))
    handler.add_action(
        ma.DowngradeTransferDataMigrationAction(
            table_name, foreign_table_name, ref_col, {column_name: ref_primary_column}, "_id"
        ),
        foreign_key=True,
    )
    commands.migrate(context, backend, migration_context, model_context, table, ref_col, NA, **updated_kwargs)
    return True


def extract_target_column(rename: RenameMap, columns: list, table: sa.Table, prop: Property):
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
        type_ = "DOUBLE PRECISION"

    return type_


def extract_using_from_columns(old_column: sa.Column, new_column: sa.Column, type_):
    using = None
    if (
        isinstance(old_column.type, geoalchemy2.types.Geometry)
        and isinstance(new_column.type, geoalchemy2.types.Geometry)
        and old_column.type.srid != new_column.type.srid
    ):
        srid_name = old_column
        srid = new_column.type.srid
        if old_column.type.srid == -1:
            srid_name = sa.func.ST_SetSRID(old_column, 4326)
        if new_column.type.srid == -1:
            srid = 4326
        using = sa.func.ST_Transform(srid_name, srid).compile(
            compile_kwargs={"literal_binds": True}, dialect=postgresql.dialect()
        )
    elif type_ is not None:
        using = sa.func.cast(old_column, type_).compile(
            compile_kwargs={"literal_binds": True}, dialect=postgresql.dialect()
        )

    return using


# Match [
#   (
#       (old_column_name, old_type),
#       (new_column_name, new_type)
#   )
# ]
def generate_type_missmatch_exception_details(columns: list):
    result = ""
    for pair in columns:
        old_data = pair[0]
        new_data = pair[1]
        result += f"\t'{old_data[0]}' [{old_data[1]}] -> '{new_data[0]}' [{new_data[1]}]\t"
        if old_data[1] == new_data[1]:
            result += f"'{old_data[1]}' == '{new_data[1]}'\n"
        else:
            result += f"'{old_data[1]}' != '{new_data[1]}'\t<= Incorrect\n"
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


def index_not_handled_condition(model_context: ModelMigrationContext):
    return lambda index: not model_context.index_states[index["name"]]


def contains_unique_constraint(constraints: list, column_name: str):
    return any(constraint["column_names"] == [column_name] for constraint in constraints)


def contains_constraint_name(constraints: list, constraint_name: str):
    return any(constraint["name"] == constraint_name for constraint in constraints)


def contains_index(indexes: list, column_name: str):
    return any(index["column_names"] == [column_name] for index in indexes)


def contains_foreign_key_with_table_columns(constraints: list, table_name: str, column_names: list[str]):
    return any(
        constraint["constrained_columns"] == column_names and constraint["referred_table"] == table_name
        for constraint in constraints
    )


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
    model_context: ModelMigrationContext,
):
    if not new.unique:
        return

    unique_name = get_pg_constraint_name(table_name, column_name)

    if model_context.unique_constraint_states[unique_name]:
        return

    unique_constraints = inspector.get_unique_constraints(table_name=table.name)
    constraint_column = old.name if renamed else column_name

    model_context.mark_unique_constraint_handled(unique_name)
    old_constraint = constraint_with_columns(unique_constraints, [constraint_column])
    if old_constraint and old_constraint["name"] == unique_name:
        return

    if not contains_constraint_name(unique_constraints, unique_name):
        if old_constraint:
            model_context.mark_unique_constraint_handled(old_constraint["name"])
            handler.add_action(
                ma.RenameConstraintMigrationAction(
                    table_name=table_name, old_constraint_name=old_constraint["name"], new_constraint_name=unique_name
                )
            )
            return

        handler.add_action(
            ma.CreateUniqueConstraintMigrationAction(
                constraint_name=unique_name, table_name=table_name, columns=[column_name]
            ),
            foreign_key,
        )
        return

    if not contains_unique_constraint(unique_constraints, constraint_column):
        handler.add_action(
            ma.DropConstraintMigrationAction(
                constraint_name=unique_name,
                table_name=table_name,
            ),
            foreign_key,
        )

        handler.add_action(
            ma.CreateUniqueConstraintMigrationAction(
                constraint_name=unique_name, table_name=table_name, columns=[column_name]
            ),
            foreign_key,
        )


def _index_using_suffix(column: sa.Column):
    if isinstance(column.type, geoalchemy2.types.Geometry):
        return "GIST"

    return None


def _requires_index(column: sa.Column, skip_unique: bool = True) -> bool:
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
    model_context: ModelMigrationContext,
):
    if not _requires_index(new):
        return

    index_name = get_pg_index_name(table_name=table_name, columns=[column_name])
    if model_context.index_states[index_name]:
        return

    constraint_column = old.name if renamed else column_name
    indexes = inspector.get_indexes(table_name=table.name)
    using = _index_using_suffix(new)

    # Check unhandled index with same columns
    existing_index = index_with_columns(
        indexes, [constraint_column], condition=index_not_handled_condition(model_context)
    )
    model_context.mark_index_handled(index_name)
    if existing_index is not None:
        if existing_index["name"] == index_name:
            return

        model_context.mark_index_handled(existing_index["name"])
        handler.add_action(
            ma.RenameIndexMigrationAction(old_index_name=existing_index["name"], new_index_name=index_name)
        )
        return

    # Check index with existing name
    if contains_index(indexes, index_name):
        existing_index = index_with_name(indexes, index_name)
        if existing_index["column_names"] == [constraint_column]:
            return

        handler.add_action(ma.DropIndexMigrationAction(index_name=index_name, table_name=table_name), foreign_key)
        handler.add_action(
            ma.CreateIndexMigrationAction(
                index_name=index_name, table_name=table_name, columns=[constraint_column], using=using
            ),
            foreign_key,
        )
        return

    handler.add_action(
        ma.CreateIndexMigrationAction(
            index_name=index_name, table_name=table_name, columns=[constraint_column], using=using
        ),
        foreign_key,
    )


def extract_sqlalchemy_columns(data: list) -> List[sa.Column]:
    return [item for item in data if isinstance(item, sa.Column)]


def reduce_columns(data: list) -> Union[sa.Column, list[sa.Column]]:
    return data[0] if len(data) == 1 else data


def is_internal(
    columns: List[sa.Column], base_name: str, table_name: str, ref_table_name: str, inspector: Inspector
) -> bool:
    column_name = get_pg_column_name(f"{base_name}._id")
    contains_column = any(column.name == column_name for column in columns)

    if not contains_column:
        return False

    foreign_keys = inspector.get_foreign_keys(table_name)
    return contains_foreign_key_with_table_columns(foreign_keys, ref_table_name, [column_name])


def _split_columns_by_reserved_internal_column(
    columns: List[sa.Column],
    old_base_name: str,
):
    primary_columns = []
    children_columns = []

    # If we know that old columns contain internal ref key, that means we can extract it and everything else are children
    for column in columns:
        column_name = column.name
        if column_name.endswith("_id") and column_name.replace(f"{old_base_name}.", "") == "_id":
            primary_columns = [column]
            continue

        children_columns.append(column)
    return primary_columns, children_columns


def _split_columns_by_primary_columns(
    columns: List[sa.Column], old_base_name: str, new_base_name: str, primary_column_names: List[str]
) -> (List[str], List[str]):
    # Primary keys are priority so if they all match we assume:
    # primary_columns = all target_primary_columns
    # children_columns = everything else
    primary_columns = []
    children_columns = []
    for column in columns:
        column_name = column.name.replace(old_base_name, new_base_name)
        if column_name in primary_column_names:
            primary_columns.append(column)
            continue

        children_columns.append(column)
    return primary_columns, children_columns


def _split_columns_by_children_columns(
    columns: List[sa.Column],
    old_base_name: str,
    new_base_name: str,
    children_column_names: List[str],
    ref_table_primary_column_names: List[str],
) -> (List[str], List[str]):
    # If all primary keys do not match, but children do, we need to figure out what is the foreign key
    primary_columns = []
    children_columns = []

    remaining_column_names = [
        column.name
        for column in columns
        if column.name.replace(old_base_name, new_base_name) not in children_column_names
    ]
    all_remaining_match = (
        all(
            f"{old_base_name}.{column_name}" in remaining_column_names for column_name in ref_table_primary_column_names
        )
        if ref_table_primary_column_names
        else False
    )
    if all_remaining_match:
        # If all renaming columns, after children, match ref tables primary keys, we assume that foreign keys
        # are ref table primary keys
        # primary_columns = all ref primary keys
        # children_columns = everything else
        for column in columns:
            column_name = column.name.replace(f"{old_base_name}.", "")
            if column_name in ref_table_primary_column_names:
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


def _split_columns_by_inference(
    columns: List[sa.Column],
    old_base_name: str,
    all_column_names: List[str],
    ref_table_column_names: List[str],
    ref_table_explicit_column_names: List[str],
    ref_table_primary_column_names: List[str],
):
    # Nothing matches we can only try to guess
    primary_columns = []
    children_columns = []

    converted_ref_names = [f"{old_base_name}.{column_name}" for column_name in ref_table_primary_column_names]
    converted_explicit_ref_names = [f"{old_base_name}.{column_name}" for column_name in ref_table_explicit_column_names]

    all_ref_explicit_keys_match = (
        all(column_name in all_column_names for column_name in converted_explicit_ref_names)
        if converted_explicit_ref_names
        else False
    )
    if all_ref_explicit_keys_match:
        # If all explicit ref keys match, we assume that foreign keys are explicit ref keys
        # primary_columns = all explicit ref keys
        # children_columns = everything else
        for column in columns:
            column_name = column.name
            if column_name in converted_explicit_ref_names:
                primary_columns.append(column)
                continue

            children_columns.append(column)
        return primary_columns, children_columns

    all_ref_primary_keys_match = (
        all(column_name in all_column_names for column_name in converted_ref_names) if converted_ref_names else False
    )
    if all_ref_primary_keys_match:
        # Since all ref model's primary keys are in the list, we can assume, that this is the foreign key
        for column in columns:
            column_name = column.name
            if column_name in converted_ref_names:
                primary_columns.append(column)
                continue

            children_columns.append(column)
        return primary_columns, children_columns

    # Check for scalar to ref convertion (it must contain 1 column that matches ref name
    if (scalar_column := next((column for column in columns if column.name == old_base_name), None)) is not None:
        primary_columns.append(scalar_column)

        for column in columns:
            if column != scalar_column:
                children_columns.append(column)
        return primary_columns, children_columns

    # If nothing worked, then we assume that all columns are foreign keys, that exist on target table
    # Filter columns that can be found on a target model (removing denorm columns)

    for column in columns:
        column_name = column.name.replace(f"{old_base_name}.", "")
        if column_name in ref_table_column_names:
            primary_columns.append(column)
        else:
            children_columns.append(column)

    return primary_columns, children_columns


def split_columns(
    old_columns: List[sa.Column],
    old_base_name: str,
    new_base_name: str,
    internal: bool,
    new_primary_column_names: List[str],
    new_children_column_names: List[str],
    ref_table_column_names: List[str],
    ref_table_primary_column_names: List[str],
    ref_explicit_primary_column_names: List[str],
):
    # If we know that old columns contain internal ref key, that means we can extract it and everything else are children
    if internal:
        return _split_columns_by_reserved_internal_column(
            columns=old_columns,
            old_base_name=old_base_name,
        )

    all_column_names = [column.name for column in old_columns]
    # Check if all old columns contain all target children columns
    all_children_match = (
        all(
            column_name.replace(new_base_name, old_base_name) in all_column_names
            for column_name in new_children_column_names
        )
        if new_children_column_names
        else False
    )

    # Check if all old columns contain all target primary key columns
    all_primary_match = (
        all(
            column_name.replace(new_base_name, old_base_name) in all_column_names
            for column_name in new_primary_column_names
        )
        if new_primary_column_names
        else False
    )

    if all_primary_match:
        return _split_columns_by_primary_columns(
            columns=old_columns,
            old_base_name=old_base_name,
            new_base_name=new_base_name,
            primary_column_names=new_primary_column_names,
        )

    if all_children_match:
        return _split_columns_by_children_columns(
            columns=old_columns,
            old_base_name=old_base_name,
            new_base_name=new_base_name,
            children_column_names=new_children_column_names,
            ref_table_primary_column_names=ref_table_primary_column_names,
        )

    return _split_columns_by_inference(
        columns=old_columns,
        old_base_name=old_base_name,
        all_column_names=all_column_names,
        ref_table_column_names=ref_table_column_names,
        ref_table_primary_column_names=ref_table_primary_column_names,
        ref_table_explicit_column_names=ref_explicit_primary_column_names,
    )


def _format_multiple_unique_constraints_error_msg(constraints: list[dict]) -> str:
    result = ""
    for constraint in constraints:
        result += f"\t'{constraint['name']}' [{', '.join(constraint['column_names'])}]\n"
    return result


def get_explicit_primary_keys(ref: Ref, rename: RenameMap) -> List[str]:
    if not ref.explicit:
        return []

    props = ref.refprops
    old_ref_table_name = rename.get_old_table_name(get_table_name(ref.model))
    old_names = [rename.get_old_column_name(old_ref_table_name, prop.name) for prop in props]
    return old_names


def get_spinta_primary_keys(table_name: str, model: Model, inspector: Inspector, error: bool = False) -> List[str]:
    """Extracts `manifest` declared primary keys (from internal PostgresSql)

    Args:
        table_name: old table's name
        model: new table's model
        inspector: SQLAlchemy Inspector object
        error: Raise an error if no primary keys were found

    Since spinta on internal backend does not actually store primary keys as `PrimaryKey`
    You can only know if primary key was set in manifest through `UniqueConstraint`
    `PrimaryKey` is reserved for `_id` property
    Issue is that it is not mandatory to set primary key
    Also you can explicitly create unique constraints through manifest, meaning you cannot be certain
    that the `UniqueConstraint` you find is actually primary key
    """

    unique_constraints = inspector.get_unique_constraints(table_name)
    unique_constraint_columns = [constraint["column_names"] for constraint in unique_constraints]

    if not unique_constraint_columns:
        if error:
            raise UnableToFindPrimaryKeysNoUniqueConstraints(model, table_name=table_name)

        return []

    if not model.external.unknown_primary_key:
        # If model contains primary key, we might be able to find column combination
        # which would take priority
        primary_property_names = [prop.place for prop in model.external.pkeys]
        for constraint in unique_constraint_columns:
            if set(constraint) == set(primary_property_names):
                return constraint

    # New model does not have declared primary key, making it hard to predict if table had it set before
    if len(unique_constraint_columns) == 1:
        # If there is only 1 combination
        return unique_constraint_columns[0]

    if error:
        raise UnableToFindPrimaryKeysMultipleUniqueConstraints(
            model,
            table_name=table_name,
            unique_constraints=_format_multiple_unique_constraints_error_msg(unique_constraints),
        )

    return []


def get_model_column_names(
    table_name: str,
    inspector: Inspector,
):
    columns = inspector.get_columns(table_name)
    return list(column["name"] for column in columns)


def nested_column_rename(column_name: str, table_name: str, rename: RenameMap) -> str:
    renamed = rename.get_column_name(table_name, column_name)
    if name_changed(column_name, renamed):
        return renamed

    if "." in column_name:
        split_name = column_name.split(".")
        renamed = nested_column_rename(".".join(split_name[:-1]), table_name, rename)
        return f"{renamed}.{split_name[-1]}"

    return column_name


def remap_and_rename_columns(
    base_name: str, columns: List[sa.Column], table_name: str, ref_table_name: str, rename: RenameMap
) -> dict:
    result = {}
    for column in columns:
        name = rename.get_column_name(table_name, column.name)
        # Handle nested renaming from 2 tables
        if column.name.startswith(base_name):
            leaf_name = column.name.removeprefix(base_name)
            if leaf_name.startswith("."):
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
    return column_name.replace(f"{prop.place}.", "", 1)


def zip_and_migrate_properties(
    context: Context,
    backend: PostgreSQL,
    old_table: sa.Table,
    new_model: Model,
    old_columns: List[sa.Column],
    new_properties: List[Property],
    migration_context: PostgresqlMigrationContext,
    rename: RenameMap,
    model_context: ModelMigrationContext,
    root_name: str = "",
    **kwargs,
):
    zipped_items = zipitems(
        old_columns,
        new_properties,
        lambda x: property_and_column_name_key(x, rename, old_table, new_model, root_name=root_name),
    )
    for zipped_item in zipped_items:
        old_columns = []
        new_properties = []
        for old_column, new_property in zipped_item:
            # Ignore deleted / reserved properties
            if new_property and new_property.name.startswith("_"):
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
                    context, backend, migration_context, model_context, old_table, old_columns, new_property, **kwargs
                )

                if not handled:
                    commands.migrate(
                        context,
                        backend,
                        migration_context,
                        model_context,
                        old_table,
                        old_columns,
                        new_property,
                        **kwargs,
                    )
        else:
            commands.migrate(context, backend, migration_context, model_context, old_table, old_columns, NA, **kwargs)


def validate_rename_map(context: Context, rename: RenameMap, manifest: Manifest):
    tables = rename.tables.values()
    for table in tables:
        models = commands.get_models(context, manifest)
        name = table.get_new_name(fallback=True)
        if name not in models.keys():
            raise ModelNotFound(model=name)
        model = models.get(name)
        for column in table.columns.values():
            if column not in model.flatprops.keys():
                raise PropertyNotFound(property=column)


def column_cast_warning_message(dtype: DataType, column_name: str, old_type: str, new_type: str) -> str:
    return f"WARNING: Casting '{column_name}' (from '{dtype.prop.model.model_type()}' model) column's type from '{old_type}' to '{new_type}' might not be possible."


def contains_any_table(
    *tables,
    inspector: Inspector,
) -> bool:
    return any(inspector.has_table(table) for table in tables)


def recreate_all_reserved_table_names(
    model: Model,
    old_name: str,
    new_name: str,
    table_type: TableType,
    rename: RenameMap,
) -> (str, str):
    renamed = name_changed(old_name, new_name)
    if not renamed:
        table = get_pg_table_name(model, table_type)
        return table, table

    old_full_name = rename.get_old_table_name(model.name)
    old_table_name = get_pg_table_name(old_full_name, table_type)
    new_table_name = get_pg_table_name(model, table_type)
    return old_table_name, new_table_name
