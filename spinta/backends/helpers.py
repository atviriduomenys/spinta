from __future__ import annotations

import dataclasses
from typing import Any
from typing import Iterable
from typing import Iterator
from typing import TypeVar

import sqlalchemy as sa

from multipledispatch import dispatch

from spinta import commands
from spinta import exceptions
from spinta import spyna
from spinta.auth import authorized
from spinta.backends import Backend
from spinta.backends.components import SelectTree
from spinta.backends.postgresql.helpers import get_pg_name
from spinta.commands import build_full_response
from spinta.components import Config, DataItem
from spinta.core.enums import Action
from spinta.components import Component
from spinta.components import Context
from spinta.components import Model
from spinta.components import Namespace
from spinta.components import Property
from spinta.exceptions import BackendUnavailable
from spinta.types.datatype import DataType, Denorm, String, Base32, PrimaryKey
from spinta.utils.data import take
from spinta.backends.constants import TableType, BackendOrigin


from sqlalchemy.dialects import postgresql

pg_identifier_preparer = postgresql.dialect().identifier_preparer


def validate_and_return_transaction(context: Context, backend: Backend, **kwargs):
    if not backend.available:
        backend.available = commands.wait(context, backend)

        # Backend is still unavailable
        if not backend.available:
            raise BackendUnavailable(backend)

    return backend.transaction(**kwargs)


def validate_and_return_begin(context: Context, backend: Backend, **kwargs):
    if not backend.available:
        backend.available = commands.wait(context, backend)

        # Backend is still unavailable
        if not backend.available:
            raise BackendUnavailable(backend)

    return backend.begin(**kwargs)


def load_backend(
    context: Context, component: Component, name: str, origin: BackendOrigin, data: dict[str, str]
) -> Backend:
    config = context.get("config")
    type_ = data.get("type")
    if not type_:
        raise exceptions.RequiredConfigParam(
            component,
            name=f"backends.{name}.type",
        )
    if type_ not in config.components["backends"]:
        raise exceptions.BackendNotFound(component, name=type_)
    Backend_ = config.components["backends"][type_]
    backend: Backend = Backend_()
    backend.type = type_
    backend.name = name
    backend.origin = origin
    backend.config = data
    load_query_builder_class(config, backend)
    load_result_builder_class(config, backend)
    commands.load(context, backend, data)
    return backend


def get_select_tree(
    context: Context,
    action: Action,
    select: list[str] | None,
) -> SelectTree:
    if isinstance(select, dict):
        select = list(select.keys())

    select = _apply_always_show_id(context, action, select)
    if select is None and action in (Action.GETALL, Action.SEARCH):
        # If select is not given, select everything.
        select = {"*": {}}
    return flat_select_to_nested(select)


def _apply_always_show_id(
    context: Context,
    action: Action,
    select: list[str] | None,
) -> list[str] | None:
    if action in (Action.GETALL, Action.SEARCH):
        config = context.get("config")
        if config.always_show_id:
            if select is None:
                return ["_id"]
            elif "_id" not in select:
                return ["_id"] + select
    return select


def get_select_prop_names(
    context: Context,
    node: Model | Property | DataType,
    props: dict[str, Property],
    action: Action,
    select: SelectTree,
    *,
    # If False, do not check if client has access to this property.
    auth: bool = True,
    # Allowed reserved property names.
    reserved: list[str] = None,
    # If False, do not include Denorm type props
    include_denorm_props: bool = True,
) -> list[str]:
    known = set(reserved or []) | set(take(props))
    check_unknown_props(node, select, known)

    if select is None or "*" in select:
        return [
            p.name
            for p in props.values()
            if (
                not p.name.startswith("_")
                and not p.hidden
                and (not auth or authorized(context, p, action))
                and (include_denorm_props or not isinstance(p.dtype, Denorm))
            )
        ]
    else:
        return list(select)


def select_model_props(
    model: Model,
    prop_names: list[str],
    value: dict,
    select: SelectTree,
    reserved: list[str],
) -> Iterator[
    tuple[
        Property | str,
        Any,
        SelectTree,
    ]
]:
    yield from select_props(
        model,
        reserved,
        model.properties,
        value,
        select,
    )
    yield from select_props(
        model,
        prop_names,
        model.properties,
        value,
        select,
        reserved=False,
    )


T = TypeVar("T")


def select_props(
    node: Namespace | Model | Property,
    keys: Iterable[str],
    props: dict[str, Property],
    value: dict[str, T],
    select: SelectTree,
    *,
    reserved: bool = True,
) -> Iterator[
    tuple[
        Property | str,
        T,
        SelectTree,
    ]
]:
    for key, val, sel in select_keys(keys, value, select, reserved=reserved):
        prop = _select_prop(key, props, node)
        if prop:
            yield prop, val, sel


def select_only_props(
    node: Namespace | Model | Property,
    keys: Iterable[str],
    props: dict[str, Property],
    select: SelectTree,
    *,
    reserved: bool = True,
) -> Iterator[
    tuple[
        Property | str,
        SelectTree,
    ]
]:
    for key, sel in select_only_keys(keys, select, reserved=reserved):
        prop = _select_prop(key, props, node)
        if prop:
            yield prop, sel


def _select_prop(
    key: str,
    props: dict[str, Property],
    node: Namespace | Model | Property,
) -> Property | None:
    if not (prop := props.get(key)) or prop.hidden:
        return None

    return prop


def select_keys(
    keys: Iterable[str],
    value: dict[str, T],
    select: SelectTree,
    *,
    reserved: bool = True,
) -> Iterator[
    tuple[
        str,
        T,
        SelectTree,
    ]
]:
    for key, sel in select_only_keys(keys, select, reserved=reserved):
        if select is None and key not in value:
            # Omit all keys if they are not present in value, this is a common
            # case in PATCH requests.
            continue

        if key in value:
            val = value[key]
        else:
            val = None

        yield key, val, sel


def select_only_keys(
    keys: Iterable[str],
    select: SelectTree,
    *,
    reserved: bool = True,
) -> Iterator[
    tuple[
        str,
        SelectTree,
    ]
]:
    for key in keys:
        if reserved is False and key.startswith("_"):
            continue

        if select is None:
            sel = None
        elif "*" in select:
            sel = select["*"]
        elif key in select:
            sel = select[key]
        else:
            continue

        if sel is not None and sel == {}:
            sel = {"*": {}}

        yield key, sel


# FIXME: We should check select list at the very beginning of
#        request, not when returning results.
def check_unknown_props(
    node: Model | Property | DataType,
    select: Iterable[str] | None,
    known: Iterable[str],
):
    unknown_properties = set(select or []) - set(known) - {"*"}
    if unknown_properties:
        raise exceptions.MultipleErrors(
            exceptions.FieldNotInResource(node, property=prop) for prop in sorted(unknown_properties)
        )


def flat_select_to_nested(select: list[str] | None) -> SelectTree:
    """
    >>> flat_select_to_nested(None)

    >>> flat_select_to_nested(['foo.bar'])
    {'foo': {'bar': {}}}

    """
    if select is None:
        return None

    res = {}
    for v in select:
        if isinstance(v, dict):
            v = spyna.unparse(v)
        names = v.split(".")
        vref = res
        for name in names:
            if name not in vref:
                vref[name] = {}
            vref = vref[name]

    return res


def get_model_reserved_props(action: Action, include_page: bool) -> list[str]:
    if action == Action.GETALL:
        reserved = ["_type", "_id", "_revision"]
    elif action == Action.SEARCH:
        reserved = ["_type", "_id", "_revision", "_base"]
    elif action == Action.CHANGES:
        return ["_cid", "_created", "_op", "_id", "_txn", "_revision", "_same_as"]
    elif action == Action.MOVE:
        return ["_type", "_revision", "_id", "_same_as"]
    else:
        reserved = ["_type", "_id", "_revision"]
    if include_page:
        reserved.append("_page")
    return reserved


def get_ns_reserved_props(action: Action) -> list[str]:
    return []


@dataclasses.dataclass
class TableIdentifier:
    """
    Represents a table identifier across logical (app) and PostgreSQL layers.

    It builds derived names used for internal logic and SQL queries, including
    schema-qualified and escaped identifiers.

    Attributes:
        schema (str | None): Logical schema/namespace (e.g. "datasets/gov/rc").
        base_name (str): Base table name (e.g. "Building").
        table_type (TableType): Table type suffix (default: TableType.MAIN).
        table_arg (str | None): Optional argument appended used for table types that require property.
        default_pg_schema (str | None): Fallback PG schema if schema is not given.

        logical_name (str): Computed name (base + type + optional arg).
            Example: "Building/:list/apartments"
        logical_qualified_name (str): Logical name with schema (dataset).
            Example: "datasets/gov/rc/Building/:list/apartments"

        pg_table_name (str): PG-safe (compressed) table name from logical_name.
        pg_schema_name (str | None): PG-safe (compressed) schema name.
        pg_qualified_name (str): PG-safe (compressed) schema with table name.
            Example: "datasets/gov/rc.Building/:list/apartments" (unescaped).
        pg_escaped_qualified_name (str): Quoted version of pg_qualified_name, used for queries.
            Example: '"datasets/gov/rc"."Building/:list/apartments"' (escaped).

    Example:
        >>> TableIdentifier("datasets/gov/rc", "Buildings", TableType.LIST, "apartments")
        # logical_qualified_name: "datasets/gov/rc/Buildings/:list/apartments"
        # pg_qualified_name: "datasets/gov/rc.Building/:list/apartments"

        >>> TableIdentifier("datasets/gov/rc", "Buildings")
        # logical_qualified_name: "datasets/gov/rc/Buildings"
        # pg_qualified_name: "datasets/gov/rc.Building"

        >>> TableIdentifier("datasets/gov/rc/very/long/dataset/name/that/does/not/fit/withing/limits", "Buildings")
        # logical_qualified_name: "datasets/gov/rc/very/long/dataset/name/that/does/not/fit/withing/limits/Buildings"
        # pg_qualified_name: "datasets/gov/rc/very/long/dataset/nam_e5985b69_t/withing/limits.Building"
    """

    schema: str | None
    base_name: str
    table_type: TableType = dataclasses.field(default=TableType.MAIN)
    table_arg: str | None = dataclasses.field(default=None)
    default_pg_schema: str | None = dataclasses.field(default=None)

    logical_name: str = dataclasses.field(init=False)
    # Name with namespace connected with '/', like it is used with Model class
    logical_qualified_name: str = dataclasses.field(init=False)

    pg_table_name: str = dataclasses.field(init=False)
    pg_schema_name: str | None = dataclasses.field(init=False)
    # Used for hashed schema and table names
    pg_qualified_name: str = dataclasses.field(init=False)
    # Escaped qualified name, used for queries
    pg_escaped_qualified_name: str = dataclasses.field(init=False)

    def __post_init__(self):
        self.logical_name = self.base_name + self.table_type.value
        if self.table_arg:
            self.logical_name += "/" + self.table_arg

        self.logical_qualified_name = f"{self.schema}/{self.logical_name}" if self.schema else self.logical_name

        self.pg_table_name = get_pg_name(self.logical_name)
        self.pg_schema_name = get_pg_name(self.schema) if self.schema else self.default_pg_schema
        self.pg_qualified_name = (
            f"{self.pg_schema_name}.{self.pg_table_name}" if self.pg_schema_name else self.pg_table_name
        )
        self.pg_escaped_qualified_name = (
            f"{pg_identifier_preparer.quote(self.pg_schema_name)}.{pg_identifier_preparer.quote(self.pg_table_name)}"
            if self.pg_schema_name
            else pg_identifier_preparer.quote(self.pg_table_name)
        )

    def change_table_type(self, new_type: TableType, table_arg: str | None = None) -> "TableIdentifier":
        return dataclasses.replace(self, table_type=new_type, table_arg=table_arg)

    def apply_removed_prefix(self, remove_model_only: bool = False) -> "TableIdentifier":
        if remove_model_only or not self.table_arg:
            if not self.base_name.startswith("__"):
                return dataclasses.replace(self, base_name=f"__{self.base_name}")
            return self

        if not self.table_arg.startswith("__"):
            return dataclasses.replace(self, table_arg=f"__{self.table_arg}")
        return self


@dispatch(str)
def get_table_identifier(item: str, **kwargs) -> TableIdentifier:
    schema, model_name, table_type, table_arg = split_logical_name(item)
    return TableIdentifier(schema=schema, base_name=model_name, table_type=table_type, table_arg=table_arg, **kwargs)


@dispatch(sa.Table)
def get_table_identifier(item: sa.Table, **kwargs) -> TableIdentifier:
    if not item.comment:
        return TableIdentifier(schema=item.schema, base_name=item.name, **kwargs)
    if item.schema not in ("public", None):
        return get_table_identifier(item.comment, **kwargs)

    schema, model_name, table_type, table_arg = split_logical_name(item.comment)
    return TableIdentifier(
        schema=None,
        base_name=f"{schema}/{model_name}" if schema else model_name,
        table_type=table_type,
        table_arg=table_arg,
        **kwargs,
    )


@dispatch((Model, Property))
def get_table_identifier(node: Model | Property, **kwargs) -> TableIdentifier:
    return get_table_identifier(node, TableType.MAIN, **kwargs)


@dispatch((Model, Property), TableType)
def get_table_identifier(
    node: Model | Property, table_type: TableType, table_arg: str = None, **kwargs
) -> TableIdentifier:
    model = node if isinstance(node, Model) else node.model

    schema = model.ns.name if model.ns else None
    base_name = model.get_name_without_ns()

    if isinstance(node, Property) and table_type in (TableType.LIST, TableType.FILE):
        table_arg = node.place

    return TableIdentifier(schema, base_name, table_type, table_arg, **kwargs)


def get_table_name(
    node: Model | Property,
    ttype: TableType = TableType.MAIN,
) -> str:
    if isinstance(node, Model):
        model = node
    else:
        model = node.model
    if ttype in (TableType.LIST, TableType.FILE):
        name = model.model_type() + ttype.value + "/" + node.place
    else:
        name = model.model_type() + ttype.value
    return name


def split_table_name(full_name: str) -> tuple[str | None, str]:
    parts = full_name.split(".", maxsplit=1)
    if len(parts) == 1:
        return None, parts[0]
    return parts[0], parts[1]


def split_logical_name(full_name: str) -> tuple[str | None, str, TableType, str | None]:
    base_name, table_type, property_name = extract_table_data_from_logical_name(full_name)
    parts = base_name.split("/")
    if len(parts) == 1:
        return None, parts[0], table_type, property_name

    for i, part in enumerate(parts):
        if part[0].isupper() or (part[:2] == "__" and part[2].isupper()):
            namespace = "/".join(parts[:i])
            model = "/".join(parts[i:])
            return namespace, model, table_type, property_name
    return None, base_name, table_type, property_name


def load_query_builder_class(config: Config, backend: Backend):
    if backend.query_builder_type is None:
        return

    backend.query_builder_class = config.components.get("querybuilders")[backend.query_builder_type]


def load_result_builder_class(config: Config, backend: Backend):
    if backend.result_builder_type is None:
        return

    backend.result_builder_class = config.components.get("resultbuilders")[backend.result_builder_type]


def prepare_response(
    context: Context,
    data: DataItem,
) -> (DataItem, dict):
    if data.action == Action.UPDATE:
        # Minor optimisation: if we querying subresource, then build
        # response only for the subresource tree, do not walk through
        # whole model property tree.
        if data.prop:
            dtype = data.prop.dtype
            patch = data.patch.get(data.prop.name, {})
            saved = data.saved.get(data.prop.name, {})
        else:
            dtype = data.model
            patch = take(data.patch)
            saved = take(data.saved)

        resp = build_full_response(
            context,
            dtype,
            patch=patch,
            saved=saved,
        )

        # When querying subresources, response still must be enclosed with
        # the subresource key.
        if data.prop:
            resp = {
                data.prop.name: resp,
            }
    elif data.patch:
        resp = data.patch
    else:
        resp = {}
    return resp


def extract_table_data_from_logical_name(table_name: str) -> tuple[str | None, TableType | None, str | None]:
    """
    Extracts the main table name, table type, and an optional property suffix from a logical
    table name string. It parses the given logical table name and determines whether it belongs
    to the main table or some specific table type. If a specific type is found, it splits the
    table name into its components.

    Parameters:
        table_name (str): The logical table name string that needs to be processed.

    Returns:
        tuple: A tuple containing:
            - str | None: The main table name.
            - TableType | None: The type of the table, which can be `MAIN` or other enum members of
              `TableType`.
            - str | None: A property suffix string if present in the logical table name, or
              None otherwise.
    """
    if "/:" not in table_name:
        return table_name, TableType.MAIN, None

    for table_type in TableType:
        if table_type is TableType.MAIN:
            continue

        if table_type.value in table_name:
            data = table_name.split(table_type.value, 1)
            if data[1]:
                return data[0], table_type, data[1][1:]  # skip /property slash
            return data[0], table_type, None

    return None, None, None


def is_custom_id_prop(prop: Property) -> bool:
    return prop.name == "_id" and not isinstance(prop.dtype, PrimaryKey)


def is_accessible_by_equals_sign(id_prop: Property, value: str | int) -> bool:
    if isinstance(id_prop.dtype, Base32):
        return True

    if isinstance(id_prop.dtype, String):
        return not check_if_model_primary_key_is_composite(id_prop.model)

    return False


def check_if_model_primary_key_is_composite(model: Model) -> bool:
    pkeys_count = len(model.external.pkeys)
    if pkeys_count > 1:
        return True
    return False
