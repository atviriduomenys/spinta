from __future__ import annotations

from typing import Any
from typing import List
from typing import Tuple
from typing import TypeVar
from typing import Union
from typing import overload

import sqlalchemy as sa
from sqlalchemy.sql.functions import Function

from spinta.auth import authorized
from spinta.components import Page
from spinta.core.enums import Action
from spinta.components import Property
from spinta.core.ufuncs import Bind, GetAttr
from spinta.core.ufuncs import Expr
from spinta.core.ufuncs import Negative
from spinta.core.ufuncs import Unresolved
from spinta.core.ufuncs import ufunc
from spinta.datasets.backends.sql.ufuncs.query.components import SqlQueryBuilder
from spinta.datasets.backends.sql.ufuncs.query.helpers import (
    select_ref_foreign_key_properties,
    select_external_ref_foreign_key_properties,
)
from spinta.dimensions.enum.helpers import prepare_enum_value
from spinta.exceptions import PropertyNotFound, SourceCannotBeList, NoExternalName, NotImplementedFeature
from spinta.types.datatype import DataType, Denorm, Object, Array, ExternalRef
from spinta.types.datatype import PrimaryKey
from spinta.types.datatype import Ref
from spinta.types.datatype import String
from spinta.types.datatype import UUID
from spinta.types.text.components import Text
from spinta.types.text.helpers import determine_language_property_for_text
from spinta.ufuncs.components import ForeignProperty
from spinta.ufuncs.querybuilder.components import LiteralProperty, Selected, ReservedProperty
from spinta.ufuncs.querybuilder.helpers import get_language_column, process_literal_value
from spinta.ufuncs.querybuilder.ufuncs import Star
from spinta.utils.data import take
from spinta.utils.itertools import flatten
from spinta.utils.schema import NA


def _gather_selected_properties(env: SqlQueryBuilder):
    result = []
    if env.selected:
        for selected in env.selected.values():
            if selected and selected.prop:
                result.append(selected.prop)
    return result


@ufunc.resolver(SqlQueryBuilder, str, object, names=["eq", "ne"])
def eq(env: SqlQueryBuilder, op: str, field: str, value: Any):
    # XXX: Backwards compatible resolver, `str` arguments are deprecated.
    return env.call(op, Bind(field), value)


def _sa_compare(op: str, column: sa.Column, value: Any):
    if op == "eq":
        return column == value

    if op == "ne":
        return column != value

    if op == "lt":
        return column < value

    if op == "le":
        return column <= value

    if op == "gt":
        return column > value

    if op == "ge":
        return column >= value

    if op == "contains":
        return column.contains(value)

    if op == "startswith":
        return column.startswith(value)

    raise NotImplementedError


COMPARE = [
    "eq",
    "ne",
    "lt",
    "le",
    "gt",
    "ge",
    "startswith",
    "contains",
]


T = TypeVar("T")


def _prepare_value(prop: Property, value: T) -> Union[T, List[T]]:
    return prepare_enum_value(prop, value)


@ufunc.resolver(SqlQueryBuilder, Bind, object, names=COMPARE)
def compare(env: SqlQueryBuilder, op: str, field: Bind, value: Any):
    prop = env.resolve_property(field)
    value = _prepare_value(prop, value)
    return env.call(op, prop.dtype, value)


@ufunc.resolver(SqlQueryBuilder, GetAttr, object, names=COMPARE)
def compare(env: SqlQueryBuilder, op: str, attr: GetAttr, value: Any):
    resolved = env.resolve_property(attr)
    return env.call(op, resolved, value)


@ufunc.resolver(SqlQueryBuilder, Bind, list, names=COMPARE)
def compare(env: SqlQueryBuilder, op: str, field: Bind, value: List[Any]):
    prop = env.resolve_property(field)
    value = list(flatten(_prepare_value(prop, v) for v in value))
    return env.call(op, prop.dtype, value)


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, object, names=COMPARE)
def compare(env: SqlQueryBuilder, op: str, fpr: ForeignProperty, value: Any):
    value = _prepare_value(fpr.right.prop, value)
    return env.call(op, fpr, fpr.right, value)


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, list, names=COMPARE)
def compare(
    env: SqlQueryBuilder,
    op: str,
    fpr: ForeignProperty,
    value: List[Any],
):
    value = list(flatten(_prepare_value(fpr.right.prop, v) for v in value))
    return env.call(op, fpr, fpr.right, value)


@ufunc.resolver(SqlQueryBuilder, DataType, object, names=COMPARE)
def compare(env: SqlQueryBuilder, op: str, dtype: DataType, value: Any):
    column = env.backend.get_column(env.table, dtype.prop)
    return _sa_compare(op, column, value)


@ufunc.resolver(SqlQueryBuilder, UUID, str, names=COMPARE)
def compare(env: SqlQueryBuilder, op: str, dtype: DataType, value: Any):
    column = env.backend.get_column(env.table, dtype.prop).cast(sa.String)
    return _sa_compare(op, column, value)


@ufunc.resolver(SqlQueryBuilder, Text, object, names=COMPARE)
def compare(env: SqlQueryBuilder, op: str, dtype: Text, value: Any):
    column = get_language_column(env, dtype)
    return _sa_compare(op, column, value)


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, DataType, object, names=COMPARE)
def compare(
    env: SqlQueryBuilder,
    op: str,
    fpr: ForeignProperty,
    dtype: DataType,
    value: Any,
):
    table = env.joins.get_table(env, fpr)
    column = env.backend.get_column(table, dtype.prop)
    return _sa_compare(op, column, value)


@ufunc.resolver(SqlQueryBuilder, Function, object, names=COMPARE)
def compare(env: SqlQueryBuilder, op: str, func: Function, value: Any):
    return _sa_compare(op, func, value)


@ufunc.resolver(SqlQueryBuilder, DataType, list)
def eq(env: SqlQueryBuilder, dtype: DataType, value: List[Any]):
    column = env.backend.get_column(env.table, dtype.prop)
    return column.in_(value)


@ufunc.resolver(SqlQueryBuilder, Text, list)
def eq(env: SqlQueryBuilder, dtype: Text, value: List[Any]):
    column = get_language_column(env, dtype)
    return column.in_(value)


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, DataType, list)
def eq(env: SqlQueryBuilder, fpr: ForeignProperty, dtype: DataType, value: list):
    table = env.joins.get_table(env, fpr)
    column = env.backend.get_column(table, dtype.prop)
    return column.in_(value)


@ufunc.resolver(SqlQueryBuilder, DataType, list)
def ne(env: SqlQueryBuilder, dtype: DataType, value: List[Any]):
    column = env.backend.get_column(env.table, dtype.prop)
    return ~column.in_(value)


@ufunc.resolver(SqlQueryBuilder, Text, list)
def ne(env: SqlQueryBuilder, dtype: Text, value: List[Any]):
    column = get_language_column(env, dtype)
    return ~column.in_(value)


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, DataType, list)
def ne(
    env: SqlQueryBuilder,
    fpr: ForeignProperty,
    dtype: DataType,
    value: List[Any],
):
    table = env.joins.get_table(env, fpr)
    column = env.backend.get_column(table, fpr.right.prop)
    return ~column.in_(value)


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, list)
def ne(env: SqlQueryBuilder, fpr: ForeignProperty, value: List[Any]):
    table = env.joins.get_table(env, fpr)
    column = env.backend.get_column(table, fpr.right.prop)
    return ~column.in_(value)


@ufunc.resolver(SqlQueryBuilder, object)
def _resolve_unresolved(env: SqlQueryBuilder, value: Any) -> Any:
    if isinstance(value, Unresolved):
        raise ValueError(f"Unresolved value {value!r}.")
    else:
        return value


@ufunc.resolver(SqlQueryBuilder, Property)
def _resolve_unresolved(env: SqlQueryBuilder, prop: Property) -> sa.Column:
    if prop.external is None:
        raise NoExternalName(prop)

    if prop.external.name:
        return env.backend.get_column(env.table, prop)

    if prop.external.prepare and isinstance(prop.external.prepare, Expr):
        result = env(this=prop).resolve(prop.external.prepare)
        columns = env.call("_resolve_unresolved", result)
        return columns

    raise NoExternalName(prop)


@ufunc.resolver(SqlQueryBuilder, Bind)
def _resolve_unresolved(env: SqlQueryBuilder, field: Bind) -> sa.Column:
    prop = env.resolve_property(field)
    return env.call("_resolve_unresolved", prop)


@ufunc.resolver(SqlQueryBuilder, GetAttr)
def _resolve_unresolved(env: SqlQueryBuilder, attr: GetAttr) -> sa.Column:
    fpr = env.resolve_property(attr)
    table = env.joins.get_table(env, fpr)
    dtype = fpr.right
    return env.backend.get_column(table, dtype.prop)


@ufunc.resolver(SqlQueryBuilder, list)
def _resolve_unresolved(env: SqlQueryBuilder, data: list) -> list:
    return list(env.call("_resolve_unresolved", value) for value in data)


@ufunc.resolver(SqlQueryBuilder, tuple)
def _resolve_unresolved(env: SqlQueryBuilder, data: tuple) -> tuple:
    return tuple(env.call("_resolve_unresolved", value) for value in data)


@ufunc.resolver(SqlQueryBuilder, Expr, name="and")
def and_(env: SqlQueryBuilder, expr: Expr):
    args, kwargs = expr.resolve(env)
    args = [env.call("_resolve_unresolved", arg) for arg in args if arg is not None]
    if len(args) > 1:
        return sa.and_(*args)
    elif args:
        return args[0]


@ufunc.resolver(SqlQueryBuilder, Expr, name="or")
def or_(env: SqlQueryBuilder, expr: Expr):
    args, kwargs = expr.resolve(env)
    args = [env.call("_resolve_unresolved", arg) for arg in args if arg is not None]
    if len(args) > 1:
        return sa.or_(*args)
    elif args:
        return args[0]


@ufunc.resolver(SqlQueryBuilder, Expr, name="list")
def list_(env: SqlQueryBuilder, expr: Expr) -> List[Any]:
    args, kwargs = expr.resolve(env)
    return list(args)


@ufunc.resolver(SqlQueryBuilder)
def count(env: SqlQueryBuilder):
    return sa.func.count()


def _get_property_for_select(
    env: SqlQueryBuilder,
    name: str,
    *,
    nested: bool = False,
) -> Property:
    # TODO: `name` can refer to (in specified order):
    #       - var - a defined variable
    #       - param - a parameter if parametrization is used
    #       - item - an item of a dict or list
    #       - prop - a property
    #       Currently only `prop` is resolved.
    prop = env.resolve_property(name)
    if (
        # Check authorization only for top level properties in select list.
        # XXX: Not sure if nested is the right property to user, probably better
        #      option is to check if this call comes from a prepare context. But
        #      then how prepare context should be defined? Probably resolvers
        #      should be called with a different env class?
        #      tag:resolving_private_properties_in_prepare_context
        nested or authorized(env.context, prop, Action.SEARCH)
    ):
        return prop
    else:
        raise PropertyNotFound(env.model, property=name)


@ufunc.resolver(SqlQueryBuilder, Expr)
def select(env: SqlQueryBuilder, expr: Expr):
    keys = [str(k) for k in expr.args]
    args, kwargs = expr.resolve(env)
    args = list(zip(keys, args)) + list(kwargs.items())
    if env.selected is not None:
        raise RuntimeError("`select` was already called.")
    env.selected = {}
    if args:
        for key, arg in args:
            selected = env.call("select", arg)
            if selected is not None:
                env.selected[key] = selected
    else:
        for prop in take(["_id", all], env.model.properties).values():
            if authorized(env.context, prop, Action.GETALL):
                processed = env.call("select", prop)
                if not prop.dtype.inherited or processed.prep is not None:
                    env.selected[prop.place] = processed

    if not (len(args) == 1 and args[0][0] == "_page"):
        if not env.columns:
            raise RuntimeError(f"{expr} didn't added anything to select list.")


@ufunc.resolver(SqlQueryBuilder, sa.sql.expression.ColumnElement)
def select(env, column):
    return Selected(env.add_column(column))


@ufunc.resolver(SqlQueryBuilder, Bind)
def select(env: SqlQueryBuilder, item: Bind, *, nested: bool = False):
    if item.name == "_page":
        return None
    prop = _get_property_for_select(env, item.name, nested=nested)
    return env.call("select", prop)


@ufunc.resolver(SqlQueryBuilder, str)
def select(env: SqlQueryBuilder, item: str, *, nested: bool = False):
    # XXX: Backwards compatible resolver, `str` arguments are deprecated.
    prop = _get_property_for_select(env, item, nested=nested)
    return env.call("select", prop)


@ufunc.resolver(SqlQueryBuilder, Property)
def select(env: SqlQueryBuilder, prop: Property) -> Selected:
    if prop.place not in env.resolved:
        if isinstance(prop.external, list):
            raise SourceCannotBeList(prop)
        if prop.external.prepare is not NA:
            # If `prepare` formula is given, evaluate formula.
            if isinstance(prop.external.prepare, Expr):
                result = env(this=prop).resolve(prop.external.prepare)
            else:
                result = process_literal_value(prop.external.prepare)

            # XXX: Maybe interpretation of prepare formula should be done under
            #      a different Env? This way, select resolvers would know when
            #      properties are resolved under a formula context and for
            #      example would be able to decide to not check permissions on
            #      properties.
            #      tag:resolving_private_properties_in_prepare_context
            result = env.call("select", prop.dtype, result)
        elif prop.external and prop.external.name:
            # If prepare is not given, then take value from `source`.
            result = env.call("select", prop.dtype)
        elif prop.is_reserved():
            # Reserved properties never have external source.
            result = env.call("select", prop.dtype)
        elif not prop.dtype.requires_source:
            # Some DataTypes might have children that have source instead of themselves, like: Text, Object
            result = env.call("select", prop.dtype)
        elif prop.dtype.inherited:
            # Some DataTypes might be inherited, or hidden, so we need to go through them in case they can be joined
            result = env.call("select", prop.dtype)
            if not isinstance(result, Selected):
                result = Selected(prop=prop, prep=None)
        else:
            # If `source` is not given, return None.
            result = Selected(prop=prop, prep=None)
        assert isinstance(result, Selected), prop
        env.resolved[prop.place] = result
    return env.resolved[prop.place]


@ufunc.resolver(SqlQueryBuilder, DataType)
def select(env: SqlQueryBuilder, dtype: DataType) -> Selected:
    table = env.backend.get_table(env.model)
    column = env.backend.get_column(table, dtype.prop, select=True)
    return Selected(
        item=env.add_column(column),
        prop=dtype.prop,
    )


@ufunc.resolver(SqlQueryBuilder, Object)
def select(env: SqlQueryBuilder, dtype: Object) -> Selected:
    prep = {}
    for prop in take(dtype.properties).values():
        sel = env.call("select", prop)
        if sel is not None:
            prep[prop.name] = sel
    return Selected(prop=dtype.prop, prep=prep)


@ufunc.resolver(SqlQueryBuilder, Ref)
def select(env: SqlQueryBuilder, dtype: Ref) -> Selected:
    prep = {}
    if not dtype.inherited:
        prep["_id"] = Selected(prop=dtype.prop, prep=select_ref_foreign_key_properties(env, dtype))

    for prop in dtype.properties.values():
        sel = env.call("select", prop)
        prep[prop.name] = sel

    return Selected(prop=dtype.prop, prep=prep)


@ufunc.resolver(SqlQueryBuilder, ExternalRef)
def select(env: SqlQueryBuilder, dtype: ExternalRef) -> Selected:
    prep = {}
    if not dtype.inherited:
        prep.update(select_external_ref_foreign_key_properties(env, dtype))

    for prop in dtype.properties.values():
        sel = env.call("select", prop)
        prep[prop.name] = sel

    return Selected(prop=dtype.prop, prep=prep)


@ufunc.resolver(SqlQueryBuilder, Ref, ReservedProperty)
def select(env: SqlQueryBuilder, dtype: Ref, reserved: ReservedProperty):
    return Selected(prop=dtype.prop, prep=select_ref_foreign_key_properties(env, dtype))


@ufunc.resolver(SqlQueryBuilder, ExternalRef, ReservedProperty)
def select(env: SqlQueryBuilder, dtype: ExternalRef, reserved: ReservedProperty):
    prep = select_external_ref_foreign_key_properties(env, dtype, target=reserved.param)
    return prep[reserved.param]


@ufunc.resolver(SqlQueryBuilder, Ref, str)
def select(env, dtype: Ref, prop: str):
    table = env.backend.get_table(env.model)
    return Selected(env.add_column(table.c[dtype.prop.place + "." + prop]))


@ufunc.resolver(SqlQueryBuilder, Ref, (list, tuple))
def select(env: SqlQueryBuilder, dtype: Ref, data: list | tuple) -> Selected:
    prep = {}
    if not dtype.inherited:
        prep["_id"] = Selected(prop=dtype.prop, prep=select_ref_foreign_key_properties(env, dtype, properties=data))

    for prop in dtype.properties.values():
        sel = env.call("select", prop)
        prep[prop.name] = sel

    return Selected(prop=dtype.prop, prep=prep)


@ufunc.resolver(SqlQueryBuilder, ExternalRef, (list, tuple))
def select(env: SqlQueryBuilder, dtype: ExternalRef, data: list | tuple) -> Selected:
    prep = {}
    if not dtype.inherited:
        prep.update(select_external_ref_foreign_key_properties(env, dtype, properties=data))

    for prop in dtype.properties.values():
        sel = env.call("select", prop)
        prep[prop.name] = sel

    return Selected(prop=dtype.prop, prep=prep)


@ufunc.resolver(SqlQueryBuilder, String)
def select(env, dtype):
    env.call("validate_dtype_for_select", dtype, _gather_selected_properties(env))
    column = env.backend.get_column(env.table, dtype.prop)
    return Selected(env.add_column(column), dtype.prop)


@ufunc.resolver(SqlQueryBuilder, Text)
def select(env, dtype):
    env.call("validate_dtype_for_select", dtype, _gather_selected_properties(env))
    if env.query_params.push:
        result = {key: env.call("select", prop) for key, prop in dtype.langs.items()}
        return Selected(prop=dtype.prop, prep=result)

    if env.query_params.lang:
        for lang in env.query_params.lang:
            if isinstance(lang, Star):
                result = {key: env.call("select", prop) for key, prop in dtype.langs.items()}
                return Selected(prop=dtype.prop, prep=result)
            break

        result = {key: env.call("select", prop) for key, prop in dtype.langs.items() if key in env.query_params.lang}
        return Selected(prop=dtype.prop, prep=result)

    default_langs = env.context.get("config").languages
    lang_prop = determine_language_property_for_text(dtype, env.query_params.lang_priority, default_langs)
    return Selected(prop=dtype.prop, prep={lang_prop.name: env.call("select", lang_prop)})


@ufunc.resolver(SqlQueryBuilder, Denorm)
def select(env, dtype: Denorm):
    fpr = env.call("_denorm_to_foreign_property", dtype)
    return env.call("select", fpr)


@ufunc.resolver(SqlQueryBuilder, Array)
def select(env: SqlQueryBuilder, dtype: Array):
    if dtype.model is not None:
        table = env.joins.get_intermediate_table(env, dtype)
        right = dtype.right_prop
        if right.external is None:
            raise NoExternalName(right)

        columns = env(table=table, model=dtype.model).call("_resolve_unresolved", right)
        column = env.call("_group_array", columns)

        # Group by all (required for aggregation functions)
        env.add_to_group_by(list(env.table.columns))
        return Selected(prop=dtype.prop, prep=Selected(item=env.add_column(column), prop=dtype.right_prop))

    if dtype.prop.external.name and dtype.prop.external.prepare:
        table = env.backend.get_table(dtype.prop.model)
        column = env.backend.get_column(table, dtype.prop)
        return Selected(item=env.add_column(column), prop=dtype.prop, prep=env.call("select", dtype.items))

    return Selected(prop=dtype.prop, prep=env.call("select", dtype.items))


@ufunc.resolver(SqlQueryBuilder, Page)
def select(env: SqlQueryBuilder, page: Page) -> List[sa.Column]:
    table = env.backend.get_table(env.model)
    return_selected = []
    for item in page.by.values():
        column = env.backend.get_column(table, item.prop, select=True)
        return_selected.append(column)
    return return_selected


@ufunc.resolver(SqlQueryBuilder, PrimaryKey)
def select(
    env: SqlQueryBuilder,
    dtype: PrimaryKey,
) -> Selected:
    model = dtype.prop.model
    pkeys = model.external.pkeys
    if not pkeys:
        # If primary key is not specified use all properties to uniquely
        # identify row.
        pkeys = take(model.properties).values()

    if len(pkeys) == 1:
        prop = pkeys[0]
        result = env.call("select", prop)
    else:
        result = [env.call("select", prop) for prop in pkeys]
    return Selected(prop=dtype.prop, prep=result)


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, DataType)
def select(
    env: SqlQueryBuilder,
    fpr: ForeignProperty,
    dtype: DataType,
) -> Selected:
    table = env.joins.get_table(env, fpr)
    column = env.backend.get_column(table, dtype.prop, select=True)
    return Selected(
        item=env.add_column(column),
        prop=dtype.prop,
    )


@ufunc.resolver(SqlQueryBuilder, Property)
def join_table_on(env: SqlQueryBuilder, prop: Property) -> Any:
    if prop.external.prepare is not NA:
        if isinstance(prop.external.prepare, Expr):
            result = env.resolve(prop.external.prepare)
        else:
            result = process_literal_value(prop.external.prepare)
        return env.call("join_table_on", prop.dtype, result)
    else:
        return env.call("join_table_on", prop.dtype)


@ufunc.resolver(SqlQueryBuilder, DataType)
def join_table_on(env: SqlQueryBuilder, dtype: DataType) -> Tuple[Any]:
    table = env.table
    if table is None:
        table = env.backend.get_table(dtype.prop.model)
    column = env.backend.get_column(table, dtype.prop)
    return column


@ufunc.resolver(SqlQueryBuilder, Text)
def join_table_on(env: SqlQueryBuilder, dtype: Text) -> Tuple[Any]:
    column = get_language_column(env, dtype)
    return column


@ufunc.resolver(SqlQueryBuilder, PrimaryKey)
def join_table_on(env: SqlQueryBuilder, dtype: DataType) -> Any:
    model = dtype.prop.model
    pkeys = model.external.pkeys

    if not pkeys:
        raise RuntimeError(f"Can't join {dtype.prop} on right table without primary key.")

    if len(pkeys) == 1:
        prop = pkeys[0]
        result = env.call("join_table_on", prop)
    else:
        result = [env.call("join_table_on", prop) for prop in pkeys]

    return result


@ufunc.resolver(SqlQueryBuilder, Ref)
def join_table_on(
    env: SqlQueryBuilder,
    dtype: Ref,
) -> Tuple[Any]:
    original_model = dtype.prop.model
    original_table = env.backend.get_table(original_model)

    prop_name = dtype.prop.name
    parent_ref = None
    if dtype.inherited:
        parent: Property = dtype.prop.parent
        parent_ref = parent.dtype.model.properties.get(prop_name)
    filtered_properties = {prop.name: prop for prop in dtype.properties.values() if not prop.dtype.inherited}
    ref_properties = {}
    # Gather overwritten properties
    # missing ref properties are None
    for prop in dtype.refprops:
        prop_name = prop.name
        value = filtered_properties.get(prop_name, None)
        if value is not None:
            value = env(table=original_table, model=original_model).call("join_table_on", value)
        ref_properties[prop_name] = value

    # Fill missing None values using parent reference (from which it was inherited from)
    if any(value is None for value in ref_properties.values()):
        if parent_ref is None:
            table = env.table
            column = env.backend.get_column(table, dtype.prop)
            return column

        parent_joined_columns = env(model=parent_ref.model).call("join_table_on", parent_ref)
        for i, (key, value) in enumerate(ref_properties.items()):
            if value is None:
                ref_properties[key] = parent_joined_columns[i]
    return tuple(value for value in ref_properties.values())


@ufunc.resolver(SqlQueryBuilder, DataType, tuple)
def join_table_on(
    env: SqlQueryBuilder,
    dtype: DataType,
    prep: tuple,
) -> Tuple[Any]:
    return tuple(env.call("join_table_on", v) for v in prep)


@ufunc.resolver(SqlQueryBuilder, Bind)
def join_table_on(env: SqlQueryBuilder, item: Bind):
    prop = env.resolve_property(item)
    if not authorized(env.context, prop, Action.SEARCH):
        raise PropertyNotFound(env.model, property=item.name)
    return env.call("join_table_on", prop)


@ufunc.resolver(SqlQueryBuilder, LiteralProperty)
def join_table_on(env: SqlQueryBuilder, item: LiteralProperty):
    return item.value


@ufunc.resolver(SqlQueryBuilder, DataType, LiteralProperty)
def join_table_on(env: SqlQueryBuilder, dtype: DataType, item: LiteralProperty):
    return env.call("join_table_on", item)


@ufunc.resolver(SqlQueryBuilder, Bind, name="len")
def len_(env: SqlQueryBuilder, bind: Bind):
    prop = env.resolve_property(bind)
    return env.call("len", prop.dtype)


@ufunc.resolver(SqlQueryBuilder, str, name="len")
def len_(env: SqlQueryBuilder, bind: str):
    # XXX: Backwards compatible resolver, `str` arguments are deprecated.
    prop = env.resolve_property(bind)
    return env.call("len", prop.dtype)


@ufunc.resolver(SqlQueryBuilder, DataType, name="len")
def len_(env: SqlQueryBuilder, dtype: DataType):
    column = env.backend.get_column(env.table, dtype.prop)
    return sa.func.length(column)


@ufunc.resolver(SqlQueryBuilder, Expr)
def sort(env: SqlQueryBuilder, expr: Expr):
    args, kwargs = expr.resolve(env)
    env.sort = []
    for key in args:
        result = env.call("sort", key)
        if isinstance(result, (list, set, tuple)):
            env.sort += result
        else:
            env.sort.append(result)


@ufunc.resolver(SqlQueryBuilder, DataType)
def sort(env, dtype):
    return env.call("asc", dtype)


@ufunc.resolver(SqlQueryBuilder, Bind)
def sort(env, field):
    prop = env.resolve_property(field)
    return env.call("asc", prop.dtype)


@ufunc.resolver(SqlQueryBuilder, Negative)
def sort(env, field):
    prop = env.resolve_property(field)
    return env.call("desc", prop.dtype)


@ufunc.resolver(SqlQueryBuilder, DataType)
def asc(env, dtype):
    column = env.backend.get_column(env.table, dtype.prop)
    return env.call("asc", column)


@ufunc.resolver(SqlQueryBuilder, Text)
def asc(env, dtype):
    column = get_language_column(env, dtype)
    return env.call("asc", column)


# Reason for column == None is NULLS LAST, because
# if it's NULL it will return 1 and if it's not NULL it will return 0
# when ordering 0 takes priority over 1, so then it will sort first values that are not NULL
@ufunc.resolver(SqlQueryBuilder, sa.sql.expression.ColumnElement)
def asc(env, column):
    return sa.case(
        [(column.is_(None), sa.literal_column("1", type_=sa.Integer))],
        else_=sa.literal_column("0", type_=sa.Integer),
    ), column.asc()


@ufunc.resolver(SqlQueryBuilder, DataType)
def desc(env, dtype):
    column = env.backend.get_column(env.table, dtype.prop)
    return env.call("desc", column)


@ufunc.resolver(SqlQueryBuilder, Text)
def desc(env, dtype):
    column = get_language_column(env, dtype)
    return env.call("desc", column)


# Reason for column != None is NULLS FIRST, because
# if it's NULL it will return 0 and if it's not NULL it will return 1
# when ordering 0 takes priority over 1, so then it will sort first values that are NULL
@ufunc.resolver(SqlQueryBuilder, sa.sql.expression.ColumnElement)
def desc(env, column):
    return sa.case(
        [(column.is_not(None), sa.literal_column("1", type_=sa.Integer))],
        else_=sa.literal_column("0", type_=sa.Integer),
    ), column.desc()


@ufunc.resolver(SqlQueryBuilder, DataType)
def negative(env: SqlQueryBuilder, dtype: DataType):
    return Negative(dtype.prop.place)


@ufunc.resolver(SqlQueryBuilder, GetAttr)
def negative(env: SqlQueryBuilder, attr: GetAttr):
    resolved = env.resolve_property(attr)
    return env.call("negative", resolved)


@ufunc.resolver(SqlQueryBuilder, String)
def negative(env: SqlQueryBuilder, dtype: String):
    if dtype.prop.parent and isinstance(dtype.prop.parent.dtype, Text):
        return Negative(dtype.prop.place.replace(".", "@"))
    return Negative(dtype.prop.place)


@ufunc.resolver(SqlQueryBuilder, int)
def limit(env: SqlQueryBuilder, n: int):
    env.limit = n


@ufunc.resolver(SqlQueryBuilder, int)
def offset(env: SqlQueryBuilder, n: int):
    env.offset = n


@ufunc.resolver(SqlQueryBuilder, Expr)
def file(env: SqlQueryBuilder, expr: Expr) -> Expr:
    args, kwargs = expr.resolve(env)
    return Expr(
        "file",
        name=env.call("select", kwargs["name"], nested=True),
        content=env.call("select", kwargs["content"], nested=True),
    )


@overload
@ufunc.resolver(SqlQueryBuilder)
def cast(env: SqlQueryBuilder) -> Expr:
    return Expr("cast")


@overload
@ufunc.resolver(SqlQueryBuilder, Bind, Bind)
def point(env: SqlQueryBuilder, x: Bind, y: Bind) -> Expr:
    return Expr(
        "point",
        env.call("select", x, nested=True),
        env.call("select", y, nested=True),
    )


@ufunc.resolver(SqlQueryBuilder)
def distinct(env: SqlQueryBuilder):
    if env.model and env.model.external and not env.model.external.unknown_primary_key:
        extracted_columns = [
            env.backend.get_column(env.table, prop) for prop in env.model.external.pkeys if prop.external
        ]
        env.add_to_group_by(extracted_columns)
    else:
        env.distinct = True


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, PrimaryKey)
def select(
    env: SqlQueryBuilder,
    fpr: ForeignProperty,
    dtype: PrimaryKey,
) -> Selected:
    super_ = ufunc.resolver[env, fpr, dtype]
    return super_(env, fpr, dtype)


@ufunc.resolver(SqlQueryBuilder, object)
def _group_array(env: SqlQueryBuilder, columns: Any):
    raise NotImplementedFeature(env.backend, feature="Ability to group arrays using default `sql` type")
