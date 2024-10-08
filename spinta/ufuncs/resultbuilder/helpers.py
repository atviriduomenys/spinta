from collections import abc

from multipledispatch import dispatch
from typing import Any, Union

from spinta import commands
from spinta.backends import Backend
from spinta.components import Context, Property
from spinta.core.ufuncs import Expr
from spinta.dimensions.enum.helpers import get_prop_enum
from spinta.exceptions import ValueNotInEnum, PropertyNotFound
from spinta.ufuncs.basequerybuilder.components import Selected
from spinta.ufuncs.resultbuilder.components import ResultBuilder
from spinta.utils.schema import NA

ResultBuilderGetter = abc.Callable[[], ResultBuilder]


def _resolve_expr(context: Context, row: Any, sel: Selected, result_builder_getter: Union[ResultBuilderGetter, ResultBuilder]) -> Any:
    if sel.item is None:
        val = None
    else:
        val = row[sel.item]

    result_builder = result_builder_getter
    if isinstance(result_builder, abc.Callable):
        result_builder = result_builder()
    env = result_builder.init(val, sel.prop, row)
    return env.resolve(sel.prep)


def _aggregate_values(data: Any, target: Property):
    key_path = target.place
    key_parts = key_path.split('.')

    # Drop first part, since if nested prop is part of the list will always be first value
    # ex: from DB we get {"notes": [{"note": 0}]}
    # but after fetching the value we only get [{"note": 0}]
    # so if our place is "notes.note", we need to drop "notes" part
    if len(key_parts) > 1:
        key_parts = key_parts[1:]

    def recursive_collect(sub_data, depth=0):
        if depth < len(key_parts):
            if isinstance(sub_data, list):
                collected = []
                for item in sub_data:
                    collected.extend(recursive_collect(item, depth))
                return collected
            elif isinstance(sub_data, dict) and key_parts[depth] in sub_data:
                return recursive_collect(sub_data[key_parts[depth]], depth + 1)
        else:
            return [sub_data]

        return []

    # Start the recursive collection process
    return recursive_collect(data, 0)


# This is the fastest way to call this function, use this if you do not want to recreate `ResultBuilder`
@dispatch(Context, ResultBuilder, object, object)
def get_row_value(context: Context, result_builder: ResultBuilder, row: Any, sel: Any) -> Any:
    return _get_row_value(
        context,
        row,
        sel,
        True,
        result_builder
    )


# This is the fastest way to call this function, use this if you do not want to recreate `ResultBuilder`
@dispatch(Context, ResultBuilder, object, object, bool)
def get_row_value(context: Context, result_builder: ResultBuilder, row: Any, sel: Any, check_enums: bool) -> Any:
    return _get_row_value(
        context,
        row,
        sel,
        check_enums,
        result_builder
    )


# This is the second-fastest way to call this function, it allows for `ResultBuilder` recreation
@dispatch(Context, abc.Callable, object, object, bool)
def get_row_value(context: Context, result_builder_getter: ResultBuilderGetter, row: Any, sel: Any, check_enums: bool) -> Any:
    return _get_row_value(
        context,
        row,
        sel,
        check_enums,
        result_builder_getter
    )


# This is the second-fastest way to call this function, it allows for `ResultBuilder` recreation
@dispatch(Context, abc.Callable, object, object)
def get_row_value(context: Context, result_builder_getter: ResultBuilderGetter, row: Any, sel: Any) -> Any:
    return _get_row_value(
        context,
        row,
        sel,
        True,
        result_builder_getter
    )


# This is the slowest way to call this function, use this only if you cannot pass `backend_result_builder_getter` directly
@dispatch(Context, Backend, object, object)
def get_row_value(context: Context, backend: Backend, row: Any, sel: Any) -> Any:
    return _get_row_value(
        context,
        row,
        sel,
        True,
        backend_result_builder_getter(context, backend)
    )


# This is the slowest way to call this function, use this only if you cannot pass `backend_result_builder_getter` directly
@dispatch(Context, Backend, object, object, bool)
def get_row_value(context: Context, backend: Backend, row: Any, sel: Any, check_enums: bool) -> Any:
    return _get_row_value(
        context,
        row,
        sel,
        check_enums,
        backend_result_builder_getter(context, backend)
    )


def backend_result_builder_getter(context: Context, backend: Backend) -> ResultBuilderGetter:
    return lambda: commands.get_result_builder(context, backend)


def _get_row_value(
    context: Context,
    row: Any,
    sel: Any,
    check_enums: bool,
    result_builder_getter: Union[ResultBuilderGetter, ResultBuilder]
):
    if isinstance(sel, Selected):
        if isinstance(sel.prep, Expr):
            val = _resolve_expr(context, row, sel, result_builder_getter)
        elif sel.prep is not NA:
            val = _get_row_value(context, row, sel.prep, check_enums, result_builder_getter)
        else:
            if sel.item is not None:
                val = row[sel.item]
                prop = sel.prop
                if prop is not None and prop.list is not None:
                    val = _aggregate_values(val, prop)
            else:
                val = None

        if check_enums:
            if enum := get_prop_enum(sel.prop):
                if val is None:
                    pass
                elif str(val) in enum:
                    item = enum[str(val)]
                    if item.prepare is not NA:
                        val = item.prepare
                else:
                    raise ValueNotInEnum(sel.prop, value=val)

        return val
    if isinstance(sel, tuple):
        return tuple(_get_row_value(context, row, v, check_enums, result_builder_getter) for v in sel)
    if isinstance(sel, list):
        return [_get_row_value(context, row, v, check_enums, result_builder_getter) for v in sel]
    if isinstance(sel, dict):
        return {k: _get_row_value(context, row, v, check_enums, result_builder_getter) for k, v in sel.items()}
    return sel
