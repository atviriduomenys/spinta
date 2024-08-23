from multipledispatch import dispatch
from typing import Any

from spinta import commands
from spinta.backends import Backend
from spinta.components import Context, Property
from spinta.core.ufuncs import Expr
from spinta.dimensions.enum.helpers import get_prop_enum
from spinta.exceptions import ValueNotInEnum, PropertyNotFound
from spinta.ufuncs.basequerybuilder.components import Selected
from spinta.ufuncs.resultbuilder.components import ResultBuilder
from spinta.utils.schema import NA


def _resolve_expr(context: Context, result_builder: ResultBuilder, row: Any, sel: Selected) -> Any:
    if sel.item is None:
        val = None
    else:
        val = row[sel.item]
    env = result_builder.init(val, sel.prop, row)
    return env.resolve(sel.prep)


def _aggregate_values(data, target: Property):
    if target is None or target.list is None:
        return data

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


@dispatch(Context, Backend, object, object)
def get_row_value(context: Context, backend: Backend, row: Any, sel: Any) -> Any:
    return get_row_value(context, backend, row, sel, True)


@dispatch(Context, Backend, object, object, bool)
def get_row_value(context: Context, backend: Backend, row: Any, sel: Any, check_enums: bool) -> Any:
    env = commands.get_result_builder(context, backend)
    return get_row_value(context, env, row, sel, check_enums)


@dispatch(Context, ResultBuilder, object, object)
def get_row_value(context: Context, result_builder: ResultBuilder, row: Any, sel: Any) -> Any:
    return get_row_value(context, result_builder, row, sel, True)


@dispatch(Context, ResultBuilder, object, object, bool)
def get_row_value(context: Context, result_builder: ResultBuilder, row: Any, sel: Any, check_enums: bool) -> Any:
    if isinstance(sel, Selected):
        if isinstance(sel.prep, Expr):
            val = _resolve_expr(context, result_builder, row, sel)
        elif sel.prep is not NA:
            val = get_row_value(context, result_builder, row, sel.prep, check_enums)
        else:
            if sel.item is not None:
                val = row[sel.item]
                val = _aggregate_values(val, sel.prop)
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
        return tuple(get_row_value(context, result_builder, row, v, check_enums) for v in sel)
    if isinstance(sel, list):
        return [get_row_value(context, result_builder, row, v, check_enums) for v in sel]
    if isinstance(sel, dict):
        return {k: get_row_value(context, result_builder, row, v, check_enums) for k, v in sel.items()}
    return sel
