import logging
from typing import Any
from typing import Iterator

from sqlalchemy.engine.row import RowProxy

from spinta import commands
from spinta.components import Context, Property
from spinta.components import Model
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.helpers import handle_ref_key_assignment
from spinta.typing import ObjectData
from spinta.datasets.backends.sql.commands.query import Selected
from spinta.datasets.backends.sql.commands.query import SqlQueryBuilder
from spinta.datasets.backends.sql.components import Sql
from spinta.datasets.backends.sql.ufuncs.components import SqlResultBuilder
from spinta.datasets.helpers import get_enum_filters
from spinta.datasets.helpers import get_ref_filters
from spinta.datasets.keymaps.components import KeyMap
from spinta.datasets.utils import iterparams
from spinta.dimensions.enum.helpers import get_prop_enum
from spinta.exceptions import ValueNotInEnum
from spinta.types.datatype import PrimaryKey
from spinta.types.datatype import Ref
from spinta.ufuncs.helpers import merge_formulas
from spinta.utils.nestedstruct import flat_dicts_to_nested
from spinta.utils.schema import NA


log = logging.getLogger(__name__)


def _resolve_expr(context: Context, row: RowProxy, sel: Selected) -> Any:
    if sel.item is None:
        val = None
    else:
        val = row[sel.item]
    env = SqlResultBuilder(context).init(val, sel.prop, row)
    return env.resolve(sel.prep)


def _get_row_value(context: Context, row: RowProxy, sel: Any) -> Any:
    if isinstance(sel, Selected):
        if isinstance(sel.prep, Expr):
            val = _resolve_expr(context, row, sel)
        elif sel.prep is not NA:
            val = _get_row_value(context, row, sel.prep)
        else:
            val = row[sel.item]

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
        return tuple(_get_row_value(context, row, v) for v in sel)
    if isinstance(sel, list):
        return [_get_row_value(context, row, v) for v in sel]
    if isinstance(sel, dict):
        return {k: _get_row_value(context, row, v) for k, v in sel.items()}
    return sel


@commands.getall.register(Context, Model, Sql)
def getall(
    context: Context,
    model: Model,
    backend: Sql,
    *,
    query: Expr = None,
) -> Iterator[ObjectData]:
    conn = context.get(f'transaction.{backend.name}')
    builder = SqlQueryBuilder(context)
    builder.update(model=model)
    # Merge user passed query with query set in manifest.
    query = merge_formulas(model.external.prepare, query)
    query = merge_formulas(query, get_enum_filters(context, model))
    query = merge_formulas(query, get_ref_filters(context, model))
    keymap: KeyMap = context.get(f'keymap.{model.keymap.name}')
    for params in iterparams(context, model):
        table = model.external.name.format(**params)
        table = backend.get_table(model, table)
        env = builder.init(backend, table)
        env.update(params=params)
        expr = env.resolve(query)
        where = env.execute(expr)
        qry = env.build(where)
        for row in conn.execute(qry):
            res = {
                '_type': model.model_type(),
            }
            pk = None
            for key, sel in env.selected.items():
                val = _get_row_value(context, row, sel)
                if sel.prop:
                    if isinstance(sel.prop.dtype, PrimaryKey):
                        pk = _generate_pk_for_row(model, row, keymap, val)
                        val = pk
                    elif isinstance(sel.prop.dtype, Ref):
                        val = handle_ref_key_assignment(keymap, val, sel.prop, pk)
                res[key] = val
            res = flat_dicts_to_nested(res)
            res = commands.cast_backend_to_python(context, model, backend, res)
            yield res


def _generate_pk_for_row(model: Model, row: dict, keymap, pk_val: Any):
    pk = None
    if model.base:
        pk_val_base = _extract_values_from_row(row, model.base.pk)
        joined = '_'.join(pk.name for pk in model.base.pk)
        key = f'{model.base.parent.model_type()}.{joined}'
        pk = keymap.encode(key, pk_val_base)

    pk = keymap.encode(model.model_type(), pk_val, pk)
    if pk and model.required_keymap_properties:
        for combination in model.required_keymap_properties:
            joined = '_'.join(combination)
            key = f'{model.model_type()}.{joined}'
            val = _extract_values_from_row(row, combination)
            keymap.encode(key, val, pk)
    return pk


# def _generate_all_possible_variations(model: Model, row: dict, pk: Any):
#     if pk:
#         allowed_properties = []
#         for name in sorted(model.properties.keys()):
#             if not name.startswith("_"):
#                 allowed_properties.append(name)
#
#         given_key = ''
#         if model.external and not model.external.unknown_primary_key:
#             disallowed_keys = []
#             for key in model.external.pkeys:
#                 disallowed_keys.append(key.name)
#             joined = '_'.join(disallowed_keys)
#             given_key = f'{model.model_type()}.{joined}'
#
#         ps = powerset(allowed_properties)
#         for combination in ps:
#             if combination:
#                 joined = '_'.join(combination)
#                 key = f'{model.model_type()}.{joined}'
#                 if key != given_key:
#                     yield key, _extract_values_from_row(row, combination)


def _extract_values_from_row(row: dict, keys: list):
    return_list = []
    for key in keys:
        if isinstance(key, Property):
            key = key.name
        return_list.append(row[key])
    return return_list
