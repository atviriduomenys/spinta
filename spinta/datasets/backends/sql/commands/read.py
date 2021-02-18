import logging
from typing import Any
from typing import Optional

from sqlalchemy.engine import RowProxy

from spinta import commands
from spinta.auth import authorized
from spinta.components import Action
from spinta.components import Context
from spinta.components import Model
from spinta.core.ufuncs import Env
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.sql.components import Sql
from spinta.datasets.backends.sql.commands.query import Selected
from spinta.datasets.backends.sql.commands.query import SqlQueryBuilder
from spinta.datasets.keymaps.components import KeyMap
from spinta.datasets.utils import iterparams
from spinta.dimensions.enum.helpers import get_prop_enum
from spinta.exceptions import ValueNotInEnum
from spinta.types.datatype import PrimaryKey
from spinta.types.datatype import Ref
from spinta.ufuncs.helpers import merge_formulas
from spinta.utils.data import take
from spinta.utils.schema import NA

log = logging.getLogger(__name__)


def _resolve_expr(context: Context, row: RowProxy, sel: Selected) -> Any:
    val = row[sel.item]
    env = Env(context, scope={
        'this': val,
        'prop': sel.prop,
        'data': row,
    })
    return env.resolve(sel.prep)


def _get_row_value(context: Context, row: RowProxy, sel: Any) -> Any:
    if isinstance(sel, Selected):
        if isinstance(sel.prep, Expr):
            val = _resolve_expr(context, row, sel)
        # TODO: `sel.prep is not NA`.
        elif sel.prep is not None:
            val = _get_row_value(context, row, sel.prep)
        else:
            val = row[sel.item]

        if enum := get_prop_enum(sel.prop):
            if str(val) in enum:
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


def _get_enum_filters(context: Context, model: Model) -> Optional[Expr]:
    args = []
    for prop in take(['_id', all], model.properties).values():
        if (
            (enum := get_prop_enum(prop)) and
            authorized(context, prop, Action.GETALL)
        ):
            if not all(item.access >= prop.access for item in enum.values()):
                values = []
                for item in enum.values():
                    if item.access >= prop.access:
                        values.append(item.prepare)
                args.append(Expr('eq', Expr('bind', prop.name), values))

    if len(args) == 1:
        return args[0]
    elif len(args) > 1:
        return Expr('and', *args)
    else:
        return None


@commands.getall.register(Context, Model, Sql)
def getall(
    context: Context,
    model: Model,
    backend: Sql,
    *,
    query: Expr = None,
):
    conn = context.get(f'transaction.{backend.name}')
    builder = SqlQueryBuilder(context)
    builder.update(model=model)

    # Merge user passed query with query set in manifest.
    query = merge_formulas(model.external.prepare, query)
    query = merge_formulas(query, _get_enum_filters(context, model))

    keymap: KeyMap = context.get(f'keymap.{model.keymap.name}')

    for params in iterparams(model):
        table = model.external.name.format(**params)
        table = backend.get_table(model, table)

        env = builder.init(backend, table)
        expr = env.resolve(query)
        where = env.execute(expr)
        qry = env.build(where)

        for row in conn.execute(qry):
            res = {
                '_type': model.model_type(),
            }
            for key, sel in env.selected.items():
                val = _get_row_value(context, row, sel)
                if sel.prop:
                    if isinstance(sel.prop.dtype, PrimaryKey):
                        val = keymap.encode(sel.prop.model.model_type(), val)
                    elif isinstance(sel.prop.dtype, Ref):
                        val = keymap.encode(sel.prop.dtype.model.model_type(), val)
                        val = {'_id': val}
                res[key] = val
            res = commands.cast_backend_to_python(context, model, backend, res)
            yield res
