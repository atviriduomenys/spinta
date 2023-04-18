import uuid
import urllib.parse
from typing import Dict, Any
from typing import Iterator

import dask.dataframe as dd
from dask.dataframe import DataFrame

from spinta import commands
from spinta.core.ufuncs import Expr
from spinta.typing import ObjectData
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.components import Model
from spinta.components import Property
from spinta.datasets.utils import iterparams
from spinta.datasets.backends.csv.components import Csv
from spinta.datasets.keymaps.components import KeyMap
from spinta.types.datatype import PrimaryKey
from spinta.types.datatype import Ref
from spinta.datasets.backends.sql.commands.query import Selected
from spinta.dimensions.enum.helpers import get_prop_enum
from spinta.exceptions import ValueNotInEnum
from spinta.utils.schema import NA
from spinta.utils.data import take
from spinta.auth import authorized
from spinta.components import Action
from spinta.exceptions import NoExternalName
from spinta.exceptions import PropertyNotFound


@commands.load.register(Context, Csv, dict)
def load(context: Context, backend: Csv, config: Dict[str, Any]):
    pass


@commands.prepare.register(Context, Csv, Manifest)
def prepare(context: Context, backend: Csv, manifest: Manifest):
    pass


@commands.bootstrap.register(Context, Csv)
def bootstrap(context: Context, backend: Csv):
    pass


def _resolve_expr(
    context: Context,
    row: Dict[str, Any],
    sel: Selected,
) -> Any:
    if sel.item is None:
        val = None
    else:
        val = row[sel.item]
    raise NotImplementedError
    return val
    # env = SqlResultBuilder(context).init(val, sel.prop, row)
    # return env.resolve(sel.prep)


def _get_row_value(context: Context, row: Dict[str, Any], sel: Any) -> Any:
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


def _select(prop: Property, df: DataFrame):
    if prop.external is None or not prop.external.name:
        raise NoExternalName(prop)
    column = prop.external.name
    if column not in df.columns:
        raise PropertyNotFound(
            prop.model,
            property=prop.name,
            external=prop.external.name,
        )
    return Selected(
        item=column,
        prop=prop,
    )


def _select_primary_key(model: Model, df: DataFrame):
    pkeys = model.external.pkeys

    if not pkeys:
        # If primary key is not specified use all properties to uniquely
        # identify row.
        pkeys = take(model.properties).values()

    if len(pkeys) == 1:
        prop = pkeys[0]
        result = _select(prop, df)
    else:
        result = [
            _select(prop, df)
            for prop in pkeys
        ]
    return Selected(prop=model.properties['_id'], prep=result)


def _get_selected(
    context: Context,
    model: Model,
    df: DataFrame,
) -> Dict[str, Selected]:
    selected: Dict[str, Selected] = {}
    for prop in take(['_id', all], model.properties).values():
        if authorized(context, prop, Action.GETALL):
            if isinstance(prop.dtype, PrimaryKey):
                sel = _select_primary_key(model, df)
            else:
                sel = _select(prop, df)
            selected[prop.place] = sel
    return selected


@commands.getall.register(Context, Model, Csv)
def getall(
    context: Context,
    model: Model,
    backend: Csv,
    *,
    query: Expr = None,
) -> Iterator[ObjectData]:
    base = model.external.resource.external
    keymap: KeyMap = context.get(f'keymap.{model.keymap.name}')
    for params in iterparams(context, model):
        if base:
            url = urllib.parse.urljoin(base, model.external.name)
        else:
            url = model.external.name
        url = url.format(**params)
        df = dd.read_csv(url)
        selected: Dict[str, Selected] = _get_selected(context, model, df)
        for row in df.itertuples(index=False):
            row = row._asdict()
            res = {
                '_type': model.model_type(),
            }
            for key, sel in selected.items():
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


@commands.wait.register(Context, Csv)
def wait(context: Context, backend: Csv, *, fail: bool = False) -> bool:
    return True
