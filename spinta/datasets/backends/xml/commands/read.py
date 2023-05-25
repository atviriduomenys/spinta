from typing import Dict, Any, Iterator

import dask
from dask.dataframe import DataFrame
from lxml import etree
import time

from spinta import commands
from spinta.auth import authorized
from spinta.components import Context, Property, Model, Action
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.xml.commands.query import DaskDataFrameQueryBuilder, Selected
from spinta.datasets.backends.xml.components import Xml
from spinta.datasets.helpers import get_enum_filters, get_ref_filters
from spinta.datasets.keymaps.components import KeyMap
from spinta.datasets.utils import iterparams
from spinta.dimensions.enum.helpers import get_prop_enum
from spinta.exceptions import ValueNotInEnum, PropertyNotFound, NoExternalName, NotImplementedFeature
from spinta.manifests.components import Manifest
from spinta.types.datatype import PrimaryKey, Ref
from spinta.typing import ObjectData
from spinta.ufuncs.helpers import merge_formulas
from spinta.utils.data import take
from spinta.utils.schema import NA


@commands.load.register(Context, Xml, dict)
def load(context: Context, backend: Xml, config: Dict[str, Any]):
    pass


@commands.prepare.register(Context, Xml, Manifest)
def prepare(context: Context, backend: Xml, manifest: Manifest):
    pass


@commands.bootstrap.register(Context, Xml)
def bootstrap(context: Context, backend: Xml):
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


def parse_iterparse(url: str, source: str, model_props: Dict):
    elem_list = []
    added_root_elements = []
    with open(url, 'rb') as f:
        for action, elem in etree.iterparse(f, events=('start', 'end'), remove_blank_text=True):
            if action == 'start':
                values = elem.xpath(source)
                if not set(values).issubset(added_root_elements):
                    for value in values:
                        if value not in added_root_elements:
                            new_dict = {}
                            for prop in model_props.values():
                                v = value.xpath(prop["source"])
                                if v:
                                    v = v[0]
                                if isinstance(v, etree._Element):
                                    new_value = v.text
                                else:
                                    new_value = str(v)
                                new_dict[prop["source"]] = new_value
                            elem_list.append(new_dict)
                            added_root_elements.append(value)
                elem.clear()

    return elem_list


def parse_source(model_source: str, prop_source: str) -> str:
    fixed_model = model_source
    if model_source.startswith("/"):
        fixed_model = model_source[1:]
    if prop_source.startswith(".."):
        split = prop_source.split("/")
        split_source = fixed_model.split("/")
        return split_source[len(split_source) - 1 - len(split)]
    else:
        return prop_source


@commands.getall.register(Context, Model, Xml)
def getall(
    context: Context,
    model: Model,
    backend: Xml,
    *,
    query: Expr = None,
) -> Iterator[ObjectData]:
    base = model.external.resource.external
    keymap: KeyMap = context.get(f'keymap.{model.keymap.name}')

    builder = DaskDataFrameQueryBuilder(context)
    builder.update(model=model)

    query = merge_formulas(model.external.prepare, query)
    query = merge_formulas(query, get_enum_filters(context, model))
    query = merge_formulas(query, get_ref_filters(context, model))

    props = {}
    for prop in model.properties.values():
        if prop.external.prepare:
            print(prop.external.prepare)
        if prop.external and prop.external.name:
            props[prop.name] = {
                "source": prop.external.name,
                "alternative": parse_source(model.external.name, prop.external.name)
            }

    for params in iterparams(context, model):

        df = dask.bag.from_sequence([base]).map(
            parse_iterparse,
            source=model.external.name.format(**params),
            model_props=props
        ).flatten().to_dataframe()

        env = builder.init(backend, df)
        expr = env.resolve(query)
        where = env.execute(expr)
        qry = env.build(where)
        for i, row in qry.iterrows():
            row = row.to_dict()
            res = {
                '_type': model.model_type(),
            }
            for key, sel in env.selected.items():
                val = _get_row_value(context, row, sel)
                if sel.prop:
                    if isinstance(sel.prop.dtype, PrimaryKey):
                        val = keymap.encode(sel.prop.model.model_type(), val)
                    elif isinstance(sel.prop.dtype, Ref):
                        if sel.prop.level.value > 3:
                            val = keymap.encode(sel.prop.dtype.model.model_type(), val)
                            val = {'_id': val}
                        else:
                            if len(sel.prop.dtype.refprops) > 1:
                                raise NotImplementedFeature(sel.prop, feature="Ability to have multiple refprops")
                            val = {
                                sel.prop.dtype.refprops[0].name: val
                            }
                res[key] = val
            res = commands.cast_backend_to_python(context, model, backend, res)
            yield res



@commands.wait.register(Context, Xml)
def wait(context: Context, backend: Xml, *, fail: bool = False) -> bool:
    return True
