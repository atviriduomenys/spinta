import json
import urllib
from typing import Dict, Any, Iterator, List

import dask
import requests
from dask.dataframe import DataFrame
from lxml import etree

from spinta import commands
from spinta.components import Context, Property, Model
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.dataframe.components import DaskBackend, Csv, Xml, Json
from spinta.datasets.backends.dataframe.commands.query import DaskDataFrameQueryBuilder, Selected

from spinta.datasets.backends.helpers import handle_ref_key_assignment
from spinta.datasets.helpers import get_enum_filters, get_ref_filters
from spinta.datasets.keymaps.components import KeyMap
from spinta.datasets.utils import iterparams
from spinta.dimensions.enum.helpers import get_prop_enum
from spinta.exceptions import ValueNotInEnum, PropertyNotFound, NoExternalName
from spinta.manifests.components import Manifest
from spinta.manifests.dict.helpers import is_list_of_dicts
from spinta.types.datatype import PrimaryKey, Ref
from spinta.typing import ObjectData
from spinta.ufuncs.helpers import merge_formulas
from spinta.utils.data import take
from spinta.utils.schema import NA


@commands.load.register(Context, DaskBackend, dict)
def load(context: Context, backend: DaskBackend, config: Dict[str, Any]):
    pass


@commands.prepare.register(Context, DaskBackend, Manifest)
def prepare(context: Context, backend: DaskBackend, manifest: Manifest):
    pass


@commands.bootstrap.register(Context, DaskBackend)
def bootstrap(context: Context, backend: DaskBackend):
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
            if sel.item in row.keys():
                val = row[sel.item]
            else:
                raise PropertyNotFound(
                    sel.prop.model,
                    property=sel.prop.name,
                    external=sel.prop.external.name,
                )

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


def parse_iterparse(data, source: str, model_props: Dict):
    elem_list = []
    added_root_elements = []

    for action, elem in etree.iterparse(data, events=('start', 'end'), remove_blank_text=True):
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


def parse_json_with_params(data, source: str, model_props: Dict):
    data = json.loads(data)
    source_list = source.split(".")
    if source == ".":
        source_list = ["."]

    values = extract_values(data, source_list, model_props)
    return values


def extract_values(data, source: list, model_props: dict):
    result = []

    def get_prop_value(items, prop_source: list):
        new_value = items
        prop_source = [prop for prop in prop_source if prop]
        for id, prop in enumerate(prop_source):
            if isinstance(new_value, dict):
                if prop in new_value.keys():
                    new_value = new_value[prop]
                else:
                    return None
            else:
                return None
            if id == len(prop_source) - 1:
                return new_value

    def traverse(items, current_value: Dict = {}, current_path: int = 0, in_model: bool = False):
        last_path = source[:current_path + 1]
        c_path = source[current_path]
        if c_path.endswith("[]"):
            c_path = c_path[:-2]

        if isinstance(items, dict):
            if in_model:
                item = items
            else:
                if c_path in items.keys():
                    item = items[c_path]
                else:
                    return
            if isinstance(item, list) and is_list_of_dicts(item):
                traverse(item, current_value, current_path, True)
            else:
                for prop in model_props.values():
                    if prop["root_source"] == last_path:
                        current_value[prop["source"]] = get_prop_value(item, prop["source"].split("."))
                if current_path != len(source) - 1:
                    traverse(item, current_value, current_path + 1, False)
                else:
                    result.append(current_value)
        elif isinstance(items, list) and is_list_of_dicts(items):
            for item in items:
                traverse(item, current_value.copy(), current_path, in_model)

    traverse(data, in_model=source == ["."])
    return result


def get_data_xml(url: str, source: str, model_props: Dict):
    if url.startswith(('http', 'https')):
        f = requests.get(url)
        return parse_iterparse(f.raw, source, model_props)
    else:
        with open(url, 'rb') as f:
            return parse_iterparse(f, source, model_props)


def get_data_json(url: str, source: str, model_props: List):
    if url.startswith(('http', 'https')):
        f = requests.get(url)
        return parse_json_with_params(f.text, source, model_props)
    else:
        with open(url, 'rb') as f:
            return parse_json_with_params(f.read(), source, model_props)


def get_full_source_json(source: str, prop_source: str):
    if prop_source.startswith(".."):
        split = prop_source[1:].count(".")
        split_source = source.split(".")
        return f'{".".join(split_source[:len(split_source) - split])}'
    else:
        return source


@commands.getall.register(Context, Model, Json)
def getall(
    context: Context,
    model: Model,
    backend: Json,
    *,
    query: Expr = None,
) -> Iterator[ObjectData]:
    base = model.external.resource.external
    builder = DaskDataFrameQueryBuilder(context)
    builder.update(model=model)
    props = {}
    for prop in model.properties.values():
        if prop.external and prop.external.name:
            root_source = get_full_source_json(model.external.name, prop.external.name)
            props[prop.external.name] = {
                "source": prop.external.name,
                "root_source": root_source.split(".") if root_source!="." else ["."]
            }

    for params in iterparams(context, model):
        df = dask.bag.from_sequence([base]).map(
            get_data_json,
            source=model.external.name.format(**params),
            model_props=props
        ).flatten().to_dataframe()
        yield from dask_get_all(context, query, df, backend, model, builder)


@commands.getall.register(Context, Model, Xml)
def getall(
    context: Context,
    model: Model,
    backend: Xml,
    *,
    query: Expr = None,
) -> Iterator[ObjectData]:
    base = model.external.resource.external
    builder = DaskDataFrameQueryBuilder(context)
    builder.update(model=model)
    props = {}
    for prop in model.properties.values():
        if prop.external and prop.external.name:
            props[prop.name] = {
                "source": prop.external.name,
                "alternative": parse_source(model.external.name, prop.external.name)
            }

    for params in iterparams(context, model):
        df = dask.bag.from_sequence([base]).map(
            get_data_xml,
            source=model.external.name.format(**params),
            model_props=props
        ).flatten().to_dataframe()
        yield from dask_get_all(context, query, df, backend, model, builder)


@commands.getall.register(Context, Model, Csv)
def getall(
    context: Context,
    model: Model,
    backend: Csv,
    *,
    query: Expr = None,
) -> Iterator[ObjectData]:
    base = model.external.resource.external
    builder = DaskDataFrameQueryBuilder(context)
    builder.update(model=model)

    for params in iterparams(context, model):
        if base:
            url = urllib.parse.urljoin(base, model.external.name)
        else:
            url = model.external.name
        url = url.format(**params)
        df = dask.dataframe.read_csv(url)
        yield from dask_get_all(context, query, df, backend, model, builder)


def dask_get_all(context: Context, query: Expr, df: dask.dataframe, backend: DaskBackend, model: Model, env: DaskDataFrameQueryBuilder):
    keymap: KeyMap = context.get(f'keymap.{model.keymap.name}')

    query = merge_formulas(model.external.prepare, query)
    query = merge_formulas(query, get_enum_filters(context, model))
    query = merge_formulas(query, get_ref_filters(context, model))
    env = env.init(backend, df)
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
                    val = handle_ref_key_assignment(keymap, val, sel.prop)
            res[key] = val
        res = commands.cast_backend_to_python(context, model, backend, res)
        yield res


@commands.wait.register(Context, DaskBackend)
def wait(context: Context, backend: DaskBackend, *, fail: bool = False) -> bool:
    return True
