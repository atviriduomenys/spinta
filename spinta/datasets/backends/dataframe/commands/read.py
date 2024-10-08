import io
import json
import pathlib
from typing import Dict, Any, Iterator

import dask
import numpy as np
import requests
from dask.dataframe import DataFrame
from lxml import etree

from spinta import commands
from spinta.components import Context, Property, Model
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.dataframe.components import DaskBackend, Csv, Xml, Json
from spinta.datasets.backends.dataframe.ufuncs.components import TabularResource
from spinta.datasets.backends.dataframe.ufuncs.query.components import DaskDataFrameQueryBuilder
from spinta.datasets.backends.helpers import handle_ref_key_assignment
from spinta.datasets.components import Resource
from spinta.datasets.helpers import get_enum_filters, get_ref_filters
from spinta.datasets.keymaps.components import KeyMap
from spinta.datasets.utils import iterparams
from spinta.dimensions.enum.helpers import get_prop_enum
from spinta.dimensions.param.components import ResolvedParams
from spinta.exceptions import PropertyNotFound, NoExternalName, ValueNotInEnum
from spinta.manifests.components import Manifest
from spinta.manifests.dict.helpers import is_list_of_dicts, is_blank_node
from spinta.types.datatype import PrimaryKey, Ref, DataType, Boolean, Number, Integer, DateTime
from spinta.typing import ObjectData
from spinta.ufuncs.basequerybuilder.components import Selected
from spinta.ufuncs.helpers import merge_formulas
from spinta.ufuncs.resultbuilder.components import ResultBuilder
from spinta.utils.data import take
from spinta.utils.schema import NA


def _resolve_expr(context: Context, row: Any, sel: Selected) -> Any:
    if sel.item is None:
        val = None
    else:
        val = row[sel.item]
    env = ResultBuilder(context).init(val, sel.prop, row)
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


def _get_row_value(context: Context, row: Any, sel: Any) -> Any:
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


@commands.load.register(Context, DaskBackend, dict)
def load(context: Context, backend: DaskBackend, config: Dict[str, Any]):
    pass


@commands.prepare.register(Context, DaskBackend, Manifest)
def prepare(context: Context, backend: DaskBackend, manifest: Manifest, **kwargs):
    pass


@commands.bootstrap.register(Context, DaskBackend)
def bootstrap(context: Context, backend: DaskBackend):
    pass


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


def _parse_xml_loop_model_properties(value, added_root_elements: list, model_props: dict, namespaces: dict):
    new_dict = {}

    # Go through each prop source with xpath of root path
    for prop in model_props.values():
        v = value.xpath(prop["source"], namespaces=namespaces)
        new_value = None
        if v:
            v = v[0]
            if isinstance(v, etree._Element):

                # Check if prop has pkeys (means its ref type)
                # If pkeys exists iterate with xpath and get the values
                pkeys = prop["pkeys"]

                if pkeys and prop["source"].endswith(('.', '/')):
                    ref_keys = []
                    for key in pkeys:
                        ref_item = v.xpath(key, namespaces=namespaces)
                        if ref_item:
                            ref_item = ref_item[0]
                            if isinstance(ref_item, etree._Element):
                                if ref_item.text:
                                    ref_item = ref_item.text
                                elif ref_item.attrib:
                                    ref_item = ref_item.attrib
                                else:
                                    ref_item = None
                            elif not ref_item:
                                ref_item = None
                            ref_keys.append(str(ref_item))
                        if len(ref_keys) == 1:
                            new_value = ref_keys[0]
                        else:
                            new_value = ref_keys
                else:
                    if v.text:
                        new_value = str(v.text)
                    elif v.attrib:
                        new_value = v.attrib.values()
                    else:
                        new_value = None
            elif v:
                new_value = str(v)
            else:
                new_value = None
        new_dict[prop["source"]] = new_value

    added_root_elements.append(value)
    return new_dict


def _parse_xml(data, source: str, model_props: Dict, namespaces={}):
    added_root_elements = []
    iterparse = etree.iterparse(data, events=['start'], remove_blank_text=True)
    _, root = next(iterparse)

    if not source.startswith(('/', '.')):
        source = f'/{source}'

    values = root.xpath(source, namespaces=namespaces)
    for value in values:
        if value not in added_root_elements:
            yield _parse_xml_loop_model_properties(value, added_root_elements, model_props, namespaces)


def _parse_json(data, source: str, model_props: Dict):
    data = json.loads(data)
    source_list = source.split(".")
    # Prepare source for blank nodes
    blank_nodes = is_blank_node(data)
    if source == ".":
        source_list = ["."]
    else:
        if blank_nodes:
            source_list = ["."] + source_list
    if blank_nodes:
        for prop in model_props.values():
            if prop["root_source"][0] != '.':
                prop["root_source"].insert(0, '.')

    yield from _parse_json_with_params(data, source_list, model_props)


def _parse_json_with_params(data, source: list, model_props: dict):

    def get_prop_value(items, pkeys: list, prop_source: list):
        new_value = items

        # Remove empty values from split()
        prop_source = [prop for prop in prop_source if prop]
        if pkeys:
            ref_keys = []
            for key in pkeys:
                split = key.split(".")
                split = [prop for prop in split if prop]
                new_value = items
                for id, prop in enumerate(split):
                    if isinstance(new_value, dict):
                        if prop in new_value.keys():
                            new_value = new_value[prop]
                        else:
                            break
                    else:
                        break
                    if id == len(split) - 1:
                        ref_keys.append(new_value)
            if len(ref_keys) == 1:
                return ref_keys[0]
            else:
                return ref_keys
        else:
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

    def traverse_json(items, current_value: Dict, current_path: int = 0, in_model: bool = False):
        last_path = source[:current_path + 1]
        c_path = source[current_path]
        if c_path.endswith("[]"):
            c_path = c_path[:-2]
        if isinstance(items, dict):
            if in_model or c_path == ".":
                item = items
            else:
                if c_path in items.keys():
                    item = items[c_path]
                else:
                    return
            if isinstance(item, list) and is_list_of_dicts(item):
                yield from traverse_json(item, current_value, current_path, True)
            else:
                for prop in model_props.values():
                    prop_path = prop["root_source"]
                    this_path = last_path
                    if prop_path == this_path:
                        current_value[prop["source"]] = get_prop_value(item, prop["pkeys"], prop["source"].split("."))
                if current_path < len(source) - 1:
                    yield from traverse_json(item, current_value, current_path + 1, False)
                else:
                    yield current_value
        elif isinstance(items, list) and is_list_of_dicts(items):
            for item in items:
                yield from traverse_json(item, current_value.copy(), current_path, in_model)

    c_value = {}
    for prop in model_props.values():
        c_value[prop["source"]] = None
    yield from traverse_json(data, current_value=c_value)


def _get_data_xml(url: str, source: str, model_props: dict, namespaces: dict):
    if url.startswith(('http', 'https')):
        f = requests.get(url, timeout=30)
        yield from _parse_xml(io.BytesIO(f.content), source, model_props, namespaces)
    else:
        with pathlib.Path(url).open('rb') as f:
            yield from _parse_xml(f, source, model_props, namespaces)


def _get_data_json(url: str, source: str, model_props: dict):
    if url.startswith(('http', 'https')):
        f = requests.get(url, timeout=30)
        yield from _parse_json(f.text, source, model_props)
    else:
        with pathlib.Path(url).open(encoding='utf-8-sig') as f:
            yield from _parse_json(f.read(), source, model_props)


def _get_prop_full_source(source: str, prop_source: str):
    if prop_source.startswith(".."):
        split = prop_source[1:].count(".")
        split_source = source.split(".")
        value = f'{".".join(split_source[:len(split_source) - split])}'
        if value == '':
            return '.'
        return value
    else:
        return source


def _get_pkeys_if_ref(prop: Property):
    return_list = []
    if isinstance(prop.dtype, Ref):
        for key in prop.dtype.refprops:
            if key.external.name:
                return_list.append(key.external.name)
    return return_list


def _get_dask_dataframe_meta(model: Model):
    dask_meta = {}
    for prop in model.properties.values():
        if prop.external and prop.external.name:
            dask_meta[prop.external.name] = spinta_to_np_dtype(prop.dtype)
    return dask_meta


@commands.getall.register(Context, Model, Json)
def getall(
    context: Context,
    model: Model,
    backend: Json,
    *,
    query: Expr = None,
    resolved_params: ResolvedParams = None,
    **kwargs
) -> Iterator[ObjectData]:
    bases = parametrize_bases(
        context,
        model,
        model.external.resource,
        resolved_params
    )

    builder = DaskDataFrameQueryBuilder(context)
    builder.update(model=model)
    props = {}
    for prop in model.properties.values():
        if prop.external and prop.external.name:
            root_source = _get_prop_full_source(model.external.name, prop.external.name)
            props[prop.external.name] = {
                "source": prop.external.name,
                "root_source": root_source.split(".") if root_source != "." else ["."],
                "pkeys": _get_pkeys_if_ref(prop)
            }

    meta = _get_dask_dataframe_meta(model)
    df = dask.bag.from_sequence(bases).map(
        _get_data_json,
        source=model.external.name,
        model_props=props
    ).flatten().to_dataframe(meta=meta)
    yield from _dask_get_all(context, query, df, backend, model, builder)


@commands.getall.register(Context, Model, Xml)
def getall(
    context: Context,
    model: Model,
    backend: Xml,
    *,
    query: Expr = None,
    resolved_params: ResolvedParams = None,
    **kwargs
) -> Iterator[ObjectData]:
    bases = parametrize_bases(
        context,
        model,
        model.external.resource,
        resolved_params
    )
    builder = DaskDataFrameQueryBuilder(context)
    builder.update(model=model)
    props = {}
    for prop in model.properties.values():
        if prop.external and prop.external.name:
            props[prop.name] = {
                "source": prop.external.name,
                "pkeys": _get_pkeys_if_ref(prop)
            }

    meta = _get_dask_dataframe_meta(model)
    df = dask.bag.from_sequence(bases).map(
        _get_data_xml,
        namespaces=_gather_namespaces_from_model(context, model),
        source=model.external.name,
        model_props=props
    ).flatten().to_dataframe(meta=meta)
    yield from _dask_get_all(context, query, df, backend, model, builder)


def _gather_namespaces_from_model(context: Context, model: Model):
    result = {}
    if model.external and model.external.dataset:
        for key, prefix in model.external.dataset.prefixes.items():
            result[key] = prefix.uri
    return result


@commands.getall.register(Context, Model, Csv)
def getall(
    context: Context,
    model: Model,
    backend: Csv,
    *,
    query: Expr = None,
    resolved_params: ResolvedParams = None,
    **kwargs
) -> Iterator[ObjectData]:
    resource_builder = TabularResource(context)
    resource_builder.resolve(model.external.resource.prepare)
    bases = parametrize_bases(
        context,
        model,
        model.external.resource,
        resolved_params,
        model.external.name
    )

    builder = DaskDataFrameQueryBuilder(context)
    builder.update(model=model)
    df = dask.dataframe.read_csv(list(bases), sep=resource_builder.seperator)
    yield from _dask_get_all(context, query, df, backend, model, builder)


def parametrize_bases(
    context: Context,
    model: Model,
    resource: Resource,
    resolved_params: dict,
    *args
):
    base = resource.external
    if len(resource.source_params) > 0:
        if not resolved_params:
            params = model.external.resource.params
            resolved_params = iterparams(context, model, model.manifest, params)
            for item in resolved_params:
                yield base.format(*args, **item)
        else:
            formatted = base.format(*args, **resolved_params)
            if formatted != base:
                yield formatted
    else:
        yield base


def _dask_get_all(context: Context, query: Expr, df: dask.dataframe, backend: DaskBackend, model: Model, env: DaskDataFrameQueryBuilder):
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
                    val = handle_ref_key_assignment(context, keymap, env, val, sel.prop.dtype)
            res[key] = val
        res = commands.cast_backend_to_python(context, model, backend, res)
        yield res


@commands.wait.register(Context, DaskBackend)
def wait(context: Context, backend: DaskBackend, *, fail: bool = False) -> bool:
    return True


@commands.spinta_to_np_dtype.register(Boolean)
def spinta_to_np_dtype(dtype: Boolean):
    return "boolean"


@commands.spinta_to_np_dtype.register(Number)
def spinta_to_np_dtype(dtype: Number):
    return np.dtype("f")


@commands.spinta_to_np_dtype.register(Integer)
def spinta_to_np_dtype(dtype: Integer):
    return "Int64"


@commands.spinta_to_np_dtype.register(DateTime)
def spinta_to_np_dtype(dtype: DateTime):
    return "object"


@commands.spinta_to_np_dtype.register(DataType)
def spinta_to_np_dtype(dtype: DataType):
    return np.dtype("S")
