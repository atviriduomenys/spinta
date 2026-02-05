import io
import pathlib
from typing import Dict, Iterator, Any, List

import requests
from dask.bag import from_sequence
from lxml import etree

from spinta import commands
from spinta.components import Context, Model, Property
from spinta.core.ufuncs import Expr, asttoexpr, asttoexpr
from spinta.datasets.backends.dataframe.backends.xml.adapter.meta_xml import MetaXml
from spinta.datasets.backends.dataframe.backends.xml.adapter.spinta import Spinta
from spinta.datasets.backends.dataframe.backends.xml.adapter.dask_xml import DaskXml
from spinta.datasets.backends.dataframe.backends.xml.adapter.xml_model import XmlModel
from spinta.datasets.backends.dataframe.backends.xml.application.data_use_cases import stream_model_data
from spinta.datasets.backends.dataframe.backends.xml.components import Xml
from spinta.datasets.backends.dataframe.commands.read import (
    parametrize_bases,
    get_dask_dataframe_meta,
    dask_get_all,
    get_pkeys_if_ref,
)
from spinta.datasets.backends.helpers import is_file_path
from spinta.datasets.keymaps.components import KeyMap
from spinta.dimensions.param.components import ResolvedParams
from spinta.exceptions import CannotReadResource, UnexpectedErrorReadingData, UnknownMethod
from spinta.types.datatype import PrimaryKey
from spinta.types.text.components import Text
from spinta.typing import ObjectData
from spinta.utils.schema import NA

def _get_query_selected_properties(query: Expr, props: dict, model: Model) -> List[str]:
    query_dict = query.todict() if query is not None else None
    manifest_paths = []
    if query_dict and 'args' in query_dict and query_dict['args'] and query_dict['name'] == 'select':
        for key, value in query_dict.items():
            if key == 'name' and value == "select" and query_dict['args']:
                for arg in query_dict['args']:
                    if arg['name'] == 'bind' and arg['args'][0] in props.keys():
                        manifest_paths.append(arg['args'][0])  
                    if arg['name'] == 'getattr':
                        if (
                            isinstance(arg.get('args'), list)
                            and len(arg['args']) > 1
                            and isinstance(arg['args'][0], dict)
                            and isinstance(arg['args'][1], dict)
                            and isinstance(arg['args'][0].get('args'), list)
                            and len(arg['args'][0]['args']) > 0
                            and isinstance(arg['args'][1].get('args'), list)
                            and len(arg['args'][1]['args']) > 0
                            and len(arg['args']) > 1
                            and arg['args'][1]['name'] == 'bind'
                            and arg['args'][1]['args']
                            and arg['args'][0]['name'] == 'bind'
                        ):
                            prop = arg['args'][0]['args'][0]
                            lang = arg['args'][1]['args'][0]
                            canidate_prop = f"{arg['args'][0]['args'][0]}@{lang}"
                            if canidate_prop in props.keys():
                                prop_def = model.properties.get(prop)
                                if (
                                    prop_def
                                    and isinstance(prop_def.dtype, Text)
                                    and lang in prop_def.dtype.langs
                                ):
                                    manifest_paths.append(str(canidate_prop))
    else:
        for prop in model.properties.values():
            manifest_paths.append(prop.name)
            if isinstance(prop.dtype, Text):
                for lang in prop.dtype.langs.keys():
                    manifest_paths.append(f"{prop.name}@{lang}")

    return manifest_paths

def _parse_xml_loop_model_properties(
    value, added_root_elements: list, model_props: dict, namespaces: dict
) -> dict[str, Any]:
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

                if pkeys and prop["source"].endswith((".", "/")):
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


def _parse_xml(data, source: str, model_props: dict, namespaces={}) -> Iterator[dict[str, Any]]:
    added_root_elements = []

    try:
        iterparse = etree.iterparse(data, events=["start"], remove_blank_text=True)
        _, root = next(iterparse)
    except etree.XMLSyntaxError as e:
        raise UnexpectedErrorReadingData(exception=type(e).__name__, message=str(e))

    if not source.startswith(("/", ".")):
        source = f"/{source}"

    values = root.xpath(source, namespaces=namespaces)
    for value in values:
        if value not in added_root_elements:
            yield _parse_xml_loop_model_properties(value, added_root_elements, model_props, namespaces)


def _get_data_xml(data_source: str, source: str, model_props: dict, namespaces: dict) -> Iterator[dict[str, Any]]:
    if data_source.startswith("http"):
        response = requests.get(data_source, timeout=30)
        yield from _parse_xml(io.BytesIO(response.content), source, model_props, namespaces)

    elif is_file_path(data_source):
        with pathlib.Path(data_source).open("rb") as file:
            yield from _parse_xml(file, source, model_props, namespaces)

    else:
        yield from _parse_xml(io.BytesIO(data_source.encode("utf-8")), source, model_props, namespaces)


def _gather_namespaces_from_model(context: Context, model: Model) -> dict:
    result = {}
    if model.external and model.external.dataset:
        for key, prefix in model.external.dataset.prefixes.items():
            result[key] = prefix.uri
    return result

def _get_dask_dataframe_mask(query: Expr, props: dict, model: Model) -> Dict[str, Dict[str, Any]]:
    df_mask = {}
    property_keys = props.keys()
    query_dict = query.todict() if query is not None else None
    if query_dict and 'args' in query_dict and query_dict['args'] and query_dict['name'] == 'and':
        for arg in query_dict['args']:
            op = arg.get('name')
            if op in {'eq', 'ne', 'lt', 'le', 'gt', 'ge', 'startswith', 'contains'}:
                left = arg['args'][0]
                right = arg['args'][1]
                if left['name'] == 'bind' and left['args'][0] in property_keys:
                    df_mask[left['args'][0]] = {"op": op, "value": right}
    if query_dict and 'args' in query_dict and query_dict['args'] and query_dict['name'] == 'eq':
        left = query_dict['args'][0]
        right = query_dict['args'][1]
        if left['name'] == 'bind' and left['args'][0] in property_keys:
            df_mask[left['args'][0]] = {"op": 'eq', "value": right}

    return df_mask

@commands.getall.register(Context, Model, Xml)
def getall(
    context: Context,
    model: Model,
    backend: Xml,
    *,
    query: Expr = None,
    resolved_params: ResolvedParams = None,
    extra_properties: dict[str, Property] = None,
    **kwargs,
) -> Iterator[ObjectData]:
    resource = model.external.resource
    keymap: KeyMap = context.get(f"keymap.{model.keymap.name}")
    
    builder = backend.query_builder_class(context)
    builder.update(model=model, params={param.name: param for param in resource.params}, url_query_params=query)

    props = {}
    for prop in model.properties.values():
        if prop.external and prop.external.name:
            props[prop.name] = {"source": prop.external.name, "pkeys": get_pkeys_if_ref(prop)}
        if isinstance(prop.dtype, Text):
            for lang, lang_prop in prop.dtype.langs.items():
                if lang_prop.external and lang_prop.external.name:
                    props[f"{prop.name}@{lang}"] = {"source": lang_prop.external.name, "pkeys": get_pkeys_if_ref(lang_prop)}

    manifest_paths = _get_query_selected_properties(query, props, model)
    df_mask = _get_dask_dataframe_mask(query, props, model)
    meta = get_dask_dataframe_meta(model)

    if resource.external:
        data_source = parametrize_bases(context, model, model.external.resource, resolved_params)
    elif resource.prepare is not NA:
        data_source = builder.resolve(resource.prepare)
    else:
        raise CannotReadResource(resource)

    df = (
        from_sequence(data_source)
        .map(
            _get_data_xml,
            namespaces=_gather_namespaces_from_model(context, model),
            source=model.external.name,
            model_props=props,
        )
        .flatten()
        .to_dataframe(meta=meta)
    )    

    yield from stream_model_data(model, Spinta(manifest_paths=manifest_paths, context=context), DaskXml(df=df, df_mask=df_mask), MetaXml(key_map=keymap), XmlModel.to_object_data)


@commands.getone.register(Context, Model, Xml)
def getone(
    context: Context,
    model: Model,
    backend: Xml,
    id_: str,
):
    private_keynames = []
    for private_key in model.external.pkeys:
        private_keynames.append(private_key.name)
    ast = {
        'name': 'select',
        'args': [{ 'name': 'bind', 'args': ['_id'] }] + [{'name': 'getattr', 'args': [ {'name': 'bind', 'args': [pk]} ]} for pk in private_keynames]
    }

    get_all = commands.getall(context, model, backend, query=asttoexpr(ast))

    for item in get_all:
        if dict(item).get('_id') == id_:
            return dict(item)
    raise UnknownMethod(f"Object with id '{id_}' not found.")