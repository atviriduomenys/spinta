import io
import pathlib
from typing import Iterator

import requests
from dask.bag import from_sequence
from lxml import etree

from spinta import commands
from spinta.components import Context, Model, Property
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.dataframe.backends.xml.components import Xml
from spinta.datasets.backends.dataframe.commands.read import (
    parametrize_bases,
    get_dask_dataframe_meta,
    dask_get_all,
    get_pkeys_if_ref,
)
from spinta.dimensions.param.components import ResolvedParams
from spinta.typing import ObjectData


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


def _parse_xml(data, source: str, model_props: dict, namespaces={}):
    added_root_elements = []
    iterparse = etree.iterparse(data, events=["start"], remove_blank_text=True)
    _, root = next(iterparse)

    if not source.startswith(("/", ".")):
        source = f"/{source}"

    values = root.xpath(source, namespaces=namespaces)
    for value in values:
        if value not in added_root_elements:
            yield _parse_xml_loop_model_properties(value, added_root_elements, model_props, namespaces)


def _get_data_xml(url: str, source: str, model_props: dict, namespaces: dict):
    if url.startswith(("http", "https")):
        f = requests.get(url, timeout=30)
        yield from _parse_xml(io.BytesIO(f.content), source, model_props, namespaces)
    else:
        with pathlib.Path(url).open("rb") as f:
            yield from _parse_xml(f, source, model_props, namespaces)


def _gather_namespaces_from_model(context: Context, model: Model) -> dict:
    result = {}
    if model.external and model.external.dataset:
        for key, prefix in model.external.dataset.prefixes.items():
            result[key] = prefix.uri
    return result


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
    bases = parametrize_bases(context, model, model.external.resource, resolved_params)
    builder = backend.query_builder_class(context)
    builder.update(model=model)
    props = {}
    for prop in model.properties.values():
        if prop.external and prop.external.name:
            props[prop.name] = {"source": prop.external.name, "pkeys": get_pkeys_if_ref(prop)}

    meta = get_dask_dataframe_meta(model)
    df = (
        from_sequence(bases)
        .map(
            _get_data_xml,
            namespaces=_gather_namespaces_from_model(context, model),
            source=model.external.name,
            model_props=props,
        )
        .flatten()
        .to_dataframe(meta=meta)
    )
    yield from dask_get_all(context, query, df, backend, model, builder, extra_properties)
