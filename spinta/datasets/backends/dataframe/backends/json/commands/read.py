from __future__ import annotations

import json
import pathlib
from typing import Iterator

import requests
from dask.bag import from_sequence

from spinta import commands
from spinta.components import Context, Model, Property
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.dataframe.backends.json.components import Json
from spinta.datasets.backends.dataframe.commands.read import (
    parametrize_bases,
    get_pkeys_if_ref,
    get_dask_dataframe_meta,
    dask_get_all,
)
from spinta.dimensions.param.components import ResolvedParams
from spinta.manifests.dict.helpers import is_blank_node, is_list_of_dicts
from spinta.typing import ObjectData


def _get_prop_full_source(source: str, prop_source: str):
    if prop_source.startswith(".."):
        split = prop_source[1:].count(".")
        split_source = source.split(".")
        value = f"{'.'.join(split_source[: len(split_source) - split])}"
        if value == "":
            return "."
        return value
    else:
        return source


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

    def traverse_json(items, current_value: dict, current_path: int = 0, in_model: bool = False):
        last_path = source[: current_path + 1]
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


def _parse_json(data, source: str, model_props: dict):
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
            if prop["root_source"][0] != ".":
                prop["root_source"].insert(0, ".")

    yield from _parse_json_with_params(data, source_list, model_props)


def _get_data_json(url: str, source: str, model_props: dict):
    if url.startswith(("http", "https")):
        f = requests.get(url, timeout=30)
        yield from _parse_json(f.text, source, model_props)
    else:
        with pathlib.Path(url).open(encoding="utf-8-sig") as f:
            yield from _parse_json(f.read(), source, model_props)


@commands.getall.register(Context, Model, Json)
def getall(
    context: Context,
    model: Model,
    backend: Json,
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
            root_source = _get_prop_full_source(model.external.name, prop.external.name)
            props[prop.external.name] = {
                "source": prop.external.name,
                "root_source": root_source.split(".") if root_source != "." else ["."],
                "pkeys": get_pkeys_if_ref(prop),
            }

    meta = get_dask_dataframe_meta(model)
    df = (
        from_sequence(bases)
        .map(_get_data_json, source=model.external.name, model_props=props)
        .flatten()
        .to_dataframe(meta=meta)
    )
    yield from dask_get_all(context, query, df, backend, model, builder, extra_properties)
