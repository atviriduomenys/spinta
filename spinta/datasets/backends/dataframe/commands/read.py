from __future__ import annotations
import math
from typing import Any, Dict, Iterator

import numpy as np
import pandas as pd
import yaml

import dask
from dask.dataframe import DataFrame

from spinta import commands
from spinta.components import Context, Property, Model
from spinta.core.ufuncs import Expr, Env
from spinta.datasets.backends.dataframe.components import DaskBackend
from spinta.datasets.backends.dataframe.backends.memory.components import MemoryDaskBackend
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
from spinta.types.datatype import PrimaryKey, Ref, DataType, Boolean, Number, Integer, DateTime
from spinta.typing import ObjectData
from spinta.ufuncs.querybuilder.components import Selected
from spinta.ufuncs.helpers import merge_formulas
from spinta.ufuncs.resultbuilder.components import ResultBuilder
from spinta.utils.data import take
from spinta.utils.schema import NA


def _resolve_expr(context: Context, row: Any, sel: Selected, params: dict) -> Any:
    if sel.item is None:
        val = None
    else:
        val = row[sel.item]
    env = ResultBuilder(context).init(val, sel.prop, row, params)
    return env.resolve(sel.prep)


def _aggregate_values(data, target: Property):
    if target is None or target.list is None:
        return data

    key_path = target.place
    key_parts = key_path.split(".")

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


def _get_row_value(context: Context, row: Any, sel: Any, params: dict | None) -> Any:
    params = params or {}

    if isinstance(sel, Selected):
        if isinstance(sel.prep, Expr):
            val = _resolve_expr(context, row, sel, params)
        elif sel.prep is not NA:
            val = _get_row_value(context, row, sel.prep, params)
        else:
            if sel.item in row.keys():
                val = row[sel.item]
            else:
                val = {}
                if isinstance(sel.prop.dtype, Ref):
                    for prop in sel.prop.dtype.refprops:
                        if f"{sel.prop.name}.{prop.name}" in row.keys():
                            val[prop.name] = row[f"{sel.prop.name}.{prop.name}"]
                        else:
                            raise PropertyNotFound(
                                sel.prop.model,
                                property=sel.prop.name,
                                external=sel.prop.external.name,
                            )
                if not val:
                    raise PropertyNotFound(
                        sel.prop.model,
                        property=sel.prop.name,
                        external=sel.prop.external.name,
                    )
        if enum_options := get_prop_enum(sel.prop):
            env = Env(context)(this=sel.prop)
            for enum_option in enum_options.values():
                if isinstance(enum_option.prepare, Expr):
                    # This is backward compatibility for older Dask versions, where empty fields returned as NaN.
                    # If we do not want to support nan type this eventually should be moved to the dask reader part.
                    if isinstance(val, float) and math.isnan(val):
                        val = None
                    processed = env.call(enum_option.prepare.name, val, *enum_option.prepare.args)
                    if val != processed:
                        val = processed
                        break
            if val is None:
                pass
            elif str(val) in enum_options:
                item = enum_options[str(val)]
                if item.prepare is not NA:
                    val = item.prepare
            else:
                raise ValueNotInEnum(sel.prop, value=val)

        return val
    if isinstance(sel, tuple):
        return tuple(_get_row_value(context, row, v, params) for v in sel)
    if isinstance(sel, list):
        return [_get_row_value(context, row, v, params) for v in sel]
    if isinstance(sel, dict):
        return {k: _get_row_value(context, row, v, params) for k, v in sel.items()}
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
        result = [_select(prop, df) for prop in pkeys]
    return Selected(prop=model.properties["_id"], prep=result)


def get_pkeys_if_ref(prop: Property) -> list[str]:
    return_list = []
    if isinstance(prop.dtype, Ref):
        for key in prop.dtype.refprops:
            if key.external.name:
                return_list.append(key.external.name)
    return return_list


def get_dask_dataframe_meta(model: Model):
    dask_meta = {}
    for prop in model.properties.values():
        if prop.external and prop.external.name:
            dask_meta[prop.external.name] = spinta_to_np_dtype(prop.dtype)
    return dask_meta


@commands.getall.register(Context, Model, MemoryDaskBackend)
def getall(
    context: Context,
    model: Model,
    backend: MemoryDaskBackend,
    *,
    query: Expr = None,
    resolved_params: ResolvedParams = None,
    extra_properties: dict[str, Property] = None,
    **kwargs,
) -> Iterator[ObjectData]:
    yaml_file = backend.config["dsn"]

    with open(yaml_file, "r") as f:
        data = list(yaml.safe_load_all(f))

    df = pd.json_normalize(data)
    mask = df["_type"] == model.name

    filtered_df = df[mask]
    filtered_df = filtered_df.dropna(axis=1)
    dask_df = dask.dataframe.from_pandas(filtered_df)

    builder = backend.query_builder_class(context)
    builder.update(model=model)

    yield from dask_get_all(context, query, dask_df, backend, model, builder, extra_properties)


def parametrize_bases(context: Context, model: Model, resource: Resource, resolved_params: dict, *args):
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


def dask_get_all(
    context: Context,
    query: Expr,
    df: dask.dataframe,
    backend: DaskBackend,
    model: Model,
    env: DaskDataFrameQueryBuilder,
    extra_properties: dict,
    params: dict | None = None,
):
    params = params or {}
    keymap: KeyMap = context.get(f"keymap.{model.keymap.name}")

    query = merge_formulas(model.external.prepare, query)
    query = merge_formulas(query, get_enum_filters(context, model))
    query = merge_formulas(query, get_ref_filters(context, model))
    env = env.init(backend, df, params)
    expr = env.resolve(query)
    where = env.execute(expr)
    qry = env.build(where)

    for i, row in qry.iterrows():
        row = row.to_dict()
        res = {
            "_type": model.model_type(),
        }
        for key, sel in env.selected.items():
            val = _get_row_value(context, row, sel, env.params)
            if sel.prop:
                if isinstance(sel.prop.dtype, PrimaryKey):
                    val = keymap.encode(sel.prop.model.model_type(), val)
                elif isinstance(sel.prop.dtype, Ref):
                    val = handle_ref_key_assignment(context, keymap, env, val, sel.prop.dtype)
            res[key] = val
        res = commands.cast_backend_to_python(context, model, backend, res, extra_properties=extra_properties)
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
