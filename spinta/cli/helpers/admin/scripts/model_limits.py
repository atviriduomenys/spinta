import json
import pathlib
from typing import Optional

import tqdm
from click import echo

from spinta import commands
from spinta.backends import Backend
from spinta.backends.postgresql.components import PostgreSQL
from spinta.cli.helpers.script.helpers import ensure_store_is_loaded, parse_input_path
from spinta.components import Context, Model, Property
from multipledispatch import dispatch

from spinta.types.datatype import DataType, Ref, ExternalRef, Boolean, Date, DateTime, Integer, Number
from spinta.types.geometry.components import Geometry
from spinta.utils.config import get_limit_path
from spinta.utils.nestedstruct import flat_dicts_to_nested

import sqlalchemy as sa
import ruamel.yaml


@dispatch(Context, Model)
def extract_limit(context: Context, model: Model) -> int:
    return extract_limit(context, model.backend, model)


@dispatch(Context, Backend, Model)
def extract_limit(context: Context, backend: Backend, model: Model):
    return None


@dispatch(Context, PostgreSQL, Model)
def extract_limit(context: Context, backend: PostgreSQL, model: Model):
    table = backend.get_table(model)

    columns = [col for col in table.columns if not col.name.startswith("_") or col.name in ["_id", "_revision"]]
    columns = [sa.func.sum(sa.func.pg_column_size(col)).label(col.name) for col in columns]

    query = sa.select(
        [
            sa.func.count().label("_count"),
            *columns,
        ]
    ).select_from(table)
    result = dict(backend.engine.execute(query).fetchone())
    count = result.pop("_count")

    if result is None:
        return None

    return generate_limit_with_json_overhead(context, model, count, result)


@dispatch(DataType)
def generate_empty_template(dtype: DataType) -> str:
    return ""


@dispatch(Ref)
def generate_empty_template(dtype: Ref) -> dict:
    return {"_id": ""}


@dispatch(ExternalRef)
def generate_empty_template(dtype: ExternalRef) -> dict:
    return {prop.name: "" for prop in dtype.refprops}


@dispatch(Property)
def generate_empty_template(prop: Property) -> object:
    return generate_empty_template(prop.dtype)


@dispatch(Model)
def generate_empty_template(model: Model) -> dict:
    reserved_props = ["_type", "_id", "_revision"]
    result = {}
    for key, prop in model.flatprops.items():
        if key.startswith("_") and key not in reserved_props:
            continue

        result[key] = generate_empty_template(prop)
    return result


@dispatch(Backend, DataType, dict, int)
def adjust_bytes(backend: Backend, dtype: DataType, totals: dict, count: int):
    return


@dispatch(PostgreSQL, Boolean, dict, int)
def adjust_bytes(backend: PostgreSQL, dtype: Boolean, totals: dict, count: int):
    name, data = _extract_column_byte_data(backend, dtype.prop, totals)
    if data is not None:
        # Converting 0 to "false" is 5x increase and 1 to "true" is 4x increase, so the average is 4.5
        totals[name] = data * 4.5


@dispatch(PostgreSQL, Geometry, dict, int)
def adjust_bytes(backend: PostgreSQL, dtype: Geometry, totals: dict, count: int):
    name, data = _extract_column_byte_data(backend, dtype.prop, totals)
    if data is not None:
        # On average, converting to wkt it increases by 2x
        totals[name] = data * 2


@dispatch(PostgreSQL, Date, dict, int)
def adjust_bytes(backend: PostgreSQL, dtype: Date, totals: dict, count: int):
    name, data = _extract_column_byte_data(backend, dtype.prop, totals)
    if data is not None:
        # Postgresql stores date as 4 bytes, so converting to string increases by 2.5x
        totals[name] = data * 2.5


@dispatch(PostgreSQL, DateTime, dict, int)
def adjust_bytes(backend: PostgreSQL, dtype: Date, totals: dict, count: int):
    name, data = _extract_column_byte_data(backend, dtype.prop, totals)
    if data is not None:
        # Postgresql stores datetime as 8 bytes, so converting to string increases by 3.25x
        totals[name] = data * 3.25


@dispatch(Backend, Property, dict, int)
def adjust_bytes(backend: Backend, prop: Property, totals: dict, count: int):
    adjust_bytes(backend, prop.dtype, totals, count)


@dispatch(Backend, Model, dict, int)
def adjust_bytes(backend: Backend, model: Model, totals: dict, count: int) -> dict:
    reserved = ["_id", "_revision"]
    result = totals.copy()
    result["_type"] = len(model.model_type()) * count
    for prop in model.flatprops.values():
        if prop.name.startswith("_") and prop.name not in reserved:
            continue

        adjust_bytes(backend, prop, result, count)
    return result


def _extract_column_byte_data(backend: PostgreSQL, prop: Property, data: dict) -> (str, Optional[int]):
    table = backend.get_table(prop.model)
    column = backend.get_column(table, prop)
    data = data.get(column.name, None)
    return column.name, data


def generate_limit_with_json_overhead(context: Context, model: Model, count: int, totals: dict):
    if count == 0:
        return None

    config = context.get("config")
    json_exporter = config.exporters["json"]
    result = json_exporter({})
    empty_template = "".join(list(result))
    props = generate_empty_template(model)

    prop_template = json.dumps(flat_dicts_to_nested(props), separators=(",", ":"), ensure_ascii=False)
    # Types that are not converted to string (does not have "" surrounding, meaning -2 bytes per row)
    json_supported_types = (Integer, Boolean, Number)
    prop_offset = len([prop for prop in model.flatprops.values() if isinstance(prop.dtype, json_supported_types)]) * 2

    empty_template_size = len(empty_template)
    property_template_size = len(prop_template) - prop_offset
    # How many commas are needed to separate all the properties
    comma_offset = count - 1

    adjusted_totals = adjust_bytes(model.backend, model, totals, count)
    total = sum(value for value in adjusted_totals.values() if value is not None)

    json_response_size = empty_template_size + property_template_size * count + comma_offset + total
    json_response_avg = json_response_size / count
    json_response_limit = config.default_limit_bytes / json_response_avg
    result = round(json_response_limit)
    if result >= count:
        return None  # No need to add limit, if limit is higher than number of rows

    return result


def generate_model_limits(
    context: Context,
    input_path: pathlib.Path = None,
    **kwargs,
):
    store = ensure_store_is_loaded(context)
    config = context.get("config")

    whitelist = parse_input_path(context, input_path, required=False)
    if whitelist:
        models = [commands.get_model(context, store.manifest, model_name) for model_name in whitelist]
    else:
        models = commands.get_models(context, store.manifest).values()

    models = [model for model in models if not model.name.startswith("_")]
    model_count = len(models)
    counter = tqdm.tqdm(desc="GENERATING MODEL LIMITS", ascii=True, total=model_count)
    yml = ruamel.yaml.YAML()

    data = {}
    try:
        for model in models:
            counter.write(f"\t{model.model_type()}", end=" - ")
            limit = extract_limit(context, model)
            if limit is not None:
                data[model.model_type()] = limit
                counter.write(f"{limit}")
            else:
                counter.write("Skipped")
            counter.update(1)
    finally:
        counter.close()

    limit_path = get_limit_path(config)
    echo(f"WRITING MODEL LIMITS TO {limit_path}")

    with limit_path.open("w") as file:
        yml.dump(data, file)
