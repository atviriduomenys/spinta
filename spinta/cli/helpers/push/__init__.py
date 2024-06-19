import datetime
import json
import pathlib
from decimal import Decimal

import dateutil.parser
from multipledispatch import dispatch

from spinta.components import Context, Page, Model, Property
from spinta.types.datatype import DataType, DateTime, Number, Date, Time, URL, URI
from spinta.utils.json import fix_data_for_json


@dispatch(Context, Model, dict)
def prepare_data_for_push_state(context: Context, model: Model, data: dict):
    processed_data = {}
    for key, value in data.items():
        processed_data[key] = prepare_data_for_push_state(context, model.flatprops[key], value)
    return processed_data


@dispatch(Context, Property, object)
def prepare_data_for_push_state(context: Context, prop: Property, data: object):
    return prepare_data_for_push_state(context, prop.dtype, data)


@dispatch(Context, DataType, object)
def prepare_data_for_push_state(context: Context, dtype: DataType, data: object):
    return data


@dispatch(Context, DataType, Decimal)
def prepare_data_for_push_state(context: Context, dtype: DataType, data: Decimal):
    return float(data)


@dispatch(Context, DataType, (dict, list))
def prepare_data_for_push_state(context: Context, dtype: DataType, data: dict):
    data = fix_data_for_json(data)
    data = json.dumps(data)
    return data


@dispatch(Context, DataType, pathlib.Path)
def prepare_data_for_push_state(context: Context, dtype: DataType, data: pathlib.Path):
    return str(data)


@dispatch(Context, DateTime, str)
def prepare_data_for_push_state(context: Context, dtype: DateTime, data: str):
    try:
        return datetime.datetime.fromisoformat(data)
    except ValueError:
        return dateutil.parser.parse(data)


@dispatch(Context, Date, str)
def prepare_data_for_push_state(context: Context, dtype: Date, data: str):
    try:
        return datetime.date.fromisoformat(data)
    except ValueError:
        return dateutil.parser.parse(data).date()


@dispatch(Context, Time, str)
def prepare_data_for_push_state(context: Context, dtype: Time, data: str):
    try:
        return datetime.time.fromisoformat(data)
    except ValueError:
        return dateutil.parser.parse(data).time()



