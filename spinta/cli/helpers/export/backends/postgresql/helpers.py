import csv
import datetime
import json
import pathlib
from functools import lru_cache
from typing import List, AsyncIterator

from multipledispatch import dispatch

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.cli.helpers.export.backends.postgresql.components import PostgresqlExportMetadata, TableData
from spinta.cli.helpers.export.components import CounterManager
from spinta.components import Context, Model, Action, DataItem
from spinta.types.datatype import Array, DataType, File
from spinta.utils.aiotools import anext, achain
from spinta.utils.itertools import ensure_list
from spinta.utils.json import fix_data_for_json


def generate_export_metadata(
        model: Model,
        name: str,
        file: object,
        fieldnames: list,
        counter: CounterManager
) -> PostgresqlExportMetadata:
    counter.add_counter(
        model,
        name
    )

    writer = csv.DictWriter(file, fieldnames=fieldnames)
    return PostgresqlExportMetadata(
        name=name,
        model=model,
        writer=writer,
    )


def generate_table_data(dtype: DataType, data: object):
    return TableData(
        name=generate_file_name(dtype),
        prop=dtype.prop,
        data=data
    )


@dispatch(Array)
@lru_cache
def generate_file_name(dtype: Array) -> str:
    return f'{dtype.prop.model.model_type()}.{dtype.prop.place}.array'


@dispatch(File)
@lru_cache
def generate_file_name(dtype: File) -> str:
    return f'{dtype.prop.model.model_type()}.{dtype.prop.place}.file'


@dispatch(DataType)
def generate_file_name(dtype: DataType):
    return None


@dispatch(Array)
@lru_cache
def generate_file_name(dtype: Array):
    return f'{dtype.prop.model.model_type()}.{dtype.prop.place}.array'


@dispatch(File)
@lru_cache
def generate_file_name(dtype: File):
    return f'{dtype.prop.model.model_type()}.{dtype.prop.place}.file'


@dispatch(Context, File, PostgreSQL)
def generate_csv_headers(context: Context, dtype: DataType, backend: PostgreSQL):
    raise Exception("NOT IMPLEMENTED")


@dispatch(Context, File, PostgreSQL)
def generate_csv_headers(context: Context, dtype: File, backend: PostgreSQL):
    raise Exception("NOT IMPLEMENTED")


@dispatch(Context, Array, PostgreSQL)
def generate_csv_headers(context: Context, dtype: Array, backend: PostgreSQL):
    columns = commands.prepare(context, backend, dtype.items)
    columns = ensure_list(columns)

    return [
        '_txn',
        '_rid',
        *[column.name for column in columns]
    ]


def split_data(data: dict) -> (dict, List[TableData]):
    other_dict = {k: v for k, v in data.items() if not isinstance(v, TableData)}
    table_list = [v for v in data.values() if isinstance(v, TableData)]
    return other_dict, table_list


def prepare_changelog(data: dict, _id: int, txn: str, created: datetime.datetime) -> dict:
    reserved_data = {k: v for k, v in data.items() if k.startswith('_')}
    given_data = {k: v for k, v in data.items() if not k.startswith('_')}
    return {
        '_id': _id,
        '_revision': reserved_data['_revision'],
        '_txn': txn,
        '_rid': reserved_data['_id'],
        'datetime': created,
        'action': Action.INSERT.value,
        'data': json.dumps(fix_data_for_json(given_data))
    }


def generate_file_paths(model: Model, root_path: pathlib.Path) -> (dict, dict):
    directory = root_path / model.ns.name
    directory.mkdir(exist_ok=True, parents=True)

    default_paths = [
        model.model_type(),
        f'{model.model_type()}.changes'
    ]

    additional_paths = {
        prop: generate_file_name(prop.dtype) for prop in model.flatprops.values()
    }
    additional_paths = {
        path: prop for prop, path in additional_paths.items() if path is not None
    }

    extended_paths = default_paths + list(additional_paths.keys())
    return {
        path: f'{root_path / pathlib.Path(path)}.csv' for path in extended_paths
    }, additional_paths


async def extract_headers(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    dstream: AsyncIterator[DataItem],
    **kwargs
) -> (list, AsyncIterator[DataItem]):
    peek = await anext(dstream)
    data = commands.before_export(
        context,
        model,
        backend,
        data=peek,
        **kwargs
    )
    data_headers = split_data(data)[0].keys()
    dstream = achain([peek], dstream)
    return data_headers, dstream
