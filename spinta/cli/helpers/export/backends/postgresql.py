import csv
import datetime
import pathlib
from contextlib import AsyncExitStack
from functools import lru_cache

from multipledispatch import dispatch

import aiofiles
from typing import Iterator, Any, List

import tqdm

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.cli.helpers.export.helpers import create_data_items, prepare_for_export, prepare_data_without_checks
from spinta.commands.write import prepare_data, prepare_patch, prepare_data_for_write
from spinta.components import Context, Model, UrlParams, Action, DataSubItem, Property
from spinta.types.datatype import Array, DataType, File
from spinta.types.geometry.components import Geometry
from spinta.utils.aiotools import aiter, aenumerate, anext, achain
from spinta.utils.data import take
from spinta.utils.json import fix_data_for_json


class TableData:
    name: str
    prop: Property
    data: Any

    def __init__(self, name: str, prop: Property, data: Any):
        self.name = name
        self.prop = prop
        self.data = data


class PostgresqlExportMetadata:
    name: str
    writer: csv.DictWriter
    counter: tqdm.tqdm

    def __init__(self, name: str, writer: csv.DictWriter, counter: tqdm.tqdm):
        self.name = name
        self.writer = writer
        self.counter = counter

    async def writeheader(self):
        await self.writer.writeheader()

    async def writerow(self, data: dict):
        await self.writer.writerow(data)
        self.counter.update(1)

    async def write_table_data(self, table_data: TableData, data: dict):
        self.counter.update(1)
        if not table_data.data or not isinstance(table_data.data, (list, dict)):
            return

        extra_fields = extract_extra_fields(table_data.prop.dtype, data)
        if isinstance(table_data.data, list):
            for value in table_data.data:
                await self.writer.writerow({
                    **extra_fields,
                    **value
                })
        else:
            await self.writer.writerow({
                **extra_fields,
                **table_data.data
            })

    def close(self):
        self.counter.close()


def generate_export_metadata(
        name: str,
        count: int,
        file: object,
        fieldnames: list
) -> PostgresqlExportMetadata:
    counter = tqdm.tqdm(
        desc=name,
        ascii=True,
        leave=False,
        total=count
    )

    writer = csv.DictWriter(file, fieldnames=fieldnames)
    return PostgresqlExportMetadata(
        name=name,
        writer=writer,
        counter=counter
    )


def generate_table_data(dtype: DataType, data: object):
    return TableData(
        name=generate_file_name(dtype),
        prop=dtype.prop,
        data=data
    )


@dispatch(DataType)
def extract_extra_fields(dtype: DataType, data: dict) -> dict:
    return {}


@dispatch(Array)
def extract_extra_fields(dtype: Array, data: dict) -> dict:
    return {
        '_txn': data['_txn'],
        '_rid': data['_id']
    }


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
def generate_csv_headers(context: Context, dtype: DataType,backend: PostgreSQL):
    raise Exception("NOT IMPLEMENTED")


@dispatch(Context, File, PostgreSQL)
def generate_csv_headers(context: Context, dtype: File, backend: PostgreSQL):
    raise Exception("NOT IMPLEMENTED")


@dispatch(Context, Array, PostgreSQL)
def generate_csv_headers(context: Context, dtype: Array, backend: PostgreSQL):
    columns = commands.prepare(context, backend, dtype.items)
    return [
        '_txn',
        '_rid',
        *[column.name for column in columns]
    ]


def split_data(data: dict) -> (dict, List[TableData]):
    other_dict = {k: v for k, v in data.items() if not isinstance(v, TableData)}
    table_list = [v for v in data.values() if isinstance(v, TableData)]
    return other_dict, table_list


def prepare_changelog(data: dict, _id: int) -> dict:
    reserved_data = {k: v for k, v in data.items() if k.startswith('_')}
    given_data = {k: v for k, v in data.items() if not k.startswith('_')}
    return {
        '_id': _id,
        '_revision': reserved_data['_revision'],
        '_txn': reserved_data['_txn'],
        '_rid': reserved_data['_id'],
        'datetime': reserved_data['_created'],
        'action': Action.INSERT.value,
        'data': fix_data_for_json(given_data)
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


@commands.export_data.register(Context, Model, PostgreSQL)
async def export_data(
        context: Context,
        model: Model,
        backend: PostgreSQL,
        *,
        data: Iterator,
        path: str,
        count: int,
        **kwargs
):
    params = UrlParams()
    params.action = Action.INSERT
    dstream = aiter(create_data_items(model, backend, data))
    dstream = prepare_data_without_checks(context, dstream)
    dstream = prepare_patch(context, dstream)
    dstream = prepare_data_for_write(context, dstream, params)

    txn = commands.gen_object_id(context, backend, model)
    dstream = prepare_for_export(context, model, backend, dstream, txn=txn)

    peek = await anext(dstream)
    data_headers = split_data(peek)[0].keys()
    dstream = achain([peek], dstream)

    file_mapping, property_mapping = generate_file_paths(model, pathlib.Path(path))
    base_key = model.model_type()
    changes_key = f'{model.model_type()}.changes'

    async with AsyncExitStack() as stack:
        files = {
            key: await stack.enter_async_context(aiofiles.open(path, 'w+', newline=''))
            for key, path in file_mapping.items()
        }

        base_export_metadata = generate_export_metadata(
            base_key,
            count,
            files[base_key],
            fieldnames=data_headers
        )

        changes_export_metadata = generate_export_metadata(
            changes_key,
            count,
            files[changes_key],
            fieldnames=['_id', '_revision', '_txn', '_rid', 'datetime', 'action', 'data']
        )

        export_metadata: dict[str, PostgresqlExportMetadata] = {
            base_key: base_export_metadata,
            changes_key: changes_export_metadata,
            **{
                key: generate_export_metadata(
                    key,
                    count,
                    files[key],
                    fieldnames=generate_csv_headers(context, prop.dtype, backend)
                ) for key, prop in property_mapping.items() if key not in [base_key, changes_key]
            }
        }

        for meta in export_metadata.values():
            await meta.writeheader()

        async for i, item in aenumerate(dstream):
            normal_data, table_data = split_data(item)
            change_data = prepare_changelog(normal_data, i)
            await base_export_metadata.writerow(normal_data)
            await changes_export_metadata.writerow(change_data)

            for table in table_data:
                await export_metadata[table.name].write_table_data(table, normal_data)

        for meta in export_metadata.values():
            meta.close()


@commands.before_export.register(Context, Model, PostgreSQL)
def before_export(
        context: Context,
        model: Model,
        backend: PostgreSQL,
        *,
        data: DataSubItem,
        txn: str = "",
):
    patch = take(['_id'], data.patch)
    patch['_revision'] = take('_revision', data.patch, data.saved)
    patch['_txn'] = txn
    patch['_created'] = datetime.datetime.now()
    for prop in take(model.properties).values():
        if not prop.dtype.inherited:
            prop_data = data[prop.name]
            value = commands.before_export(
                context,
                prop.dtype,
                backend,
                data=prop_data,
            )
            patch.update(value)
    return patch


@commands.before_export.register(Context, Geometry, PostgreSQL)
def before_export(
        context: Context,
        dtype: Geometry,
        backend: PostgreSQL,
        *,
        data: DataSubItem
):
    value = data.patch
    if (
            dtype.srid is not None and
            value and
            "SRID" not in value
    ):
        value = f"SRID={dtype.srid};{value}"
    return take({dtype.prop.place: value})


@commands.before_export.register(Context, Array, PostgreSQL)
def before_export(
        context: Context,
        dtype: Array,
        backend: PostgreSQL,
        *,
        data: DataSubItem,
):
    if dtype.prop.list:
        return {}
    else:
        key = dtype.prop.place
        array_data = take({key: data.patch})
        table_data = generate_table_data(dtype, array_data[key])
        return {
            **array_data,
            table_data.name: table_data
        }
