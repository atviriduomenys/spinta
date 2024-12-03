import csv
from typing import Any
from multipledispatch import dispatch

from spinta.cli.helpers.export.components import CounterManager
from spinta.components import Model, Property
from spinta.types.datatype import DataType, Array


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
    model: Model
    writer: csv.DictWriter

    def __init__(self, name: str, model: Model, writer: csv.DictWriter):
        self.name = name
        self.model = model
        self.writer = writer

    async def writeheader(self):
        await self.writer.writeheader()

    async def writerow(self, data: dict, counter: CounterManager):
        await self.writer.writerow(data)
        counter.update_specific(self.model, self.name, 1)

    async def write_table_data(self, table_data: TableData, data: dict, counter: CounterManager):
        counter.update_specific(self.model, self.name, 1)
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


@dispatch(DataType)
def extract_extra_fields(dtype: DataType, data: dict) -> dict:
    return {}


@dispatch(Array)
def extract_extra_fields(dtype: Array, data: dict) -> dict:
    return {
        '_txn': data['_txn'],
        '_rid': data['_id']
    }
