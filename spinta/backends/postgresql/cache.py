import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from spinta.commands import prepare, push, get, dump
from spinta.types.dataset import Dataset
from spinta.backends.postgresql import get_table_name
from spinta.backends.postgresql import CACHE_TABLE
from spinta.backends.postgresql import ModelTables
from spinta.backends.postgresql import PostgreSQL
from spinta.components import Context, Cache


@prepare.register()
def prepare(context: Context, cache: Cache, dataset: Dataset, backend: PostgreSQL):
    name = _get_table_name(dataset)
    table_name = get_table_name(backend, dataset.manifest.name, name, CACHE_TABLE)
    table = sa.Table(
        table_name, backend.schema,
        sa.Column('id', sa.String(40), primary_key=True),
        sa.Column('data', JSONB),
        sa.Column('created', sa.DateTime),
        sa.Column('updated', sa.DateTime, nullable=True),
        sa.Column('transaction_id', sa.Integer, sa.ForeignKey('transaction.id')),
    )
    backend.tables[dataset.manifest.name][name] = ModelTables(cache=table)


@push.register()
def push(context: Context, cache: Cache, dataset: Dataset, backend: PostgreSQL, data: dict):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = _get_table(dataset, backend)
    data = dump(data)


@get.register()
def get(context: Context, cache: Cache, dataset: Dataset, backend: PostgreSQL, id: str):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = _get_table(dataset, backend)


def _get_table_name(dataset: Dataset):
    return f':source/{dataset.name}'


def _get_table(dataset: Dataset, backend: PostgreSQL):
    return backend.tables[dataset.manifest.name][_get_table_name(dataset)]
