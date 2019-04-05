import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from spinta.commands import Command
from spinta.types.dataset import Dataset
from spinta.backends.postgresql import get_table_name
from spinta.backends.postgresql import CACHE_TABLE
from spinta.backends.postgresql import ModelTables
from spinta.backends.postgresql import PostgreSQL


class Cache:

    def __init__(self):
        pass

    def exists(self, key):
        pass


class Prepare(Command):
    metadata = {
        'name': 'cache.prepare',
        'components': {
            'dataset': Dataset,
            'backend': PostgreSQL,
        },
    }

    def execute(self):
        name = _get_table_name(self.dataset)
        table_name = get_table_name(self.backend, self.ns, name, CACHE_TABLE)
        table = sa.Table(
            table_name, self.backend.schema,
            sa.Column('id', sa.String(40), primary_key=True),
            sa.Column('data', JSONB),
            sa.Column('created', sa.DateTime),
            sa.Column('updated', sa.DateTime, nullable=True),
            sa.Column('transaction_id', sa.Integer, sa.ForeignKey('transaction.id')),
        )
        self.backend.tables[self.ns][name] = ModelTables(cache=table)


class Put(Command):
    metadata = {
        'name': 'cache.put',
        'components': {
            'dataset': Dataset,
            'backend': PostgreSQL,
        },
    }

    def execute(self):
        transaction = self.args.transaction
        connection = transaction.connection
        table = _get_table(self)
        data = self.serialize(self.args.data)


class Get(Command):
    metadata = {
        'name': 'cache.get',
        'components': {
            'dataset': Dataset,
            'backend': PostgreSQL,
        },
    }

    def execute(self):
        transaction = self.args.transaction
        connection = transaction.connection
        table = _get_table(self)
        data = self.serialize(self.args.data)


def _get_table_name(dataset: Dataset):
    return f':source/{dataset.name}'


def _get_table(cmd):
    return cmd.backend.tables[cmd.ns][_get_table_name(cmd.dataset)]
