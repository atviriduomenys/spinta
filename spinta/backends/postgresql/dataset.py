import hashlib

import msgpack
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, BIGINT

from spinta.commands import Command

from spinta.backends.postgresql import utcnow
from spinta.backends.postgresql import get_table_name
from spinta.backends.postgresql import MAIN_TABLE
from spinta.backends.postgresql import ModelTables


class Prepare(Command):
    metadata = {
        'name': 'backend.prepare',
        'type': 'dataset.model',
        'backend': 'postgresql',
    }

    def execute(self):
        name = _get_table_name(self)
        table_name = get_table_name(self.backend, self.ns, name, MAIN_TABLE)
        table = sa.Table(
            table_name, self.backend.schema,
            sa.Column('id', BIGINT, primary_key=True),
            sa.Column('key', sa.String(40), index=True),
            sa.Column('data', JSONB),
            sa.Column('created', sa.DateTime),
            sa.Column('transaction_id', sa.Integer, sa.ForeignKey('transaction.id')),
        )
        self.backend.tables[self.ns][name] = ModelTables(table)


class Check(Command):
    metadata = {
        'name': 'check',
        'type': 'dataset.model',
        'backend': 'postgresql',
    }

    def execute(self):
        pass


class Push(Command):
    metadata = {
        'name': 'push',
        'type': 'dataset.model',
        'backend': 'postgresql',
    }

    def execute(self):
        transaction = self.args.transaction
        connection = transaction.connection
        table = _get_table(self)
        data = self.args.data

        if isinstance(data['id'], list):
            key = msgpack.dumps(data['id'], use_bin_type=True)
            key = hashlib.sha1(key).hexdigest()
        else:
            key = data['id']

        connection.execute(
            table.main.insert().values(
                key=key,
                data=data,
                created=utcnow(),
                transaction_id=transaction.id,
            )
        )

        return key


class GetAll(Command):
    metadata = {
        'name': 'getall',
        'type': 'dataset.model',
        'backend': 'postgresql',
    }

    def execute(self):
        connection = self.args.transaction.connection
        table = _get_table(self).main

        agg = (
            sa.select([table.c.key, sa.func.max(table.c.id).label('id')]).
            group_by(table.c.key).
            alias()
        )

        query = (
            sa.select([table]).
            select_from(table.join(agg, table.c.id == agg.c.id))
        )

        result = connection.execute(query)

        for row in result:
            yield row[table.c.data]


class Wipe(Command):
    metadata = {
        'name': 'wipe',
        'type': 'dataset.model',
        'backend': 'postgresql',
    }

    def execute(self):
        connection = self.args.transaction.connection
        table = _get_table(self)
        connection.execute(table.main.delete())


def _get_table(cmd):
    return cmd.backend.tables[cmd.ns][_get_table_name(cmd)]


def _get_table_name(cmd):
    return f'{cmd.obj.name}/:source/{cmd.obj.parent.name}'
