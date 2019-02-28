import datetime
import itertools

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, BIGINT

from spinta.commands import Command

from spinta.backends.postgresql import utcnow
from spinta.backends.postgresql import get_table_name
from spinta.backends.postgresql import MAIN_TABLE
from spinta.backends.postgresql import ModelTables
from spinta.utils.refs import get_ref_id


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
        data = self.serialize(self.args.data)
        key = get_ref_id(data['id'])

        connection.execute(
            table.main.insert().values(
                key=key,
                data=data,
                created=utcnow(),
                transaction_id=transaction.id,
            )
        )

        return key

    def serialize(self, value):
        if isinstance(value, dict):
            return {k: self.serialize(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self.serialize(v) for v in value]
        if isinstance(value, datetime.datetime):
            return value.isoformat()
        return value


class Get(Command):
    metadata = {
        'name': 'get',
        'type': 'dataset.model',
        'backend': 'postgresql',
    }

    def execute(self):
        connection = self.args.transaction.connection
        table = _get_table(self).main

        agg = (
            sa.select([table.c.key, sa.func.max(table.c.id).label('id')]).
            where(table.c.key == self.args.id).
            group_by(table.c.key).
            alias()
        )

        query = (
            sa.select([table]).
            select_from(table.join(agg, table.c.id == agg.c.id))
        )

        result = connection.execute(query)
        result = list(itertools.islice(result, 2))

        if len(result) == 1:
            row = result[0]
            return {
                **row[table.c.data],
                'id': row[table.c.key],
                'type': _get_table_name(self),
            }

        elif len(result) == 0:
            return None

        else:
            self.error(f"Multiple rows were found, key={self.args.id}.")


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

        query = self.order_by(query, table)
        query = self.limit(query)
        query = self.offset(query)

        result = connection.execute(query)

        for row in result:
            yield {
                **row[table.c.data],
                'id': row[table.c.key],
                'type': _get_table_name(self),
            }

    def order_by(self, query, table):
        if self.args.sort:
            db_sort_keys = []
            for sort_key in self.args.sort:
                if sort_key['name'] == 'id':
                    column = table.c.id
                else:
                    column = table.c.data[sort_key['name']]

                if sort_key['ascending']:
                    column = column.asc()
                else:
                    column = column.desc()

                db_sort_keys.append(column)
            return query.order_by(*db_sort_keys)
        else:
            return query

    def limit(self, query):
        if self.args.limit:
            return query.limit(self.args.limit)
        else:
            return query

    def offset(self, query):
        if self.args.offset:
            return query.offset(self.args.offset)
        else:
            return query


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
