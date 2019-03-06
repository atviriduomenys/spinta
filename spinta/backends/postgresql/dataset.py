import datetime
import itertools

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from spinta.commands import Command

from spinta.backends.postgresql import utcnow
from spinta.backends.postgresql import get_table_name
from spinta.backends.postgresql import get_changes_table
from spinta.backends.postgresql import MAIN_TABLE, CHANGES_TABLE
from spinta.backends.postgresql import INSERT_ACTION, UPDATE_ACTION
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
            sa.Column('id', sa.String(40), primary_key=True),
            sa.Column('data', JSONB),
            sa.Column('created', sa.DateTime),
            sa.Column('updated', sa.DateTime, nullable=True),
            sa.Column('transaction_id', sa.Integer, sa.ForeignKey('transaction.id')),
        )
        changes_table_name = get_table_name(self.backend, self.ns, name, CHANGES_TABLE)
        changes_table = get_changes_table(self.backend, self.obj, self.ns, changes_table_name, sa.String(40))
        self.backend.tables[self.ns][name] = ModelTables(table, changes_table)


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
        key = get_ref_id(data.pop('id'))

        values = {
            'data': data,
            'transaction_id': transaction.id,
        }

        row = self.backend.get(
            connection,
            [table.main.c.data, table.main.c.transaction_id],
            table.main.c.id == key,
            default=None,
        )

        action = None

        # Insert.
        if row is None:
            action = INSERT_ACTION
            result = connection.execute(
                table.main.insert().values({
                    'id': key,
                    'created': utcnow(),
                    **values,
                })
            )
            changes = data

        # Update.
        else:
            changes = _get_patch_changes(row[table.main.c.data], data)

            if changes:
                action = UPDATE_ACTION
                result = connection.execute(
                    table.main.update().
                    where(table.main.c.id == key).
                    where(table.main.c.transaction_id == row[table.main.c.transaction_id]).
                    values({
                        **values,
                        'updated': utcnow(),
                    })
                )

                # TODO: Retries are needed if result.rowcount is 0, if such
                #       situation happens, that means a concurrent transaction
                #       changed the data and we need to reread it.
                #
                #       And assumption is made here, than in the same
                #       transaction there are no concurrent updates, if this
                #       assumption is false, then we need to check against
                #       change_id instead of transaction_id.

            else:
                # Nothing to update.
                return key

        # Track changes.
        connection.execute(
            table.changes.insert().values(
                transaction_id=transaction.id,
                id=key,
                datetime=utcnow(),
                action=action,
                change=changes,
            ),
        )

        # Sanity check, is primary key was really what we tell it to be?
        assert action != INSERT_ACTION or result.inserted_primary_key[0] == key, f'{result.inserted_primary_key[0]} == {key}'

        # Sanity check, do we really updated just one row?
        assert action != UPDATE_ACTION or result.rowcount == 1, result.rowcount

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

        query = (
            sa.select([table]).
            where(table.c.id == self.args.id)
        )

        result = connection.execute(query)
        result = list(itertools.islice(result, 2))

        if len(result) == 1:
            row = result[0]
            return _get_data_from_row(self, table, row)

        elif len(result) == 0:
            return None

        else:
            self.error(f"Multiple rows were found, id={self.args.id}.")


class GetAll(Command):
    metadata = {
        'name': 'getall',
        'type': 'dataset.model',
        'backend': 'postgresql',
    }

    def execute(self):
        connection = self.args.transaction.connection
        table = _get_table(self).main

        query = sa.select([table])
        query = self.order_by(query, table)
        query = self.offset(query)
        query = self.limit(query)

        result = connection.execute(query)

        for row in result:
            yield _get_data_from_row(self, table, row)

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

    def offset(self, query):
        if self.args.offset:
            return query.offset(self.args.offset)
        else:
            return query

    def limit(self, query):
        if self.args.limit:
            return query.limit(self.args.limit)
        else:
            return query


class Changes(Command):
    metadata = {
        'name': 'changes',
        'type': 'dataset.model',
        'backend': 'postgresql',
    }

    def execute(self):
        connection = self.args.transaction.connection
        table = _get_table(self).changes

        query = sa.select([table]).order_by(table.c.change_id)
        query = self.id(table, query)
        query = self.offset(table, query)
        query = self.limit(query)

        result = connection.execute(query)

        for row in result:
            yield {
                'change_id': row[table.c.change_id],
                'transaction_id': row[table.c.transaction_id],
                'id': row[table.c.id],
                'datetime': row[table.c.datetime],
                'action': row[table.c.action],
                'change': row[table.c.change],
            }

    def id(self, table, query):
        if self.args.id:
            return query.where(table.c.id == self.args.id)
        else:
            return query

    def offset(self, table, query):
        if self.args.offset:
            return query.where(table.c.change_id > self.args.offset)
        else:
            return query

    def limit(self, query):
        if self.args.limit:
            return query.limit(self.args.limit)
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
        connection.execute(table.changes.delete())
        connection.execute(table.main.delete())


def _get_table(cmd):
    return cmd.backend.tables[cmd.ns][_get_table_name(cmd)]


def _get_table_name(cmd):
    return f'{cmd.obj.name}/:source/{cmd.obj.parent.name}'


def _get_data_from_row(cmd, table, row):
    row = {
        **row[table.c.data],
        'id': row[table.c.id],
    }
    data = {}
    for prop in cmd.obj.properties.values():
        data[prop.name] = row.get(prop.name)
    data['type'] = _get_table_name(cmd)
    return data


def _get_patch_changes(old, new):
    changes = {}
    for k, v in new.items():
        if old.get(k) != v:
            changes[k] = v
    return changes
