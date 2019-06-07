import datetime
import itertools
import string
import typing

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from spinta.commands import prepare, check, push, get, getall, changes, wipe, authorize, is_object_id
from spinta.components import Context, Action
from spinta.types.dataset import Model

from spinta.backends import Backend, check_model_properties
from spinta.backends.postgresql import PostgreSQL
from spinta.backends.postgresql import utcnow
from spinta.backends.postgresql import get_table_name
from spinta.backends.postgresql import get_changes_table
from spinta.backends.postgresql import MAIN_TABLE, CHANGES_TABLE
from spinta.backends.postgresql import ModelTables
from spinta.utils.refs import get_ref_id


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, model: Model):
    name = model.get_type_value()
    table_name = get_table_name(backend, model.manifest.name, name, MAIN_TABLE)
    table = sa.Table(
        table_name, backend.schema,
        sa.Column('id', sa.String(40), primary_key=True),
        sa.Column('data', JSONB),
        sa.Column('created', sa.DateTime),
        sa.Column('updated', sa.DateTime, nullable=True),
        sa.Column('transaction_id', sa.Integer, sa.ForeignKey('transaction.id')),
    )
    changes_table_name = get_table_name(backend, model.manifest.name, name, CHANGES_TABLE)
    changes_table = get_changes_table(backend, changes_table_name, sa.String(40))
    backend.tables[model.manifest.name][name] = ModelTables(table, changes_table)


@check.register()
def check(context: Context, model: Model, backend: PostgreSQL, data: dict, *, action: Action):
    check_model_properties(context, model, backend, data, action)


@push.register()
def push(context: Context, model: Model, backend: PostgreSQL, data: dict, *, action: Action):
    authorize(context, action, model, data=data)

    transaction = context.get('transaction')
    connection = transaction.connection
    table = _get_table(backend, model)
    data = _serialize(data)
    key = get_ref_id(data.pop('id'))

    values = {
        'data': data,
        'transaction_id': transaction.id,
    }

    row = backend.get(
        connection,
        [table.main.c.data, table.main.c.transaction_id],
        table.main.c.id == key,
        default=None,
    )

    action = None

    # Insert.
    if row is None:
        action = Action.INSERT
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
            action = Action.UPDATE
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
            return None

    # Track changes.
    connection.execute(
        table.changes.insert().values(
            transaction_id=transaction.id,
            id=key,
            datetime=utcnow(),
            action=action.value,
            change=changes,
        ),
    )

    # Sanity check, is primary key was really what we tell it to be?
    assert action != Action.INSERT or result.inserted_primary_key[0] == key, f'{result.inserted_primary_key[0]} == {key}'

    # Sanity check, do we really updated just one row?
    assert action != Action.UPDATE or result.rowcount == 1, result.rowcount

    return {'id': key}


def _serialize(value):
    if isinstance(value, dict):
        return {k: _serialize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_serialize(v) for v in value]
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    return value


@get.register()
def get(context: Context, model: Model, backend: PostgreSQL, id: str):
    authorize(context, Action.GETONE, model)

    connection = context.get('transaction').connection
    table = _get_table(backend, model).main

    query = (
        sa.select([table]).
        where(table.c.id == id)
    )

    result = connection.execute(query)
    result = list(itertools.islice(result, 2))

    if len(result) == 1:
        row = result[0]
        return _get_data_from_row(model, table, row)

    elif len(result) == 0:
        return None

    else:
        context.error(f"Multiple rows were found, id={id}.")


class JoinManager:
    """
    This class is responsible for keeping track of joins.

    Client can provide field names in a foo.bar.baz form, where baz is the
    column name, and foo.baz are references.

    So in foo.bar.baz example, following joins will be produced:

        FROM table
        LEFT OUTER JOIN foo AS foo_1 ON table.data->foo = foo_1.id
        LEFT OUTER JOIN bar AS bar_1 ON foo_1.data->bar = bar_1.id

    Basically for every reference a join is created.

    """

    def __init__(self, backend, model, table):
        self.backend = backend
        self.model = model
        self.left = table
        self.aliases = {(): table}

    def __call__(self, name):
        *refs, name = name.split('.')
        refs = tuple(refs)
        model = self.model
        for i in range(len(refs)):
            ref = refs[i]
            left_ref = refs[:i]
            right_ref = refs[:i] + (ref,)
            model = self.model.parent.objects[model.properties[ref].ref]
            if right_ref not in self.aliases:
                self.aliases[right_ref] = _get_table(self.backend, model).main.alias()
                left = self.aliases[left_ref]
                right = self.aliases[right_ref]
                self.left = self.left.outerjoin(right, left.c.data[ref] == right.c.id)
        if name == 'id':
            return self.aliases[refs].c.id
        else:
            return self.aliases[refs].c.data[name]


@getall.register()
def getall(
    context: Context, model: Model, backend: PostgreSQL, *,
    show: typing.List[str] = None,
    sort: typing.List[typing.Dict[str, str]] = None,
    offset=None, limit=None,
    count: bool = False,
    query_params: typing.List[typing.Dict[str, str]] = None,
):
    if query_params is None:
        query_params = []

    authorize(context, Action.GETALL, model)

    connection = context.get('transaction').connection
    table = _get_table(backend, model).main
    jm = JoinManager(backend, model, table)

    if count:
        query = sa.select([sa.func.count()]).select_from(table)
        result = connection.execute(query)
        yield {'count': result.scalar()}

    else:
        query = sa.select(_getall_show(table, jm, show))
        query = _getall_order_by(query, table, jm, sort)
        query = _getall_offset(query, offset)
        query = _getall_limit(query, limit)

        result = connection.execute(query)

        for row in result:
            yield _get_data_from_row(model, table, row, show=show)


def _getall_show(table: sa.Table, jm: JoinManager, show: typing.List[str]):
    if not show:
        return [table]
    return [jm(name).label(name) for name in show]


def _getall_order_by(query, table: sa.Table, jm: JoinManager, sort: typing.List[typing.Dict[str, str]]):
    if sort:
        db_sort_keys = []
        for sort_key in sort:
            if sort_key['name'] == 'id':
                column = table.c.id
            else:
                column = jm(sort_key['name'])

            if sort_key['ascending']:
                column = column.asc()
            else:
                column = column.desc()

            db_sort_keys.append(column)
        return query.order_by(*db_sort_keys)
    else:
        return query


def _getall_offset(query, offset):
    if offset:
        return query.offset(offset)
    else:
        return query


def _getall_limit(query, limit):
    if limit:
        return query.limit(limit)
    else:
        return query


@changes.register()
def changes(context: Context, model: Model, backend: PostgreSQL, *, id=None, offset=None, limit=None):
    authorize(context, Action.CHANGES, model)

    connection = context.get('transaction').connection
    table = _get_table(backend, model).changes

    query = sa.select([table]).order_by(table.c.change_id)
    query = _changes_id(table, query, id)
    query = _changes_offset(table, query, offset)
    query = _changes_limit(query, limit)

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


def _changes_id(table, query, id):
    if id:
        return query.where(table.c.id == id)
    else:
        return query


def _changes_offset(table, query, offset):
    if offset:
        if offset > 0:
            offset = offset
        else:
            offset = (
                query.with_only_columns([
                    sa.func.max(table.c.change_id) - abs(offset),
                ]).
                order_by(None).alias()
            )
        return query.where(table.c.change_id > offset)
    else:
        return query


def _changes_limit(query, limit):
    if limit:
        return query.limit(limit)
    else:
        return query


@wipe.register()
def wipe(context: Context, model: Model, backend: PostgreSQL):
    authorize(context, Action.WIPE, model)

    connection = context.get('transaction').connection
    table = _get_table(backend, model)
    connection.execute(table.changes.delete())
    connection.execute(table.main.delete())


def _get_table(backend, model):
    return backend.tables[model.manifest.name][model.get_type_value()]


def _get_data_from_row(model: Model, table, row, *, show=False):
    if show:
        data = dict(row)
    else:
        data = {
            'type': model.get_type_value(),
            'id': row[table.c.id],
        }
        for prop in model.properties.values():
            if prop.name not in data:
                data[prop.name] = row[table.c.data].get(prop.name)
    return data


def _get_patch_changes(old, new):
    changes = {}
    for k, v in new.items():
        if old.get(k) != v:
            changes[k] = v
    return changes


@is_object_id.register()
def is_object_id(context: Context, backend: Backend, model: Model, value: str):
    return len(value) == 40 and not set(value) - set(string.hexdigits)
