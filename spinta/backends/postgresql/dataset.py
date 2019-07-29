import datetime
import string
import typing

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine.result import RowProxy

from starlette.requests import Request
from starlette.exceptions import HTTPException

from spinta.commands import prepare, check, push, getone, getall, changes, wipe, authorize, is_object_id
from spinta.components import Context, Action, UrlParams
from spinta.types.dataset import Model
from spinta.backends import Backend, check_model_properties
from spinta.backends.postgresql import PostgreSQL
from spinta.backends.postgresql import utcnow
from spinta.backends.postgresql import get_table_name
from spinta.backends.postgresql import get_changes_table
from spinta.backends.postgresql import MAIN_TABLE, CHANGES_TABLE
from spinta.backends.postgresql import ModelTables
from spinta.renderer import render
from spinta import commands
from spinta.exceptions import NotFound
from spinta.utils.response import get_request_data
from spinta.utils.idgen import get_new_id
from spinta.utils.changes import get_patch_changes


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
def check(context: Context, model: Model, backend: PostgreSQL, data: dict, *, action: Action, id_: str):
    check_model_properties(context, model, backend, data, action, id_)


@push.register()
async def push(
    context: Context,
    request: Request,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    authorize(context, action, model)

    data = await get_request_data(request)
    data = commands.load(context, model, data)
    check(context, model, backend, data, action=action, id_=params.id)
    data = prepare(context, model, data, action=action)

    if action == Action.INSERT:
        data['id'] = commands.insert(context, model, backend, data=data)

    elif action == Action.UPSERT:
        data['id'] = commands.upsert(context, model, backend, data=data)

    elif action == Action.UPDATE:
        commands.update(context, model, backend, id_=params.id, data=data)

    elif action == Action.PATCH:
        commands.patch(context, model, backend, id_=params.id, data=data)

    elif action == Action.DELETE:
        commands.delete(context, model, backend, id_=params.id)

    else:
        raise Exception(f"Unknown action: {action!r}.")

    data = prepare(context, action, model, backend, data)

    status_code = 201 if action == Action.INSERT else 200
    return render(context, request, model, action, params, data, status_code=status_code)


@commands.insert.register()
def insert(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    data: dict,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = _get_table(backend, model)

    if 'id' in data:
        id_ = data['id']
    else:
        id_ = commands.gen_object_id(context, backend, model)

    if 'revision' in data.keys():
        raise HTTPException(status_code=400, detail="cannot create 'revision'")
    data['revision'] = get_new_id('revision id')

    connection.execute(
        table.main.insert().values({
            'id': id_,
            'transaction_id': transaction.id,
            'created': utcnow(),
            'data': data,
        })
    )

    # Track changes.
    connection.execute(
        table.changes.insert().values(
            transaction_id=transaction.id,
            id=id_,
            datetime=utcnow(),
            action=Action.INSERT.value,
            change=data,
        ),
    )

    return id_


@commands.upsert.register()
def upsert(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    key: typing.List[str],
    data: dict,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = _get_table(backend, model)

    condition = []
    for k in key:
        condition.append(table.main.c[k] == data[k])

    row = backend.get(
        connection,
        [table.main.c.id, table.main.c.data, table.main.c.transaction_id],
        sa.and_(*condition),
        default=None,
    )

    if row is None:
        action = Action.INSERT

        if 'id' in data:
            id_ = data['id']
        else:
            id_ = commands.gen_object_id(context, backend, model)

        if 'revision' in data.keys():
            raise HTTPException(status_code=400, detail="cannot create 'revision'")
        data['revision'] = get_new_id('revision id')

        data = _fix_data_for_json(data)

        connection.execute(
            table.main.insert().values({
                'id': id_,
                'transaction_id': transaction.id,
                'created': utcnow(),
                'data': data,
            })
        )
    else:
        action = Action.PATCH

        id_ = row[table.main.c.id]

        data = _patch(transaction, connection, table, id_, row, data)

        if data is None:
            # Nothing changed.
            return None

        data = _fix_data_for_json(data)

    # Track changes.
    connection.execute(
        table.changes.insert().values(
            transaction_id=transaction.id,
            id=id_,
            datetime=utcnow(),
            action=action.value,
            change={
                k: v for k, v in data.items() if k not in ('id', 'revision')
            },
        ),
    )

    return id_


@commands.update.register()
def update(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    id_: str,
    data: dict,
):
    raise NotImplementedError


@commands.patch.register()
def patch(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    id_: str,
    data: dict,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = _get_table(backend, model)

    row = backend.get(
        connection,
        [table.main.c.data, table.main.c.transaction_id],
        table.main.c.id == id_,
        default=None,
    )
    if row is None:
        type_ = model.get_type_value()
        raise NotFound(f"Object {type_!r} with id {id_!r} not found.")

    data = _patch(transaction, connection, table, id_, row, data)

    if data is None:
        # Nothing changed.
        return None

    # Track changes.
    connection.execute(
        table.changes.insert().values(
            transaction_id=transaction.id,
            id=id_,
            datetime=utcnow(),
            action=Action.PATCH.value,
            change=data,
        ),
    )

    return data


def _patch(transaction, connection, table, id_, row, data):
    changes = get_patch_changes(row[table.main.c.data], data)

    if not changes:
        # Nothing to update.
        return None

    data['revision'] = get_new_id('revision id')

    data = _fix_data_for_json(data)

    result = connection.execute(
        table.main.update().
        where(table.main.c.id == id_).
        where(table.main.c.transaction_id == row[table.main.c.transaction_id]).
        values({
            'id': id_,
            'transaction_id': transaction.id,
            'updated': utcnow(),
            'data': data,
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
    assert result.rowcount > 0

    return changes


@commands.delete.register()
def delete(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    id_: str,
):
    raise NotImplementedError


@getone.register()
async def getone(
    context: Context,
    request: Request,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
    ref: bool = False,
):
    authorize(context, action, model)
    data = getone(context, model, backend, id_=params.id)
    data = prepare(context, Action.GETONE, model, backend, data, show=params.show)
    return render(context, request, model, action, params, data)


@getone.register()
def getone(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    id_: str,
):
    connection = context.get('transaction').connection
    table = _get_table(backend, model)
    data = backend.get(connection, table.main.c.data, table.main.c.id == id_)
    return {**data, 'id': id_}


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
            model = self.model.parent.objects[model.properties[ref].type.object]
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
async def getall(
    context: Context,
    request: Request,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    # show=_params.get('show'),
    # sort=_params.get('sort', [{'name': 'id', 'ascending': True}]),
    # offset=_params.get('offset'),
    # limit=_params.get('limit', 100),
    # count='count' in _params,
    # search=params.search
    # ---
    # context: Context, request: Request, model: Model, backend: PostgreSQL, *,
    # show: typing.List[str] = None,
    # sort: typing.List[typing.Dict[str, str]] = None,
    # offset=None, limit=None,
    # count: bool = False,
    # query_params: typing.List[typing.Dict[str, str]] = None,
    # search: bool = False,

    authorize(context, action, model)
    data = commands.getall(
        context, model, model.backend,
        action=action,
        show=params.show,
        sort=params.sort,
        offset=params.offset,
        limit=params.limit,
        count=params.count,
    )
    return render(context, request, model, action, params, data)



@getall.register()  # noqa
def getall(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action = Action.GETALL,
    show: typing.List[str] = None,
    sort: typing.Dict[str, dict] = None,
    offset: int = None,
    limit: int = None,
    count: bool = False,
):
    connection = context.get('transaction').connection
    table = _get_table(backend, model).main
    jm = JoinManager(backend, model, table)

    if count:
        query = sa.select([sa.func.count()]).select_from(table)
        result = connection.execute(query)
        return {'count': result.scalar()}

    else:
        query = sa.select(_getall_show(table, jm, show))
        query = _getall_order_by(query, table, jm, sort)
        query = _getall_offset(query, offset)
        query = _getall_limit(query, limit)

        result = connection.execute(query)

        if len(jm.aliases) > 1:
            # FIXME: currently `prepare` does not support joins, but that should
            #        be fixed, so for now skipping `prepare`.
            return (dict(row) for row in result)

        if show:
            return (
                prepare(context, action, model, backend, dict(row), show=show)
                for row in result
            )
        else:
            return (
                prepare(
                    context, action, model, backend,
                    {**row[table.c.data], 'id': row[table.c.id]},
                    show=show,
                )
                for row in result
            )


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
async def changes(
    context: Context,
    request: Request,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    authorize(context, action, model)
    data = changes(context, model, backend, id_=params.id, limit=params.limit, offset=params.offset)
    return render(context, request, model, action, params, (
        {
            **row,
            'datetime': row['datetime'].isoformat(),
        }
        for row in data
    ))


@changes.register()
def changes(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    id_: str = None,
    limit: int = 100,
    offset: int = -10,
):
    connection = context.get('transaction').connection
    table = _get_table(backend, model).changes

    query = sa.select([table]).order_by(table.c.change_id)
    query = _changes_id(table, query, id_)
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


def _changes_id(table, query, id_):
    if id_:
        return query.where(table.c.id == id_)
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


@is_object_id.register()
def is_object_id(context: Context, backend: Backend, model: Model, value: str):
    return len(value) == 40 and not set(value) - set(string.hexdigits)


@prepare.register()
def prepare(context: Context, action: Action, model: Model, backend: PostgreSQL, value: RowProxy, *, show: typing.List[str] = None) -> dict:
    return prepare(context, action, model, backend, dict(value), show=show)


def _fix_data_for_json(data):
    # XXX: a temporary workaround
    #
    #      Dataset data is stored in a JSONB column and has to be converted
    #      into JSON friendly types.
    _data = {}
    for k, v in data.items():
        if isinstance(v, (datetime.datetime, datetime.date)):
            v = v.isoformat()
        _data[k] = v
    return _data
