from typing import Optional, List, Dict, Union

import datetime
import json
import logging
import string
import typing

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine.result import RowProxy

from starlette.requests import Request

from spinta.commands import prepare, check, push, getone, getall, changes, wipe, authorize, is_object_id
from spinta.components import Context, Action, UrlParams, Node, Operator
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
from spinta.exceptions import ManagedProperty
from spinta.utils.response import get_request_data
from spinta.utils.idgen import get_new_id
from spinta.utils.changes import get_patch_changes
from spinta import exceptions
from spinta.types.datatype import String
from spinta.utils.streams import splitlines
from spinta.urlparams import get_model_by_name
from spinta.utils.errors import report_error


log = logging.getLogger(__name__)


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, model: Model):
    name = model.model_type()
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
    if _is_streaming_request(request):
        return await _push_streaming_batch(context, request, model, backend, params=params)
    else:
        authorize(context, action, model)
        return await _push_single_action(context, request, model, backend, action=action, params=params)


async def _group_batch_actions(
    context: Context,
    request: Request,
    scope: Node,
    stop_on_error: bool = True,
    batch_size: int = 1000,
):
    errors = 0
    batch = []
    batch_key = None
    batch_return_key = None
    known_actions = {x.value: x for x in Action}
    transaction = context.get('transaction')
    async for line in splitlines(request.stream()):
        try:
            payload = json.loads(line.strip())
        except json.decoder.JSONDecodeError as e:
            error = exceptions.JSONError(scope, error=str(e), transaction=transaction.id)
            report_error(error, stop_on_error)
            errors += 1
            continue

        # TODO: We need a proper data validation functions, something like that:
        #
        #           validate(payload, {
        #               'type': 'object',
        #               'properties': {
        #                   '_op': {
        #                       'type': 'string',
        #                       'cast': str_to_action,
        #                   }
        #               }
        #           })
        if not isinstance(payload, dict):
            error = exceptions.InvalidValue(scope)
            report_error(error, stop_on_error)
            errors += 1
            continue

        action = payload.get('_op')
        if action not in known_actions:
            error = exceptions.UnknownAction(
                scope,
                action=action,
                supported_actions=list(known_actions.keys())
            )
            report_error(error, stop_on_error)
            errors += 1
            continue
        action = known_actions[action]

        node = payload.get('_type')
        node = get_model_by_name(context, scope.manifest, node)
        if node not in scope:
            error = exceptions.OutOfScope(node, scope=scope)
            report_error(error, stop_on_error)
            errors += 1
            continue

        loop_key = action.value, node.model_type()

        if batch_key is None:
            batch_key = loop_key
            batch_return_key = action, node

        if loop_key != batch_key or len(batch) >= batch_size:
            yield batch_return_key + (errors, batch)
            batch_key = loop_key
            batch_return_key = action, node
            batch = []
            errors = 0

        batch.append(payload)

    if batch:
        yield batch_return_key + (errors, batch)


STREAMING_CONTENT_TYPES = [
    'application/x-jsonlines',
    'application/x-ndjson',
]


def _is_streaming_request(request: Request):
    content_type = request.headers.get('content-type')
    return content_type in STREAMING_CONTENT_TYPES


async def _push_streaming_batch(
    context: Context,
    request: Request,
    scope: Node,
    backend: PostgreSQL,
    *,
    params: UrlParams,
    stop_on_error: bool = True,
):
    if not _is_streaming_request(request):
        raise exceptions.UnknownContentType(
            scope,
            content_type=request.headers.get('content-type'),
            supported_content_types=STREAMING_CONTENT_TYPES,
        )

    authorized: Dict[str, Union[bool, exceptions.BaseError]] = {}

    stats = {
        'errors': 0,
        'insert': 0,
    }
    async for action, node, errors, batch in _group_batch_actions(context, request, scope):
        stats['errors'] += errors

        # Authorization.
        action_and_node = action.value, node.model_type()
        if action_and_node not in authorized:
            try:
                authorize(context, action, node)
            except exceptions.InsufficientScopeError as e:
                authorized[action_and_node] = e
            else:
                authorized[action_and_node] = True
        if authorized[action_and_node] is not True:
            stats['errors'] += len(batch)
            exc = authorized[action_and_node]
            report_error(exc, stop_on_error)
            continue

        # Execute actions.
        if action == Action.INSERT:
            try:
                commands.insert_many(context, node, backend, batch=batch)
            except exceptions.UserError as e:
                stats['errors'] += len(batch)
                report_error(e, stop_on_error)
            else:
                stats['insert'] += len(batch)

        else:
            stats['errors'] += len(batch)
            error = exceptions.UnknownAction(
                node,
                action=action.value,
                supported_actions=[
                    Action.INSERT.value,
                ],
            )
            report_error(error, stop_on_error)

    status_code = 400 if stats['errors'] > 0 else 200
    transaction = context.get('transaction')
    response = {
        'transaction': transaction.id,
        'status': 'error' if stats['errors'] > 0 else 'ok',
        'stats': stats,
    }
    return render(context, request, scope, params, response, status_code=status_code)


async def _push_single_action(
    context: Context,
    request: Request,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    data = await get_request_data(model, request)
    data = commands.load(context, model, data)
    check(context, model, backend, data, action=action, id_=params.id)
    data = prepare(context, model, data, action=action)
    data = _execute_action(context, model, backend, action=action, params=params, data=data)
    data = prepare(context, action, model, backend, data)
    status_code = 201 if action == Action.INSERT else 200
    return render(context, request, model, params, data, action=action, status_code=status_code)


def _execute_action(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
    data: dict,
):
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
        raise ManagedProperty(model, property='revision')
    data['revision'] = get_new_id('revision id')

    data = commands.make_json_serializable(model, data)

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


@commands.insert_many.register()
def insert_many(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    batch: List[dict],
    stop_on_error: bool = True,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = _get_table(backend, model)

    values = []
    changes = []
    for data in batch:
        if 'id' in data:
            id_ = data['id']
        else:
            id_ = commands.gen_object_id(context, backend, model)

        if 'revision' in data.keys():
            error = ManagedProperty(model, property='revision')
            report_error(error, stop_on_error)
        data['revision'] = get_new_id(model.model_type())

        data = commands.make_json_serializable(model, data)
        values.append({
            'id': id_,
            'data': data,
        })
        changes.append({
            'id': id_,
            'change': data,
        })

    qry = table.main.insert().values(
        transaction_id=transaction.id,
        created=utcnow(),
    )
    connection.execute(qry, values)

    qry = table.changes.insert().values(
        transaction_id=transaction.id,
        datetime=utcnow(),
        action=Action.INSERT.value,
    )
    connection.execute(qry, changes)

    return [x['data'] for x in values]


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
            raise ManagedProperty(model, property='revision')
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
        raise exceptions.ItemDoesNotExist(model, id=id_)

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
    return render(context, request, model, params, data, action=action)


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
    data = backend.get(connection, table.main.c.data, table.main.c.id == id_, default=None)
    if data is None:
        raise exceptions.ItemDoesNotExist(model, id=id_)
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
            model = self.model.parent.objects[model.origin][model.properties[ref].dtype.object]
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
        query=params.query,
    )
    return render(context, request, model, params, data, action=action)



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
    query: Optional[List[dict]] = None,
):
    connection = context.get('transaction').connection
    table = _get_table(backend, model).main
    jm = JoinManager(backend, model, table)

    if count:
        qry = sa.select([sa.func.count()]).select_from(table)
        result = connection.execute(qry)
        return {'count': result.scalar()}

    else:
        qry = _getall_show(table, jm, show)
        qry = _getall_query(context, backend, model, qry, jm, query)
        qry = _getall_order_by(qry, table, jm, sort)
        qry = _getall_offset(qry, offset)
        qry = _getall_limit(qry, limit)

        result = connection.execute(qry)

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


def _getall_show(
    table: sa.Table,
    jm: JoinManager,
    show: typing.List[str],
) -> sa.sql.Select:
    if show:
        return sa.select([jm(name).label(name) for name in show])
    else:
        return sa.select([table])


def _getall_query(
    context: Context,
    backend: PostgreSQL,
    model: Model,
    qry: sa.sql.Select,
    jm: JoinManager,
    query: Optional[List[dict]]
) -> sa.sql.Select:
    where = []
    for qp in query or []:
        if qp['key'] not in model.flatprops:
            raise exceptions.FieldNotInResource(model, property=qp['key'])
        prop = model.flatprops[qp['key']]
        operator = qp.get('operator')
        value = commands.load_search_params(context, prop.dtype, backend, qp)
        if operator == Operator.EXACT:
            field = jm(qp['key'])
            if isinstance(prop.dtype, String):
                field = sa.func.lower(field.astext)
                value = value.lower()
            else:
                value = sa.cast(value, JSONB)
            where.append(field == value)
        else:
            raise NotImplementedError(f"Operator {operator!r} is not implemented for postgresql dataset models.")
    if where:
        qry = qry.where(sa.and_(*where))
    return qry


def _getall_order_by(
    qry: sa.sql.Select,
    table: sa.Table,
    jm: JoinManager,
    sort: typing.List[typing.Dict[str, str]],
) -> sa.sql.Select:
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
        return qry.order_by(*db_sort_keys)
    else:
        return qry


def _getall_offset(qry: sa.sql.Select, offset: Optional[int]) -> sa.sql.Select:
    if offset:
        return qry.offset(offset)
    else:
        return qry


def _getall_limit(qry: sa.sql.Select, limit: Optional[int]) -> sa.sql.Select:
    if limit:
        return qry.limit(limit)
    else:
        return qry


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
    data = (
        {
            **row,
            'datetime': row['datetime'].isoformat(),
        }
        for row in data
    )
    return render(context, request, model, params, data, action=action)


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

    qry = sa.select([table]).order_by(table.c.change_id)
    qry = _changes_id(table, qry, id_)
    qry = _changes_offset(table, qry, offset)
    qry = _changes_limit(qry, limit)

    result = connection.execute(qry)

    for row in result:
        yield {
            'change_id': row[table.c.change_id],
            'transaction_id': row[table.c.transaction_id],
            'id': row[table.c.id],
            'datetime': row[table.c.datetime],
            'action': row[table.c.action],
            'change': row[table.c.change],
        }


def _changes_id(table, qry, id_):
    if id_:
        return qry.where(table.c.id == id_)
    else:
        return qry


def _changes_offset(table, qry, offset):
    if offset:
        if offset > 0:
            offset = offset
        else:
            offset = (
                qry.with_only_columns([
                    sa.func.max(table.c.change_id) - abs(offset),
                ]).
                order_by(None).alias()
            )
        return qry.where(table.c.change_id > offset)
    else:
        return qry


def _changes_limit(qry, limit):
    if limit:
        return qry.limit(limit)
    else:
        return qry


@wipe.register()
def wipe(context: Context, model: Model, backend: PostgreSQL):
    authorize(context, Action.WIPE, model)

    connection = context.get('transaction').connection
    table = _get_table(backend, model)
    connection.execute(table.changes.delete())
    connection.execute(table.main.delete())


def _get_table(backend, model):
    return backend.tables[model.manifest.name][model.model_type()]


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
