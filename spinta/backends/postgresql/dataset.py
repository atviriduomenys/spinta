from typing import Optional, List

import logging
import string
import typing
import types

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine.result import RowProxy

from starlette.requests import Request

from spinta.commands import prepare, getone, getall, changes, wipe, authorize, is_object_id
from spinta.components import Context, Action, UrlParams, DataStream, DataItem
from spinta.types.dataset import Model, Property
from spinta.backends import Backend
from spinta.backends.postgresql import PostgreSQL
from spinta.backends.postgresql import utcnow
from spinta.backends.postgresql import get_table_name, get_pg_name
from spinta.backends.postgresql import get_changes_table
from spinta.backends.postgresql import TableType
from spinta.renderer import render
from spinta import commands
from spinta import exceptions
from spinta.types.datatype import DataType, String, Binary
from spinta import components
from spinta.utils.refs import get_ref_id
from spinta.utils.json import fix_data_for_json

log = logging.getLogger(__name__)


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, model: Model):
    pkey_type = commands.get_primary_key_type(context, backend)
    table_name = get_pg_name(get_table_name(model))
    table = sa.Table(
        table_name, backend.schema,
        sa.Column('id', sa.String(40), primary_key=True),
        sa.Column('revision', sa.String(40)),
        sa.Column('data', JSONB),
        sa.Column('created', sa.DateTime),
        sa.Column('updated', sa.DateTime, nullable=True),
        sa.Column('transaction', pkey_type, index=True),
    )
    backend.add_table(table, model)
    backend.add_table(
        get_changes_table(context, backend, model),
        model,
        TableType.CHANGELOG,
    )


@commands.insert.register()
async def insert(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: DataStream,
    stop_on_error: bool = True,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = _get_table(backend, model)

    async for data in dstream:
        # TODO: Optimize this and execute one query with multiple data row.
        #
        #       There are multiple issues with this:
        #
        #       - Between data rows there can be errors, we still want to keep
        #         same order while returning data back, so probably we need to
        #         split data chunks by appearing errors.
        #
        #       - If error happens while importing batch, we should yield
        #         successful inserts, bet also report failed attempts to insert,
        #         but I don't know how to do that with batches.
        #
        #       - If errors happened during batch insert, in the changelog we
        #         need to add only successful insert.
        #
        #       So for now, I just insert rows one by one.
        #
        # TODO: Detect when duplicate constraint happens, and then report error
        #       without interrupting transaction if stop_on_error is False.
        qry = table.insert().values(
            transaction=transaction.id,
            created=utcnow(),
        )
        connection.execute(qry, [{
            'id': data.patch['_id'],
            'revision': data.patch['_revision'],
            'data': fix_data_for_json(
                {k: v for k, v in data.patch.items() if not k.startswith('_')},
            )
        }])
        yield data


@commands.update.register()
async def update(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: DataStream,
    stop_on_error: bool = True,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = _get_table(backend, model)
    async for data in dstream:
        if not data.patch:
            yield data
            continue

        values = {
            'revision': data.patch['_revision'],
            'transaction': transaction.id,
            'updated': utcnow(),
            'data': fix_data_for_json({
                # FIXME: Support patching nested properties.
                **{k: v for k, v in data.saved.items() if not k.startswith('_')},
                **{k: v for k, v in data.patch.items() if not k.startswith('_')},
            }),
        }
        if '_id' in data.patch:
            values['id'] = data.patch['_id']
        result = connection.execute(
            table.update().
            where(table.c.id == data.saved['_id']).
            where(table.c.revision == data.saved['_revision']).
            values(values)
        )

        # TODO: Retries are needed if result.rowcount is 0, if such
        #       situation happens, that means a concurrent transaction
        #       changed the data and we need to reread it.
        assert result.rowcount == 1, result.rowcount

        yield data


@commands.delete.register()
def delete(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: DataStream,
    stop_on_error: bool = True,
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
):
    authorize(context, action, model)
    data = getone(context, model, backend, id_=params.pk)
    data = commands.prepare_data_for_response(
        context,
        Action.GETONE,
        model,
        backend,
        data,
        select=params.select,
    )
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
    row = backend.get(connection, table, table.c.id == id_, default=None)
    if row is None:
        raise exceptions.ItemDoesNotExist(model, id=id_)
    return commands.cast_backend_to_python(context, model, backend, {
        '_id': row[table.c.id],
        '_revision': row[table.c.revision],
        '_type': model.model_type(),
        **row[table.c.data],
    })


class JoinManager:
    """
    This class is responsible for keeping track of joins.

    Client can provide field names in a foo.bar.baz form, where baz is the
    column name, and foo.baz are references.

    So in foo.bar.baz example, following joins will be produced:

        FROM table
        LEFT OUTER JOIN foo AS foo_1 ON table.data->>foo = foo_1.id
        LEFT OUTER JOIN bar AS bar_1 ON foo_1.data->>bar = bar_1.id

    Basically for every reference a join is created.

    """

    def __init__(self, context, backend, model, table):
        self.context = context
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
            if ref not in model.properties:
                raise exceptions.UnknownProperty(model, property=ref)
            prop = model.properties[ref]
            model = commands.get_referenced_model(self.context, prop, prop.dtype.object)
            if right_ref not in self.aliases:
                self.aliases[right_ref] = _get_table(self.backend, model).alias()
                left = self.aliases[left_ref]
                right = self.aliases[right_ref]
                self.left = self.left.outerjoin(right, left.c.data[ref].astext == right.c.id)
        if name == '_id':
            return self.aliases[refs].c.id
        elif name == '_revision':
            return self.aliases[refs].c.revision
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
    # select=_params.get('select'),
    # sort=_params.get('sort', [{'name': 'id', 'ascending': True}]),
    # offset=_params.get('offset'),
    # limit=_params.get('limit', 100),
    # count='count' in _params,
    # search=params.search
    # ---
    # context: Context, request: Request, model: Model, backend: PostgreSQL, *,
    # select: typing.List[str] = None,
    # sort: typing.List[typing.Dict[str, str]] = None,
    # offset=None, limit=None,
    # count: bool = False,
    # query_params: typing.List[typing.Dict[str, str]] = None,
    # search: bool = False,

    authorize(context, action, model)
    data = commands.getall(
        context, model, model.backend,
        action=action,
        select=params.select,
        sort=params.sort,
        offset=params.offset,
        limit=params.limit,
        count=params.count,
        query=params.query,
    )
    if not params.count:
        data = (
            commands.prepare_data_for_response(
                context,
                action,
                model,
                backend,
                row,
                select=params.select,
            )
            for row in data
        )
    return render(context, request, model, params, data, action=action)



@getall.register()  # noqa
def getall(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action = Action.GETALL,
    select: typing.List[str] = None,
    sort: typing.Dict[str, dict] = None,
    offset: int = None,
    limit: int = None,
    count: bool = False,
    query: Optional[List[dict]] = None,
):
    connection = context.get('transaction').connection
    table = _get_table(backend, model)
    jm = JoinManager(context, backend, model, table)

    if count:
        qry = sa.select([sa.func.count()]).select_from(table)
        result = connection.execute(qry)
        return [{'count': result.scalar()}]

    else:
        qry = _getall_select(table, jm, select)
        qry = _getall_query(context, backend, model, qry, jm, query)
        qry = _getall_order_by(qry, table, jm, sort)
        qry = _getall_offset(qry, offset)
        qry = _getall_limit(qry, limit)
        qry = qry.select_from(jm.left)

        result = connection.execute(qry)

        if select:
            return (
                commands.cast_backend_to_python(
                    context, model, backend,
                    _flat_dicts_to_nested(row),
                )
                for row in result
            )
        else:
            return (
                commands.cast_backend_to_python(context, model, backend, {
                    '_id': row[table.c.id],
                    '_revision': row[table.c.revision],
                    '_type': model.model_type(),
                    **row[table.c.data],
                })
                for row in result
            )


def _flat_dicts_to_nested(value):
    res = {}
    for k, v in dict(value).items():
        names = k.split('.')
        vref = res
        for name in names[:-1]:
            if name not in vref:
                vref[name] = {}
            vref = vref[name]
        vref[names[-1]] = v
    return res


def _getall_select(
    table: sa.Table,
    jm: JoinManager,
    select: typing.List[str],
) -> sa.sql.Select:
    if select:
        return sa.select([jm(name).label(name) for name in select])
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
        key = qp['args'][0]
        # TODO: Fix RQL parser to support `foo.bar=baz` notation.
        key = '.'.join(key) if isinstance(key, tuple) else key
        if key not in model.flatprops:
            raise exceptions.FieldNotInResource(model, property=key)
        prop = model.flatprops[key]
        name = qp['name']
        value = commands.load_search_params(context, prop.dtype, backend, qp)
        if name == 'eq':
            field = jm(prop.place)
            if key == '_id':
                pass
            elif isinstance(prop.dtype, String):
                field = sa.func.lower(field.astext)
                value = value.lower()
            else:
                value = sa.cast(value, JSONB)
            where.append(field == value)
        else:
            raise exceptions.UnknownOperator(prop, operator=name)
    if where:
        qry = qry.where(sa.and_(*where))
    return qry


def _getall_order_by(
    qry: sa.sql.Select,
    table: sa.Table,
    jm: JoinManager,
    sort: typing.List[typing.Tuple[str, str]],
) -> sa.sql.Select:
    if sort:
        direction = {
            'positive': lambda c: c.asc(),
            'negative': lambda c: c.desc(),
        }
        db_sort_keys = []
        for key in sort:
            # Optional sort direction: sort(+key) or sort(key)
            if isinstance(key, dict) and key['name'] in direction:
                d = direction[key['name']]
                key = key['args'][0]
            else:
                d = direction['positive']
            if key == '_id':
                column = table.c.id
            else:
                column = jm(key)
            column = d(column)
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
    data = changes(
        context,
        model,
        backend,
        id_=params.pk,
        limit=params.limit,
        offset=params.offset,
        start=params.changes_offset,
    )
    return render(context, request, model, params, data, action=action)


@changes.register()
def changes(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    id_: Optional[str] = None,
    limit: int = 100,
    offset: int = -10,
    start: Optional[str] = None,
):
    connection = context.get('transaction').connection
    table = backend.get_table(model, TableType.CHANGELOG)

    qry = sa.select([table]).order_by(table.c._id)
    qry = _changes_id(table, qry, id_)
    qry = _changes_offset(table, qry, offset)
    qry = _changes_limit(qry, limit)

    if start:
        qry = qry.where(table.c._id > start)

    result = connection.execute(qry)

    for row in result:
        yield {
            '_id': row[table.c._id],
            '_rid': row[table.c._rid],
            '_revision': row[table.c._revision],
            '_txn': row[table.c._txn],
            '_created': row[table.c.datetime].isoformat(),
            '_op': row[table.c.action],
            **dict(row[table.c.data]),
        }


def _changes_id(table, qry, id_):
    if id_:
        return qry.where(table.c._rid == id_)
    else:
        return qry


def _changes_offset(table, qry, offset):
    if offset:
        if offset > 0:
            offset = offset
        else:
            offset = (
                qry.with_only_columns([
                    sa.func.max(table.c.change) - abs(offset),
                ]).
                order_by(None).alias()
            )
        return qry.where(table.c.change > offset)
    else:
        return qry


def _changes_limit(qry, limit):
    if limit:
        return qry.limit(limit)
    else:
        return qry


@wipe.register()
def wipe(context: Context, model: Model, backend: PostgreSQL):
    connection = context.get('transaction').connection

    table = backend.get_table(model, TableType.CHANGELOG)
    connection.execute(table.delete())

    table = backend.get_table(model)
    connection.execute(table.delete())


def _get_table(backend, model):
    return backend.get_table(model)


@is_object_id.register()
def is_object_id(context: Context, backend: Backend, model: Model, value: str):
    return len(value) == 40 and not set(value) - set(string.hexdigits)


@commands.prepare_data_for_response.register()
def prepare_data_for_response(
    context: Context,
    action: Action,
    model: Model,
    backend: PostgreSQL,
    value: RowProxy,
    *,
    select: typing.List[str] = None,
) -> dict:
    return commands.prepare_data_for_response(
        context,
        action,
        model,
        backend,
        dict(value),
        select=select,
    )


@commands.prepare_dtype_for_response.register()
def prepare_dtype_for_response(  # noqa
    context: Context,
    backend: PostgreSQL,
    model: Model,
    dtype: Binary,
    value: str,
    *,
    select: dict = None,
):
    return value


@commands.create_changelog_entry.register()
async def create_changelog_entry(
    context: Context,
    node: (Model, Property),
    backend: PostgreSQL,
    *,
    dstream: types.AsyncGeneratorType,
) -> None:
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.get_table(node, TableType.CHANGELOG)
    async for data in dstream:
        qry = table.insert().values(
            _txn=transaction.id,
            datetime=utcnow(),
            action=data.action.value,
        )
        connection.execute(qry, [{
            '_rid': data.saved['_id'] if data.saved else data.patch['_id'],
            '_revision': data.patch['_revision'] if data.patch else data.saved['_revision'],
            '_txn': transaction.id,
            'datetime': utcnow(),
            'action': data.action.value,
            'data': fix_data_for_json(
                {k: v for k, v in data.patch.items() if not k.startswith('_')},
            ),
        }])
        yield data


@commands.check_unique_constraint.register()
def check_unique_constraint(
    context: Context,
    data: DataItem,
    dtype: DataType,
    prop: Property,
    backend: PostgreSQL,
    value: object,
):
    table = _get_table(backend, prop.model)

    if data.action in (Action.UPDATE, Action.PATCH):
        condition = sa.and_(
            table.c.data[prop.name] == sa.cast(value, JSONB),
            table.c._id != data.saved['_id'],
        )
    # PATCH requests are allowed to send protected fields in requests JSON
    # PATCH handling will use those fields for validating data, though
    # won't change them.
    elif data.action == Action.PATCH and dtype.prop.name in {'_id', '_type', '_revision'}:
        return
    else:
        condition = table.c.data[prop.name] == sa.cast(value, JSONB)
    not_found = object()
    connection = context.get('transaction').connection
    result = backend.get(connection, table.c.data[prop.name], condition, default=not_found)
    if result is not not_found:
        raise exceptions.UniqueConstraint(prop)


@commands.gen_object_id.register()
def gen_object_id(context: Context, backend: Backend, model: Model):
    pk = commands.gen_object_id[type(context), Backend, components.Model](
        context, backend, model,
    )
    return get_ref_id(str(pk))
