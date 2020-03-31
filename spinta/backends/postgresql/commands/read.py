from typing import List, Dict

import sqlalchemy as sa

from starlette.requests import Request

from spinta import commands
from spinta.renderer import render
from spinta.components import Context, Model, Property, Action, UrlParams
from spinta.backends import log_getall, log_getone
from spinta.types.datatype import DataType
from spinta.exceptions import NotFoundError, ItemDoesNotExist, UnavailableSubresource
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import flat_dicts_to_nested
from spinta.backends.postgresql.commands.query import QueryBuilder


@commands.getone.register(Context, Request, Model, PostgreSQL)
async def getone(
    context: Context,
    request: Request,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    commands.authorize(context, action, model)
    data = getone(context, model, backend, id_=params.pk)
    hidden_props = [prop.name for prop in model.properties.values() if prop.hidden]
    log_getone(context, data, select=params.select, hidden=hidden_props)
    data = commands.prepare_data_for_response(
        context,
        Action.GETONE,
        model,
        backend,
        data,
        select=params.select,
    )
    return render(context, request, model, params, data, action=action)


@commands.getone.register(Context, Model, PostgreSQL)
def getone(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    id_: str,
):
    connection = context.get('transaction').connection
    table = backend.get_table(model)
    try:
        result = backend.get(connection, table, table.c._id == id_)
    except NotFoundError:
        raise ItemDoesNotExist(model, id=id_)
    data = flat_dicts_to_nested(dict(result))
    data['_type'] = model.model_type()
    return commands.cast_backend_to_python(context, model, backend, data)


@commands.getone.register(Context, Request, Property, DataType, PostgreSQL)
async def getone(
    context: Context,
    request: Request,
    prop: Property,
    dtype: DataType,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    raise UnavailableSubresource(prop=prop.name, prop_type=prop.dtype.name)


@commands.getall.register(Context, Request, Model, PostgreSQL)
async def getall(
    context: Context,
    request: Request,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    commands.authorize(context, action, model)
    rows = getall(
        context, model, backend,
        select=params.select,
        sort=params.sort,
        offset=params.offset,
        limit=params.limit,
        query=params.query,
        count=params.count,
    )
    hidden_props = [prop.name for prop in model.properties.values() if prop.hidden]
    rows = log_getall(context, rows, select=params.select, hidden=hidden_props)
    if not params.count:
        rows = (
            commands.prepare_data_for_response(
                context,
                action,
                model,
                backend,
                row,
                select=params.select,
            )
            for row in rows
        )
    return render(context, request, model, params, rows, action=action)


@commands.getall.register(Context, Model, PostgreSQL)
def getall(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action = Action.GETALL,
    select: List[str] = None,
    sort: Dict[str, dict] = None,
    offset: int = None,
    limit: int = None,
    query: List[Dict[str, str]] = None,
    count: bool = False,
):
    connection = context.get('transaction').connection

    if count:
        table = backend.get_table(model)
        qry = sa.select([sa.func.count()]).select_from(table)
        result = connection.execute(qry)
        yield {'count': result.scalar()}
        return

    qb = QueryBuilder(context, model, backend)
    qry = qb.build(select, sort, offset, limit, query)

    for row in connection.execute(qry):
        row = flat_dicts_to_nested(dict(row))
        row = {
            '_type': model.model_type(),
            **row,
        }
        yield commands.cast_backend_to_python(context, model, backend, row)
