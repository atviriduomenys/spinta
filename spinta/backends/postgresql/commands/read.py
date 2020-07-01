from starlette.requests import Request

from spinta import commands
from spinta.compat import urlparams_to_expr
from spinta.core.ufuncs import Expr
from spinta.renderer import render
from spinta.components import Context, Model, Property, Action, UrlParams
from spinta.backends import log_getall, log_getone
from spinta.types.datatype import DataType
from spinta.exceptions import NotFoundError, ItemDoesNotExist, UnavailableSubresource
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import flat_dicts_to_nested
from spinta.backends.postgresql.commands.query import PgQueryBuilder


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
    expr = urlparams_to_expr(params)
    rows = getall(context, model, backend, query=expr)
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
    query: Expr = None,
):
    assert isinstance(query, (Expr, type(None))), query
    connection = context.get('transaction').connection

    builder = PgQueryBuilder(context)
    builder.update(model=model)
    table = backend.get_table(model)
    env = builder.init(backend, table)
    expr = env.resolve(query)
    where = env.execute(expr)
    qry = env.build(where)

    for row in connection.execute(qry):
        row = flat_dicts_to_nested(dict(row))
        row = {
            '_type': model.model_type(),
            **row,
        }
        row = commands.cast_backend_to_python(context, model, backend, row)
        yield row
