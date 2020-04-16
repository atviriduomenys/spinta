from typing import Iterator

from multipledispatch import dispatch

from starlette.requests import Request

from spinta import commands
from spinta.renderer import render
from spinta.components import Context, Model, Property, Action, UrlParams
from spinta.backends import log_getone
from spinta.types.datatype import DataType, File, Object
from spinta.exceptions import NotFoundError, ItemDoesNotExist
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_column_name, flat_dicts_to_nested


@commands.getone.register(Context, Request, Property, Object, PostgreSQL)
async def getone(
    context: Context,
    request: Request,
    prop: Property,
    dtype: Object,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    commands.authorize(context, action, prop)
    data = getone(context, prop, dtype, backend, id_=params.pk)
    log_getone(context, data)
    data = commands.prepare_data_for_response(
        context,
        Action.GETONE,
        prop.dtype,
        backend,
        data,
    )
    return render(context, request, prop, params, data, action=action)


@commands.getone.register(Context, Property, Object, PostgreSQL)
def getone(
    context: Context,
    prop: Property,
    dtype: Object,
    backend: PostgreSQL,
    *,
    id_: str,
):
    table = backend.get_table(prop.model)
    connection = context.get('transaction').connection
    selectlist = [
        table.c._id,
        table.c._revision,
    ] + [
        table.c[name]
        for name in _iter_prop_names(prop.dtype)
    ]
    try:
        data = backend.get(connection, selectlist, table.c._id == id_)
    except NotFoundError:
        raise ItemDoesNotExist(prop.model, id=id_)

    result = {
        '_type': prop.model_type(),
        '_id': data[table.c._id],
        '_revision': data[table.c._revision],
    }

    data = flat_dicts_to_nested(data)
    result[prop.name] = data[prop.name]
    return commands.cast_backend_to_python(context, prop, backend, result)


@dispatch((Model, Object))
def _iter_prop_names(dtype) -> Iterator[Property]:
    for prop in dtype.properties.values():
        yield from _iter_prop_names(prop.dtype)


@dispatch(DataType)
def _iter_prop_names(dtype) -> Iterator[Property]:  # noqa
    if not dtype.prop.name.startswith('_'):
        yield get_column_name(dtype.prop)


@dispatch(File)
def _iter_prop_names(dtype) -> Iterator[Property]:  # noqa
    yield dtype.prop.place + '._id'
    yield dtype.prop.place + '._content_type'
