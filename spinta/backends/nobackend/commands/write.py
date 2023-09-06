from typing import AsyncIterator

from starlette.requests import Request

from spinta import commands
from spinta.types.datatype import File, DataType
from spinta.backends.helpers import get_table_name
from spinta.components import Context, Model, DataItem, Action, UrlParams, DataSubItem
from spinta.backends.nobackend.components import NoBackend
from spinta.exceptions import BackendNotGiven


@commands.insert.register(Context, Model, NoBackend)
async def insert(
    context: Context,
    model: Model,
    backend: NoBackend,
    *,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
):
    table = get_table_name(model)
    raise BackendNotGiven(table)


@commands.update.register(Context, Model, NoBackend)
async def update(
    context: Context,
    model: Model,
    backend: NoBackend,
    *,
    dstream: dict,
    stop_on_error: bool = True,
):
    table = get_table_name(model)
    raise BackendNotGiven(table)


@commands.delete.register(Context, Model, NoBackend)
async def delete(
    context: Context,
    model: Model,
    backend: NoBackend,
    *,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
):
    table = get_table_name(model)
    raise BackendNotGiven(table)


@commands.push.register(Context, Request, File, NoBackend)
async def push(
    context: Context,
    request: Request,
    dtype: File,
    backend: NoBackend,
    *,
    action: Action,
    params: UrlParams,
):
    prop = dtype.prop
    raise BackendNotGiven(model=prop.model.model_type())
