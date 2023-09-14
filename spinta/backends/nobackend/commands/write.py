from typing import AsyncIterator

from starlette.requests import Request

from spinta import commands
from spinta.types.datatype import File
from spinta.components import Context, Model, DataItem, Action, UrlParams
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
    raise BackendNotGiven(model)


@commands.update.register(Context, Model, NoBackend)
async def update(
    context: Context,
    model: Model,
    backend: NoBackend,
    *,
    dstream: dict,
    stop_on_error: bool = True,
):
    raise BackendNotGiven(model)


@commands.delete.register(Context, Model, NoBackend)
async def delete(
    context: Context,
    model: Model,
    backend: NoBackend,
    *,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
):
    raise BackendNotGiven(model)


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
    raise BackendNotGiven(prop.model)
