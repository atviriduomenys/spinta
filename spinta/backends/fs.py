from typing import AsyncIterator

import cgi
import pathlib
import shutil

from starlette.requests import Request
from starlette.responses import FileResponse

from spinta.backends import Backend
from spinta.commands import load, prepare, migrate, push, getone, wipe, wait, authorize, complex_data_check
from spinta.components import Context, Manifest, Model, Property, Attachment, Action, UrlParams, DataItem
from spinta.config import RawConfig
from spinta.types.datatype import DataType, File
from spinta.exceptions import FileNotFound, ItemDoesNotExist
from spinta import commands
from spinta.renderer import render
from spinta.commands.write import prepare_patch, simple_response
from spinta.utils.aiotools import aiter


class FileSystem(Backend):
    pass


@load.register()
def load(context: Context, backend: FileSystem, config: RawConfig):
    backend.path = config.get('backends', backend.name, 'path', cast=pathlib.Path, required=True)


@wait.register()
def wait(context: Context, backend: FileSystem, config: RawConfig, *, fail: bool = False):
    return backend.path.exists()


@prepare.register()
def prepare(context: Context, backend: FileSystem, manifest: Manifest):
    pass


@prepare.register()
def prepare(context: Context, backend: FileSystem, dtype: File):
    pass


@prepare.register()
def prepare(context: Context, dtype: File, backend: Backend, value: Attachment):
    return {
        'filename': value.filename,
        'content_type': value.content_type,
    }


@migrate.register()
def migrate(context: Context, backend: FileSystem):
    pass


@complex_data_check.register()
def complex_data_check(
    context: Context,
    data: DataItem,
    dtype: File,
    prop: Property,
    backend: Backend,
    given: dict,
):
    complex_data_check[
        context.__class__,
        DataItem,
        DataType,
        Property,
        Backend,
        dict,
    ](context, data, dtype, prop, backend, given)
    path = prop.backend.path / given['filename']
    if not path.exists():
        raise FileNotFound(prop, file=given['filename'])


@push.register()
async def push(
    context: Context,
    request: Request,
    prop: Property,
    backend: FileSystem,
    *,
    action: str,
    params: UrlParams,
):
    authorize(context, action, prop)

    data = DataItem(prop.model, prop, propref=True, backend=prop.model.backend)
    data.saved = getone(context, prop, prop.model.backend, id_=params.pk)
    data.given = {
        'content_type': request.headers.get('content-type'),
        'filename': None,
    }

    if 'content-disposition' in request.headers:
        data.given['filename'] = cgi.parse_header(request.headers['content-disposition'])[1]['filename']

    if not data.given['filename']:
        data.given['filename'] = params.pk

    filepath = backend.path / data.given['filename']

    if action in (Action.UPDATE, Action.PATCH):
        with open(filepath, 'wb') as f:
            async for chunk in request.stream():
                f.write(chunk)
        dstream = aiter([data])
        dstream = prepare_patch(context, dstream)
        dstream = commands.update(context, prop, prop.dtype, prop.model.backend, dstream=dstream)

    elif action == Action.DELETE:
        if filepath.exists():
            filepath.unlink()
        dstream = aiter([data])
        dstream = prepare_patch(context, dstream)
        dstream = commands.delete(context, prop, prop.dtype, prop.model.backend, dstream=dstream)

    else:
        raise Exception(f"Unknown action {action!r}.")

    status_code, response = await simple_response(context, dstream)
    return render(context, request, prop, params, response, status_code=status_code)


@getone.register()
async def getone(
    context: Context,
    request: Request,
    prop: Property,
    dtype: DataType,
    backend: FileSystem,
    *,
    action: str,
    params: UrlParams,
):
    authorize(context, action, prop)
    data = getone(context, prop, prop.model.backend, id_=params.pk)
    data = data[prop.name]
    if data is None:
        raise ItemDoesNotExist(prop, id=params.pk)
    filename = data['filename'] or params.pk
    return FileResponse(prop.backend.path / filename, media_type=data['content_type'])


@getone.register()
def getone(
    context: Context,
    prop: Property,
    dtype: DataType,
    backend: FileSystem,
    *,
    id_: str,
):
    data = getone(context, prop, prop.model.backend, id_=id_)
    if data is None:
        raise ItemDoesNotExist(prop, id=id_)
    data = data[prop.name]
    filename = data['filename'] or id_
    return (prop.backend.path / filename).read_bytes()


@wipe.register()
def wipe(context: Context, model: Model, backend: FileSystem):
    authorize(context, 'wipe', model)
    for path in backend.path.iterdir():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


@commands.create_changelog_entry.register()
def create_changelog_entry(
    context: Context,
    model: Property,
    backend: FileSystem,
    *,
    dstream: AsyncIterator[DataItem],
) -> AsyncIterator[DataItem]:
    # FileSystem properties don't have change log, change log entry will be
    # created on property model backend.
    return dstream
