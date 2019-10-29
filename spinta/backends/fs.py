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
from spinta.types.datatype import File
from spinta.exceptions import FileNotFound, ItemDoesNotExist
from spinta import commands
from spinta.renderer import render


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
def complex_data_check(context: Context, data: DataItem, dtype: File, prop: Property, backend: FileSystem, value: dict):
    path = backend.path / value['filename']
    if not path.exists():
        raise FileNotFound(prop, file=value['filename'])


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

    data = getone(context, prop, prop.model.backend, id_=params.pk)

    content_type = None
    filename = None

    if 'content-type' in request.headers:
        content_type = request.headers['content-type']

    if 'content-disposition' in request.headers:
        filename = cgi.parse_header(request.headers['content-disposition'])[1]['filename']

    if content_type is None or filename is None:
        data = data or {}
        data.setdefault('content_type', None)
        data.setdefault('filename', None)

        if content_type is None:
            content_type = data['content_type']

        if filename is None:
            filename = data['filename'] or params.pk
    else:
        data = {
            'content_type': None,
            'filename': None,
        }

    filepath = backend.path / filename

    if action in (Action.UPDATE, Action.PATCH):
        with open(filepath, 'wb') as f:
            async for chunk in request.stream():
                f.write(chunk)
        update = {
            'content_type': content_type,
            'filename': filename,
        }
    elif action == Action.DELETE:
        if filepath.exists():
            filepath.unlink()
        update = None
    else:
        raise Exception(f"Unknown action {action!r}.")

    if data != update:
        commands.update(context, prop, prop.model.backend, id_=params.pk, data=update)

    return render(context, request, prop, params, update, action=action)


@getone.register()
async def getone(
    context: Context,
    request: Request,
    prop: Property,
    backend: FileSystem,
    *,
    action: str,
    params: UrlParams,
):
    authorize(context, action, prop)
    data = getone(context, prop, prop.model.backend, id_=params.pk)
    if data is None:
        raise ItemDoesNotExist(prop, id=params.pk)
    filename = data['filename'] or params.pk
    return FileResponse(prop.backend.path / filename, media_type=data['content_type'])


@getone.register()
async def getone(
    context: Context,
    prop: Property,
    backend: FileSystem,
    *,
    id_: str,
):
    data = getone(context, prop, prop.model.backend, id_=id_)
    if data is None:
        raise ItemDoesNotExist(prop, id=id_)
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
