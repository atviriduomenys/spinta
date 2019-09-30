import cgi
import pathlib
import shutil

from starlette.requests import Request
from starlette.responses import FileResponse

from spinta.backends import Backend
from spinta.commands import load, prepare, migrate, check, push, getone, wipe, wait, authorize
from spinta.components import Context, Manifest, Model, Property, Attachment, Action, UrlParams
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


@check.register()
def check(context: Context, dtype: File, prop: Property, backend: FileSystem, value: dict, *, data: dict, action: Action):
    path = backend.path / value['filename']
    if not path.exists():
        raise FileNotFound(prop, file=value['filename'])


@check.register()
def check(context: Context, dtype: File, prop: Property, backend: Backend, value: dict, *, data: dict, action: Action):
    if prop.backend.name != backend.name:
        check(context, dtype, prop, prop.backend, value, data=data, action=action)


@push.register()
async def push(
    context: Context,
    request: Request,
    prop: Property,
    backend: FileSystem,
    *,
    action: str,
    params: UrlParams,
    ref: bool = False,
):
    authorize(context, action, prop)

    data = getone(context, prop, prop.model.backend, id_=params.id)

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
            filename = data['filename'] or params.id
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
        commands.update(context, prop, prop.model.backend, id_=params.id, data=update)

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
    ref: bool = False,
):
    authorize(context, action, prop)
    data = getone(context, prop, prop.model.backend, id_=params.id)
    if data is None:
        raise ItemDoesNotExist(prop, id=params.id)
    filename = data['filename'] or params.id
    return FileResponse(prop.backend.path / filename, media_type=data['content_type'])


@wipe.register()
def wipe(context: Context, model: Model, backend: FileSystem):
    authorize(context, 'wipe', model)
    for path in backend.path.iterdir():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
