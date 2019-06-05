import cgi
import pathlib
import shutil

from starlette.requests import Request

from spinta.backends import Backend, Action
from spinta.commands import load, prepare, migrate, check, push, get, wipe, wait, authorize
from spinta.components import Context, Manifest, Model, Property, Attachment
from spinta.config import RawConfig
from spinta.types.type import File


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
def prepare(context: Context, backend: FileSystem, type: File):
    pass


@migrate.register()
def migrate(context: Context, backend: FileSystem):
    pass


@check.register()
def check(context: Context, model: Model, backend: FileSystem, data: dict):
    pass


@load.register()
async def load(context: Context, prop: Property, backend: FileSystem, request: Request) -> Attachment:
    return Attachment(
        # FIXME: see [fs_content_type_param]
        content_type=request.headers['content-type'],
        filename=cgi.parse_header(request.headers['content-disposition'])[1]['filename'],
        data=await request.body(),
    )


@push.register()
def push(context: Context, prop: Property, backend: FileSystem, attachment: Attachment, *, action: str):
    path = backend.path / attachment.filename
    if action in (Action.INSERT, Action.UPDATE):
        with open(path, 'wb') as f:
            f.write(attachment.data)
    elif action == Action.DELETE:
        if path.exists():
            path.unlink()
    else:
        raise Exception(f"Unknown action {action!r}.")


@get.register()
def get(context: Context, prop: Property, backend: FileSystem, filename: str):
    with open(backend.path / filename) as f:
        data = f.read()
    return data


@wipe.register()
def wipe(context: Context, model: Model, backend: FileSystem):
    authorize(context, 'wipe', model)
    for path in backend.path.iterdir():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
