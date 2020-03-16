from typing import AsyncIterator, Optional, Union

import cgi
import pathlib
import shutil

from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import FileResponse

from spinta import commands
from spinta.backends import Backend, simple_data_check
from spinta.commands import load, prepare, migrate, push, getone, wipe, wait, authorize, complex_data_check
from spinta.commands.write import prepare_patch, simple_response, validate_data
from spinta.components import Context, Manifest, Model, Property, Attachment, Action, UrlParams, DataItem
from spinta.core.config import RawConfig
from spinta.exceptions import ConflictingValue, FileNotFound, ItemDoesNotExist
from spinta.renderer import render
from spinta.types.datatype import DataType, File
from spinta.utils.aiotools import aiter
from spinta.utils.schema import NotAvailable, NA


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
        '_id': value.filename,
        '_content_type': value.content_type,
    }


@commands.bootstrap.register()
def bootstrap(context: Context, backend: FileSystem):
    pass


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
        type(context),
        DataItem,
        DataType,
        Property,
        Backend,
        dict,
    ](context, data, dtype, prop, backend, given)
    if isinstance(dtype.prop.backend, FileSystem):
        path = prop.backend.path / given['_id']
        if not path.exists():
            raise FileNotFound(prop, file=given['_id'])


@push.register()
async def push(
    context: Context,
    request: Request,
    dtype: File,
    backend: FileSystem,
    *,
    action: Action,
    params: UrlParams,
):
    prop = dtype.prop

    authorize(context, action, prop)

    data = DataItem(
        prop.model,
        prop,
        propref=False,
        backend=backend,
        action=action
    )
    data.given = {
        prop.name: {
            '_content_type': request.headers.get('content-type'),
            '_id': None,
        }
    }

    if 'revision' in request.headers:
        data.given['_revision'] = request.headers.get('revision')
    if 'content-disposition' in request.headers:
        data.given[prop.name]['_id'] = cgi.parse_header(request.headers['content-disposition'])[1]['filename']
    if 'content-length' not in request.headers:
        raise HTTPException(status_code=411)
    if not data.given[prop.name]['_id']:
        # XXX: Probably here should be a new UUID.
        data.given[prop.name]['_id'] = params.pk

    simple_data_check(context, data, data.prop, data.model.backend)

    data.saved = getone(context, prop, dtype, prop.model.backend, id_=params.pk)

    filepath = backend.path / data.given[prop.name]['_id']

    if action in (Action.UPDATE, Action.PATCH):
        with open(filepath, 'wb') as f:
            async for chunk in request.stream():
                f.write(chunk)
        dstream = aiter([data])
        dstream = validate_data(context, dstream)
        dstream = prepare_patch(context, dstream)
        dstream = commands.update(context, prop, dtype, prop.model.backend, dstream=dstream)
        dstream = commands.create_changelog_entry(
            context, prop.model, prop.model.backend, dstream=dstream,
        )

    elif action == Action.DELETE:
        if filepath.exists():
            filepath.unlink()
        dstream = aiter([data])
        dstream = validate_data(context, dstream)
        dstream = prepare_patch(context, dstream)
        dstream = commands.delete(context, prop, dtype, prop.model.backend, dstream=dstream)
        dstream = commands.create_changelog_entry(
            context, prop.model, prop.model.backend, dstream=dstream,
        )

    else:
        raise Exception(f"Unknown action {action!r}.")

    status_code, response = await simple_response(context, dstream)
    return render(context, request, prop, params, response, status_code=status_code)


@getone.register()
async def getone(
    context: Context,
    request: Request,
    prop: Property,
    dtype: File,
    backend: FileSystem,
    *,
    action: str,
    params: UrlParams,
):
    authorize(context, action, prop)
    data = getone(context, prop, prop.dtype, prop.model.backend, id_=params.pk)
    value = data[prop.name]
    filename = value['_id']
    if filename is None:
        raise ItemDoesNotExist(prop, id=params.pk)
    return FileResponse(
        str(prop.backend.path / filename),
        media_type=value.get('_content_type'),
        headers={
            'Revision': data['_revision'],
            'Content-Disposition': (
                f'attachment; filename="{filename}"'
                if filename else
                'attachment'
            )
        },
    )


@getone.register()
def getone(
    context: Context,
    prop: Property,
    dtype: DataType,
    backend: FileSystem,
    *,
    id_: str,
):
    raise NotImplementedError


@getone.register()
def getone(
    context: Context,
    prop: Property,
    dtype: File,
    backend: FileSystem,
    *,
    id_: str,
):
    data = getone(context, prop, prop.model.backend, id_=id_)
    if data is None:
        raise ItemDoesNotExist(prop, id=id_)
    data = (prop.backend.path / data[prop.name]['_id']).read_bytes()
    return commands.cast_backend_to_python(context, prop, backend, data)


@wipe.register()
def wipe(context: Context, model: Model, backend: FileSystem):
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


@commands.build_data_patch_for_write.register()
def build_data_patch_for_write(
    context: Context,
    dtype: File,
    *,
    given: Optional[dict],
    saved: Optional[dict],
    insert_action: bool = False,
    update_action: bool = False,
) -> Union[dict, NotAvailable]:
    if (insert_action or update_action):
        given = {
            '_id': given.get('_id', None) if given else None,
            '_content_type': given.get('_content_type', None) if given else None,
            '_content': given.get('_content', NA) if given else NA,
        }
    else:
        given = {
            '_id': given.get('_id', NA) if given else NA,
            '_content_type': given.get('_content_type', NA) if given else NA,
            '_content': given.get('_content', NA) if given else NA,
        }
    saved = {
        '_id': saved.get('_id', NA) if saved else NA,
        '_content_type': saved.get('_content_type', NA) if saved else NA,
        '_content': saved.get('_content', NA) if saved else NA,
    }
    given = {
        k: v for k, v in given.items()
        if v != saved[k]
    }
    given = {k: v for k, v in given.items() if v is not NA}
    return given or NA


@complex_data_check.register()
def complex_data_check(
    context: Context,
    data: DataItem,
    dtype: File,
    prop: Property,
    backend: FileSystem,
    value: dict,
):
    # TODO: revision check for files
    if data.action in (Action.UPDATE, Action.PATCH, Action.DELETE):
        for k in ('_type', '_revision'):
            if k in data.given and data.saved[k] != data.given[k]:
                raise ConflictingValue(
                    dtype.prop,
                    given=data.given[k],
                    expected=data.saved[k],
                )
