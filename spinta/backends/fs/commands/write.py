import cgi
import pathlib
import typing

from starlette.exceptions import HTTPException
from starlette.requests import Request

from spinta import commands
from spinta.renderer import render
from spinta.utils.aiotools import aiter
from spinta.components import Context, Action, UrlParams, DataItem
from spinta.commands.write import prepare_patch, simple_response, validate_data
from spinta.types.datatype import File
from spinta.backends.fs.components import FileSystem


@commands.push.register()
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

    commands.authorize(context, action, prop)

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

    commands.simple_data_check(context, data, data.prop, data.model.backend)

    data.saved = commands.getone(context, prop, dtype, prop.model.backend, id_=params.pk)

    filepath = backend.path / data.given[prop.name]['_id']

    if action in (Action.UPDATE, Action.PATCH):
        dstream = aiter([data])
        dstream = validate_data(context, dstream)
        dstream = before_write(context, prop.model, backend, filepath, dstream, request.stream())
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


async def before_write(
    context: Context,
    model: File,
    backend: FileSystem,
    filepath: pathlib.PosixPath,
    dstream: typing.AsyncIterator[DataItem],
    fstream: typing.AsyncIterator[bytes]
) -> typing.AsyncGenerator[DataItem, DataItem]:
    async for d in dstream:
        yield d

    with open(filepath, 'wb') as f:
        async for chunk in fstream:
            f.write(chunk)
