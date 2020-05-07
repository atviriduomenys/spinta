import cgi
import pathlib
import typing

from starlette.exceptions import HTTPException
from starlette.requests import Request

from spinta import commands
from spinta.components import Context
from spinta.backends.fs.components import FileSystem

from spinta.commands import simple_data_check
from spinta.commands.write import prepare_patch, simple_response, validate_data, log_write
from spinta.components import Action, UrlParams, DataItem
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

    dstream = aiter([data])
    dstream = validate_data(context, dstream)
    dstream = prepare_patch(context, dstream)
    dstream = log_write(context, dstream)
    if action in (Action.UPDATE, Action.PATCH):
        dstream = create_file(filepath, dstream, request.stream())
        dstream = commands.update(context, prop, dtype, prop.model.backend, dstream=dstream)
    elif action == Action.DELETE:
        dstream = commands.delete(context, prop, dtype, prop.model.backend, dstream=dstream)
    else:
        raise Exception(f"Unknown action {action!r}.")
    dstream = commands.create_changelog_entry(
        context, prop.model, prop.model.backend, dstream=dstream,
    )

    status_code, response = await simple_response(context, dstream)
    return render(context, request, prop, params, response, action=action, status_code=status_code)


async def create_file(
    filepath: pathlib.PosixPath,
    dstream: typing.AsyncIterator[DataItem],
    fstream: typing.AsyncIterator[bytes]
) -> typing.AsyncGenerator[DataItem, None]:
    # we know that dstream contains one DataItem element
    async for d in dstream:
        with open(filepath, 'wb') as f:
            async for chunk in fstream:
                f.write(chunk)
        yield d
