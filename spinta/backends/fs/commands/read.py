from starlette.requests import Request
from starlette.responses import FileResponse

from spinta import commands
from spinta.components import Context, Property, UrlParams
from spinta.backends import log_getone
from spinta.backends.fs.components import FileSystem
from spinta.types.datatype import DataType, File
from spinta.exceptions import ItemDoesNotExist


@commands.getone.register(Context, Request, Property, File, FileSystem)
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
    commands.authorize(context, action, prop)
    data = getone(context, prop, prop.dtype, prop.model.backend, id_=params.pk)
    log_getone(context, data)
    value = data[prop.name]
    filename = value['_id']
    if filename is None:
        raise ItemDoesNotExist(prop, id=params.pk)
    return FileResponse(
        str(dtype.backend.path / filename),
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


@commands.getone.register(Context, Property, DataType, FileSystem)
def getone(  # noqa
    context: Context,
    prop: Property,
    dtype: DataType,
    backend: FileSystem,
    *,
    id_: str,
):
    raise NotImplementedError


@commands.getone.register(Context, Property, File, FileSystem)
def getone(  # noqa
    context: Context,
    prop: Property,
    dtype: File,
    backend: FileSystem,
    *,
    id_: str,
):
    data = commands.getone(context, prop, prop.model.backend, id_=id_)
    if data is None:
        raise ItemDoesNotExist(prop, id=id_)
    data = (dtype.backend.path / data[prop.name]['_id']).read_bytes()
    return commands.cast_backend_to_python(context, prop, backend, data)
