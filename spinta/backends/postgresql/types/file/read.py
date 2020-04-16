from starlette.requests import Request
from starlette.responses import Response

from spinta import commands
from spinta.renderer import render
from spinta.components import Context, Property, Action, UrlParams
from spinta.backends.components import BackendFeatures
from spinta.backends import log_getone
from spinta.backends.postgresql.files import DatabaseFile
from spinta.utils.data import take
from spinta.types.datatype import File
from spinta.exceptions import NotFoundError, ItemDoesNotExist
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.constants import TableType
from spinta.backends.postgresql.helpers import flat_dicts_to_nested


@commands.getone.register(Context, Request, Property, File, PostgreSQL)
async def getone(
    context: Context,
    request: Request,
    prop: Property,
    dtype: File,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    commands.authorize(context, action, prop)
    data = getone(context, prop, dtype, backend, id_=params.pk)

    # Return file metadata
    if params.propref:
        log_getone(context, data)
        data = commands.prepare_data_for_response(
            context,
            Action.GETONE,
            prop.dtype,
            backend,
            data,
        )
        return render(context, request, prop, params, data, action=action)

    # Return file content
    else:
        value = take(prop.place, data)

        if not take('_blocks', value):
            raise ItemDoesNotExist(dtype, id=params.pk)

        filename = value['_id']

        connection = context.get('transaction').connection
        table = backend.get_table(prop, TableType.FILE)
        with DatabaseFile(
            connection,
            table,
            value['_size'],
            value['_blocks'],
            value['_bsize'],
            mode='r',
        ) as f:
            content = f.read()

        return Response(
            content,
            media_type=value['_content_type'],
            headers={
                'Revision': data['_revision'],
                'Content-Disposition': (
                    f'attachment; filename="{filename}"'
                    if filename else
                    'attachment'
                )
            },
        )


@commands.getone.register(Context, Property, File, PostgreSQL)
def getone(
    context: Context,
    prop: Property,
    dtype: File,
    backend: PostgreSQL,
    *,
    id_: str,
):
    table = backend.get_table(prop.model)
    connection = context.get('transaction').connection
    selectlist = [
        table.c._id,
        table.c._revision,
        table.c[prop.place + '._id'],
        table.c[prop.place + '._content_type'],
        table.c[prop.place + '._size'],
    ]

    if BackendFeatures.FILE_BLOCKS in prop.dtype.backend.features:
        selectlist += [
            table.c[prop.place + '._bsize'],
            table.c[prop.place + '._blocks'],
        ]

    try:
        data = backend.get(connection, selectlist, table.c._id == id_)
    except NotFoundError:
        raise ItemDoesNotExist(dtype, id=id_)

    result = {
        '_type': prop.model_type(),
        '_id': data[table.c._id],
        '_revision': data[table.c._revision],
    }

    data = flat_dicts_to_nested(data)
    result[prop.name] = data[prop.name]
    return commands.cast_backend_to_python(context, prop, backend, result)


@commands.getfile.register()
def getfile(
    context: Context,
    prop: Property,
    dtype: File,
    backend: PostgreSQL,
    *,
    data: dict,
):
    if not data['_blocks']:
        return None

    if len(data['_blocks']) > 1:
        # TODO: Use propper UserError exception.
        raise Exception(
            "File content is to large to retrun it inline. Try accessing "
            "this file directly using subresources API."
        )

    connection = context.get('transaction').connection
    table = backend.get_table(prop, TableType.FILE)
    with DatabaseFile(
        connection,
        table,
        data['_size'],
        data['_blocks'],
        data['_bsize'],
        mode='r',
    ) as f:
        return f.read()
