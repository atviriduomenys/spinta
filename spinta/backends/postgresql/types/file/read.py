from typing import Optional
from typing import overload

from spinta.typing import FileObjectData
from spinta import commands
from spinta.components import Context, Property
from spinta.backends.components import BackendFeatures
from spinta.backends.postgresql.files import DatabaseFile
from spinta.types.datatype import File
from spinta.exceptions import NotFoundError, ItemDoesNotExist
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.utils.nestedstruct import flat_dicts_to_nested


@overload
@commands.getone.register(Context, Property, File, PostgreSQL)
def getone(
    context: Context,
    prop: Property,
    dtype: File,
    backend: PostgreSQL,
    *,
    id_: str,
) -> FileObjectData:
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
    data: FileObjectData,
) -> Optional[bytes]:
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
