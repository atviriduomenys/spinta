from typing import Iterator
from typing import overload

from multipledispatch import dispatch

from spinta.typing import ObjectData
from spinta import commands
from spinta.components import Context
from spinta.components import Model
from spinta.components import Property
from spinta.types.datatype import DataType
from spinta.types.datatype import File
from spinta.types.datatype import Object
from spinta.exceptions import NotFoundError, ItemDoesNotExist
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_column_name
from spinta.utils.nestedstruct import flat_dicts_to_nested


@overload
@commands.getone.register(Context, Property, Object, PostgreSQL)
def getone(
    context: Context,
    prop: Property,
    dtype: Object,
    backend: PostgreSQL,
    *,
    id_: str,
) -> ObjectData:
    table = backend.get_table(prop.model)
    connection = context.get('transaction').connection
    selectlist = [
        table.c._id,
        table.c._revision,
    ] + [
        table.c[name]
        for name in _iter_prop_names(prop.dtype)
    ]
    try:
        data = backend.get(connection, selectlist, table.c._id == id_)
    except NotFoundError:
        raise ItemDoesNotExist(prop.model, id=id_)

    result = {
        '_type': prop.model_type(),
        '_id': data[table.c._id],
        '_revision': data[table.c._revision],
    }

    data = flat_dicts_to_nested(data)
    result[prop.name] = data[prop.name]
    return commands.cast_backend_to_python(context, prop, backend, result)


@dispatch((Model, Object))
def _iter_prop_names(dtype) -> Iterator[Property]:
    for prop in dtype.properties.values():
        yield from _iter_prop_names(prop.dtype)


@dispatch(DataType)
def _iter_prop_names(dtype) -> Iterator[Property]:  # noqa
    if not dtype.prop.name.startswith('_'):
        yield get_column_name(dtype.prop)


@dispatch(File)
def _iter_prop_names(dtype) -> Iterator[Property]:  # noqa
    yield dtype.prop.place + '._id'
    yield dtype.prop.place + '._content_type'
