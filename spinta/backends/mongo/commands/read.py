from typing import Iterator
from typing import overload

from spinta.typing import ObjectData
from spinta.typing import FileObjectData
from spinta import commands
from spinta.components import Context
from spinta.components import Model
from spinta.components import Property
from spinta.core.ufuncs import Expr
from spinta.types.datatype import File, Object
from spinta.exceptions import ItemDoesNotExist
from spinta.backends.mongo.components import Mongo
from spinta.backends.mongo.commands.query import MongoQueryBuilder


@overload
@commands.getone.register(Context, Model, Mongo)
def getone(
    context: Context,
    model: Model,
    backend: Mongo,
    *,
    id_: str,
) -> ObjectData:
    table = backend.db[model.model_type()]
    keys = {k: 1 for k in model.flatprops}
    keys['__id'] = 1
    keys['_id'] = 0
    query = {'__id': id_}
    data = table.find_one(query, keys)
    if data is None:
        raise ItemDoesNotExist(model, id=id_)
    data['_id'] = data['__id']
    data['_type'] = model.model_type()
    return commands.cast_backend_to_python(context, model, backend, data)


@overload
@commands.getone.register(Context, Property, Object, Mongo)
def getone(
    context: Context,
    prop: Property,
    dtype: Object,
    backend: Mongo,
    *,
    id_: str,
) -> ObjectData:
    table = backend.db[prop.model.model_type()]
    data = table.find_one({'__id': id_}, {
        '__id': 1,
        '_revision': 1,
        prop.name: 1,
    })
    if data is None:
        raise ItemDoesNotExist(prop, id=id_)
    result = {
        '_id': data['__id'],
        '_revision': data['_revision'],
        '_type': prop.model_type(),
        prop.name: (data.get(prop.name) or {}),
    }
    return commands.cast_backend_to_python(context, prop, backend, result)


@overload
@commands.getone.register(Context, Property, File, Mongo)
def getone(
    context: Context,
    prop: Property,
    dtype: File,
    backend: Mongo,
    *,
    id_: str,
) -> FileObjectData:
    table = backend.db[prop.model.model_type()]
    data = table.find_one({'__id': id_}, {
        '__id': 1,
        '_revision': 1,
        prop.name: 1,
    })
    if data is None:
        raise ItemDoesNotExist(prop, id=id_)
    # merge file property data with defaults
    file_data = {
        **{
            '_content_type': None,
            '_id': None,
            '_size': None,
        },
        **data.get(prop.name, {}),
    }
    result = {
        '_id': data['__id'],
        '_revision': data['_revision'],
        '_type': prop.model_type(),
        prop.name: file_data,
    }
    return commands.cast_backend_to_python(context, prop, backend, result)


@commands.getall.register(Context, Model, Mongo)
def getall(
    context: Context,
    model: Model,
    backend: Mongo,
    *,
    query: Expr = None,
) -> Iterator[ObjectData]:
    builder = MongoQueryBuilder(context)
    builder.update(model=model)
    table = backend.db[model.model_type()]
    env = builder.init(backend, table)
    expr = env.resolve(query)
    where = env.execute(expr)
    cursor = env.build(where)
    for row in cursor:
        if '__id' in row:
            row['_id'] = row.pop('__id')
        row['_type'] = model.model_type()
        yield commands.cast_backend_to_python(context, model, backend, row)
