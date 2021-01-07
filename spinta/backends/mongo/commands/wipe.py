from spinta import commands
from spinta.components import Context, Model
from spinta.backends.mongo.components import Mongo
from spinta.types.datatype import DataType


@commands.wipe.register(Context, Model, Mongo)
def wipe(context: Context, model: Model, backend: Mongo):
    for prop in model.properties.values():
        wipe(context, prop.dtype, backend)

    table_main = backend.db[model.model_type()]
    table_changelog = backend.db[model.model_type() + '__changelog']
    table_main.delete_many({})
    table_changelog.delete_many({})


@commands.wipe.register(Context, DataType, Mongo)
def wipe(context: Context, dtype: DataType, backend: Mongo):
    same_backend = backend.name == dtype.backend.name

    if not same_backend:
        wipe(context, dtype, dtype.backend)
