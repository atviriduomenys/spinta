from spinta import commands
from spinta.types.datatype import Object
from spinta.components import Context, DataSubItem, Action
from spinta.backends.mongo.components import Mongo
from spinta.utils.schema import NA


@commands.before_write.register(Context, Object, Mongo)
def before_write(
    context: Context,
    dtype: Object,
    backend: Mongo,
    *,
    data: DataSubItem,
) -> dict:
    patch = {}
    if data.root.action == Action.INSERT or (data.root.action == Action.UPSERT and data.saved is NA):
        return {dtype.prop.place: data.patch}

    for prop in dtype.properties.values():
        value = commands.before_write(
            context,
            prop.dtype,
            backend,
            data=data[prop.name],
        )
        patch.update(value)
    return patch
