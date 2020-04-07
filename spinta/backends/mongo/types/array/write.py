from spinta import commands
from spinta.utils.schema import NA
from spinta.utils.data import take
from spinta.types.datatype import Array
from spinta.components import Context, Action, DataSubItem
from spinta.backends.mongo.components import Mongo


@commands.before_write.register(Context, Array, Mongo)
def before_write(
    context: Context,
    dtype: Array,
    backend: Mongo,
    *,
    data: DataSubItem,
):
    if data.root.action == Action.INSERT or (data.root.action == Action.UPSERT and data.saved is NA):
        return {dtype.prop.place: data.patch}
    else:
        if dtype.prop.list:
            return {}
        else:
            return take({dtype.prop.place: data.patch})
