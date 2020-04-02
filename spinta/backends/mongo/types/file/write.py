from spinta import commands
from spinta.backends.fs.components import FileSystem
from spinta.utils.data import take
from spinta.types.datatype import File
from spinta.components import Context, DataSubItem
from spinta.backends.components import Backend
from spinta.backends.mongo.components import Mongo


@commands.before_write.register(Context, File, Mongo)
def before_write(  # noqa
    context: Context,
    dtype: File,
    backend: Mongo,
    *,
    data: DataSubItem,
):
    content = take('_content', data.patch)
    if isinstance(content, bytes) and isinstance(dtype.backend, FileSystem):
        filepath = dtype.backend.path / data.given['_id']
        with open(filepath, 'wb') as f:
            f.write(content)

    return commands.before_write[type(context), File, Backend](
        context,
        dtype,
        backend,
        data=data,
    )
