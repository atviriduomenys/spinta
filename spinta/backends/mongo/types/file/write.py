from spinta import commands
from spinta.backends.fs.components import FileSystem
from spinta.types.file.helpers import prepare_patch_data
from spinta.utils.data import take
from spinta.types.datatype import File
from spinta.components import Context, DataSubItem, Action
from spinta.backends.mongo.components import Mongo


@commands.before_write.register(Context, File, Mongo)
def before_write(
    context: Context,
    dtype: File,
    backend: Mongo,
    *,
    data: DataSubItem,
):
    content = take('_content', data.patch)
    if isinstance(content, bytes) and isinstance(dtype.backend, FileSystem):
        filepath = dtype.backend.path / take('_id', data.given, data.saved)
        with open(filepath, 'wb') as f:
            f.write(content)

    patch = prepare_patch_data(dtype, data)

    if data.root.action == Action.INSERT:
        return {dtype.prop.place: patch}

    return {
        f'{dtype.prop.place}.{k}': v for k, v in patch.items()
    }
