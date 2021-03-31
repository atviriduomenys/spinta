from spinta import commands
from spinta.components import Context, Attachment
from spinta.manifests.components import Manifest
from spinta.backends.components import Backend
from spinta.types.datatype import File
from spinta.backends.fs.components import FileSystem


@commands.prepare.register(Context, FileSystem, Manifest)
def prepare(context: Context, backend: FileSystem, manifest: Manifest):
    pass


@commands.prepare.register(Context, FileSystem, File)
def prepare(context: Context, backend: FileSystem, dtype: File):
    pass


@commands.prepare_for_write.register(Context, File, Backend, Attachment)
def prepare_for_write(
    context: Context,
    dtype: File,
    backend: Backend,
    value: Attachment,
):
    return {
        '_id': value.filename,
        '_content_type': value.content_type,
    }
