from spinta import commands
from spinta.backends.components import Backend
from spinta.backends.fs.components import FileSystem
from spinta.components import Attachment, Context
from spinta.manifests.components import Manifest
from spinta.types.datatype import File


@commands.prepare.register(Context, FileSystem, Manifest)
def prepare(context: Context, backend: FileSystem, manifest: Manifest, **kwargs):
    pass


@commands.prepare.register(Context, FileSystem, File)
def prepare(context: Context, backend: FileSystem, dtype: File, **kwargs):
    pass


@commands.prepare_for_write.register(Context, File, Backend, Attachment)
def prepare_for_write(
    context: Context,
    dtype: File,
    backend: Backend,
    value: Attachment,
):
    return {
        "_id": value.filename,
        "_content_type": value.content_type,
    }
