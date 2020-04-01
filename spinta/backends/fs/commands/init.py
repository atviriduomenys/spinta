from spinta import commands
from spinta.components import Context, Attachment
from spinta.manifests.components import Manifest
from spinta.backends.components import Backend
from spinta.types.datatype import File
from spinta.backends.fs.components import FileSystem


@commands.prepare.register()
def prepare(context: Context, backend: FileSystem, manifest: Manifest):
    pass


@commands.prepare.register()
def prepare(context: Context, backend: FileSystem, dtype: File):
    pass


@commands.prepare.register()
def prepare(context: Context, dtype: File, backend: Backend, value: Attachment):
    return {
        '_id': value.filename,
        '_content_type': value.content_type,
    }
