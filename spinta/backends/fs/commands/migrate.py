from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.backends.fs.components import FileSystem


@commands.migrate.register(Context, Manifest, FileSystem)
def migrate(context: Context, manifest: Manifest, backend: FileSystem):
    pass
