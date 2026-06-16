from spinta import commands
from spinta.backends.fs.components import FileSystem
from spinta.components import Context
from spinta.manifests.components import Manifest


@commands.migrate.register(Context, Manifest, FileSystem)
def migrate(context: Context, manifest: Manifest, backend: FileSystem):
    pass
