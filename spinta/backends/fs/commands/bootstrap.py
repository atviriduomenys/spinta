from spinta import commands
from spinta.backends.fs.components import FileSystem
from spinta.components import Context


@commands.bootstrap.register(Context, FileSystem)
def bootstrap(context: Context, backend: FileSystem):
    pass
