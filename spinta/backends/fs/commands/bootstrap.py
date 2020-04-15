from spinta import commands
from spinta.components import Context
from spinta.backends.fs.components import FileSystem


@commands.bootstrap.register(Context, FileSystem)
def bootstrap(context: Context, backend: FileSystem):
    pass
