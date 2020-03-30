from spinta import commands
from spinta.components import Context
from spinta.backends.fs.components import FileSystem


@commands.migrate.register()
def migrate(context: Context, backend: FileSystem):
    pass
