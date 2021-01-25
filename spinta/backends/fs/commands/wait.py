from spinta import commands
from spinta.components import Context
from spinta.backends.fs.components import FileSystem


@commands.wait.register(Context, FileSystem)
def wait(context: Context, backend: FileSystem, *, fail: bool = False) -> bool:
    return backend.path.exists()
