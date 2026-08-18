from spinta import commands
from spinta.backends.fs.components import FileSystem
from spinta.components import Context


@commands.wait.register(Context, FileSystem)
def wait(context: Context, backend: FileSystem, *, fail: bool = False) -> bool:
    return backend.path.exists()
