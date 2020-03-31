from spinta import commands
from spinta.core.config import RawConfig
from spinta.components import Context
from spinta.backends.fs.components import FileSystem


@commands.wait.register()
def wait(context: Context, backend: FileSystem, config: RawConfig, *, fail: bool = False) -> bool:
    return backend.path.exists()
