import pathlib

from spinta import commands
from spinta.core.config import RawConfig
from spinta.components import Context
from spinta.backends.fs.components import FileSystem


@commands.load.register(Context, FileSystem, RawConfig)
def load(context: Context, backend: FileSystem, config: RawConfig):
    backend.path = config.get('backends', backend.name, 'path', cast=pathlib.Path, required=True)
