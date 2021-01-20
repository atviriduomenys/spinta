import pathlib
from typing import Dict, Any

from spinta import commands
from spinta.components import Context
from spinta.backends.fs.components import FileSystem


@commands.load.register(Context, FileSystem, dict)
def load(context: Context, backend: FileSystem, config: Dict[str, Any]):
    backend.path = pathlib.Path(config['path'])
