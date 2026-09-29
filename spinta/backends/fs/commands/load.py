import pathlib
from typing import Any, Dict

from spinta import commands
from spinta.backends.fs.components import FileSystem
from spinta.components import Context


@commands.load.register(Context, FileSystem, dict)
def load(context: Context, backend: FileSystem, config: Dict[str, Any]):
    backend.path = pathlib.Path(config["path"])
