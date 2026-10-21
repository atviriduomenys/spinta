import pathlib
from typing import Any

from spinta import commands
from spinta.backends.fs.components import FileSystem
from spinta.components import Context


@commands.load.register(Context, FileSystem, dict)
def load(context: Context, backend: FileSystem, config: dict[str, Any]):
    backend.path = pathlib.Path(config["path"])
