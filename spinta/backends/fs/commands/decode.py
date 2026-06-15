import pathlib

from spinta import commands
from spinta.backends.fs.components import FileSystem
from spinta.components import Context
from spinta.formats.components import Format
from spinta.types.datatype import File


@commands.decode.register(Context, Format, FileSystem, File, dict)
def decode(context: Context, source: Format, target: FileSystem, dtype: File, value: dict):
    if value.get("_id"):
        value["_id"] = pathlib.Path(value["_id"])
    return value
