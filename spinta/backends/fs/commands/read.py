from pathlib import Path
from typing import Optional, overload

from spinta import commands
from spinta.backends.fs.components import FileSystem
from spinta.components import Context, Property
from spinta.exceptions import ItemDoesNotExist
from spinta.types.datatype import DataType, File
from spinta.typing import FileObjectData, ObjectData


@overload
@commands.getone.register(Context, Property, DataType, FileSystem)
def getone(
    context: Context,
    prop: Property,
    dtype: DataType,
    backend: FileSystem,
    *,
    id_: str,
) -> ObjectData:
    raise NotImplementedError


@overload
@commands.getone.register(Context, Property, File, FileSystem)
def getone(
    context: Context,
    prop: Property,
    dtype: File,
    backend: FileSystem,
    *,
    id_: str,
) -> FileObjectData:
    data = commands.getone(context, prop, dtype, prop.model.backend, id_=id_)
    if data is None:
        raise ItemDoesNotExist(prop, id=id_)
    data = (dtype.backend.path / data[prop.name]["_id"]).read_bytes()
    return commands.cast_backend_to_python(context, prop, backend, data)


@commands.getfile.register()
def getfile(
    context: Context,
    prop: Property,
    dtype: File,
    backend: FileSystem,
    *,
    data: FileObjectData,
) -> Optional[Path]:
    filename = data["_id"]
    if filename is None:
        return None
    else:
        return dtype.backend.path / filename
