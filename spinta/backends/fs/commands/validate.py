import pathlib
import os

from spinta import commands
from spinta.components import Context, Action, Property, DataItem
from spinta.backends.components import Backend
from spinta.types.datatype import DataType, File
from spinta.backends.fs.components import FileSystem
from spinta.exceptions import FileNotFound, ConflictingValue, UnacceptableFileName


@commands.simple_data_check.register(Context, DataItem, File, Property, FileSystem, dict)
def simple_data_check(
    context: Context,
    data: DataItem,
    dtype: File,
    prop: Property,
    backend: FileSystem,
    value: dict,
):
    if value['_id'] is not None:
        # Check if given filepath stays on backend.path.
        _validate_path(value['_id'], backend, dtype)


@commands.complex_data_check.register()
def complex_data_check(
    context: Context,
    data: DataItem,
    dtype: File,
    prop: Property,
    backend: Backend,
    given: dict,
):
    complex_data_check[
        type(context),
        DataItem,
        DataType,
        Property,
        Backend,
        dict,
    ](context, data, dtype, prop, backend, given)
    if isinstance(dtype.backend, FileSystem):
        _validate_path(given['_id'], dtype.backend, dtype)
        path = dtype.backend.path / given['_id']
        if '_content' not in given and not path.exists():
            raise FileNotFound(prop, file=given['_id'])


@commands.complex_data_check.register()
def complex_data_check(
    context: Context,
    data: DataItem,
    dtype: File,
    prop: Property,
    backend: FileSystem,
    value: dict,
):
    # TODO: revision check for files
    if data.action in (Action.UPDATE, Action.PATCH, Action.DELETE):
        for k in ('_type', '_revision'):
            if k in data.given and data.saved[k] != data.given[k]:
                raise ConflictingValue(
                    dtype.prop,
                    given=data.given[k],
                    expected=data.saved[k],
                )
        if value.get('_id'):
            if isinstance(dtype.backend, FileSystem):
                filename = pathlib.PosixPath(value['_id'])
                _validate_path(filename, dtype.backend, dtype)


def _validate_path(filename: pathlib.PosixPath(), fs: FileSystem, dtype: File):
    commonpath = os.path.commonpath([
        fs.path.resolve(),
        (fs.path / filename).resolve(),
    ])
    if str(commonpath) != str(fs.path.resolve()):
        raise UnacceptableFileName(dtype, file=filename)
