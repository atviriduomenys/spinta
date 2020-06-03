from typing import Union

from spinta import commands
from spinta.commands import getall
from spinta.components import Context, Model
from spinta.backends.fs.components import FileSystem
from spinta.types.datatype import File
from spinta.utils.data import take


@commands.wipe.register(Context, File, FileSystem)
def wipe(context: Context, dtype: File, backend: FileSystem):
    rows = getall(context, dtype.prop.model, dtype.prop.model.backend)
    for r in rows:
        fn = take(dtype.prop.place + '._id', r)
        if fn:
            fpath = backend.path / fn
            if fpath.exists():
                fpath.unlink()
