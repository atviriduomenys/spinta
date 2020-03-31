import shutil

from spinta import commands
from spinta.components import Context, Model
from spinta.backends.fs.components import FileSystem


@commands.wipe.register()
def wipe(context: Context, model: Model, backend: FileSystem):
    for path in backend.path.iterdir():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
