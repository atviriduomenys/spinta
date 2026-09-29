from spinta import commands
from spinta.backends.nobackend.components import NoBackend
from spinta.components import Context


@commands.wait.register(Context, NoBackend)
def wait(context: Context, backend: NoBackend, *, fail: bool = False):
    return True
