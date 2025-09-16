from spinta import commands
from spinta.components import Context
from spinta.backends.nobackend.components import NoBackend


@commands.wait.register(Context, NoBackend)
def wait(context: Context, backend: NoBackend, *, fail: bool = False):
    return True
