from spinta import commands
from spinta.components import Context
from spinta.backends.memory.components import Memory


@commands.wait.register()
def wait(context: Context, backend: Memory, *, fail: bool = False):
    return True
