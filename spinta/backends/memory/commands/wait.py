from spinta import commands
from spinta.components import Context
from spinta.backends.memory.components import Memory


@commands.wait.register(Context, Memory)
def wait(context: Context, backend: Memory, *, fail: bool = False):
    return True
