from spinta import commands
from spinta.backends.memory.components import Memory
from spinta.components import Context


@commands.wait.register(Context, Memory)
def wait(context: Context, backend: Memory, *, fail: bool = False):
    return True
