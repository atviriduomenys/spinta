from spinta import commands
from spinta.backends.memory.components import Memory
from spinta.components import Context


@commands.bootstrap.register(Context, Memory)
def bootstrap(context: Context, backend: Memory):
    pass
