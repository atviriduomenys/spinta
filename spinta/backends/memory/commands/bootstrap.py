from spinta import commands
from spinta.components import Context
from spinta.backends.memory.components import Memory


@commands.bootstrap.register(Context, Memory)
def bootstrap(context: Context, backend: Memory):
    pass
