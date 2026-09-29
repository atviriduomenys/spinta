from spinta import commands
from spinta.backends.memory.components import Memory
from spinta.components import Context, Model


@commands.wipe.register(Context, Model, Memory)
def wipe(context: Context, model: Model, backend: Memory):
    if model.name in backend.data:
        backend.data[model.name] = {}
