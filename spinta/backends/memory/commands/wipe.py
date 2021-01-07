from spinta import commands
from spinta.components import Context
from spinta.components import Model
from spinta.backends.memory.components import Memory


@commands.wipe.register(Context, Model, Memory)
def wipe(context: Context, model: Model, backend: Memory):
    if model.name in backend.data:
        backend.data[model.name] = {}
