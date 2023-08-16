from spinta import commands
from spinta.components import Context
from spinta.components import Model
from spinta.backends.nobackend.components import NoBackend


@commands.wipe.register(Context, Model, NoBackend)
def wipe(context: Context, model: Model, backend: NoBackend):
    pass
