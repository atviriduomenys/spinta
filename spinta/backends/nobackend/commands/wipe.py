from spinta import commands
from spinta.backends.nobackend.components import NoBackend
from spinta.components import Context, Model


@commands.wipe.register(Context, Model, NoBackend)
def wipe(context: Context, model: Model, backend: NoBackend):
    pass
