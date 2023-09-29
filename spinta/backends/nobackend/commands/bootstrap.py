from spinta import commands
from spinta.backends.nobackend.components import NoBackend
from spinta.components import Context


@commands.bootstrap.register(Context, NoBackend)
def bootstrap(context: Context, backend: NoBackend):
    pass
