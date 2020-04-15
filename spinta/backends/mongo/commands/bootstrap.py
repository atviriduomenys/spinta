from spinta import commands
from spinta.components import Context
from spinta.backends.mongo.components import Mongo


@commands.bootstrap.register(Context, Mongo)
def bootstrap(context: Context, backend: Mongo):
    pass
