from spinta import commands
from spinta.backends.mongo.components import Mongo
from spinta.components import Context


@commands.bootstrap.register(Context, Mongo)
def bootstrap(context: Context, backend: Mongo):
    pass
