from spinta import commands
from spinta.components import Context
from spinta.backends.mongo.components import Mongo


@commands.migrate.register()
def migrate(context: Context, backend: Mongo):
    pass
