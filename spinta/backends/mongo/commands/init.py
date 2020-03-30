from spinta import commands
from spinta.components import Context, Manifest
from spinta.backends.mongo.components import Mongo


@commands.prepare.register()
def prepare(context: Context, backend: Mongo, manifest: Manifest):
    # Mongo does not need any table or database preparations
    pass
