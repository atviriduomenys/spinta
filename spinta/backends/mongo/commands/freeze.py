from spinta import commands
from spinta.components import Context
from spinta.migrations import SchemaVersion
from spinta.backends.mongo.components import Mongo


@commands.freeze.register()
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: Mongo,
    old: object,
    new: object,
):
    pass
