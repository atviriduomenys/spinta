from spinta import commands
from spinta.components import Context
from spinta.migrations import SchemaVersion
from spinta.backends.mongo.components import Mongo


@commands.freeze.register(Context, SchemaVersion, Mongo, object, object)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: Mongo,
    old: object,
    new: object,
):
    pass
