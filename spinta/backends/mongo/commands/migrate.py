from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.backends.mongo.components import Mongo


@commands.migrate.register(Context, Manifest, Mongo)
def migrate(context: Context, manifest: Manifest, backend: Mongo):
    pass
