from spinta import commands
from spinta.components import Context
from spinta.manifests.internal.components import InternalManifest


@commands.migrate.register()
def migrate(context: Context, manifest: InternalManifest):
    store = context.get('store')
    for backend in store.backends.values():
        migrate(context, backend)
