from spinta import commands
from spinta.components import Context
from spinta.manifests.rdf.components import RdfManifest


@commands.bootstrap.register(Context, RdfManifest)
def bootstrap(context: Context, manifest: RdfManifest):
    store = context.get('store')
    for backend in store.backends.values():
        commands.bootstrap(context, backend)
