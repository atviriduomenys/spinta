from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest


@commands.prepare.register(Context, Manifest)
def prepare(context: Context, manifest: Manifest):
    store = context.get('store')
    for backend in store.backends.values():
        prepare(context, backend, manifest)
