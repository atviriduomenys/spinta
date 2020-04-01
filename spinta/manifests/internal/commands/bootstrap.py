from spinta import commands
from spinta.components import Context
from spinta.manifests.internal.components import InternalManifest


@commands.bootstrap.register()
def bootstrap(context: Context, manifest: InternalManifest):
    backend = manifest.backend
    if backend.bootstrapped(manifest):
        # Manifest backend is already bootstrapped, do nothing.
        return

    # Bootstrap all backends
    store = context.get('store')
    for backend in store.backends.values():
        commands.bootstrap(context, backend)
