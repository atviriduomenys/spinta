from spinta import commands
from spinta.components import Context
from spinta.manifests.backend.components import BackendManifest
from spinta.manifests.backend.helpers import run_bootstrap


@commands.bootstrap.register()
async def bootstrap(context: Context, manifest: BackendManifest):
    store = manifest.store
    backend = manifest.backend

    commands.load(context, store.internal, into=store.manifest, freezed=True)

    if not backend.bootstrapped(manifest):
        await run_bootstrap(context, manifest)
