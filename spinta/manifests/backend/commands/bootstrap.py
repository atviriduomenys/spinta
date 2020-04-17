import asyncio

from spinta import commands
from spinta.components import Context
from spinta.manifests.backend.components import BackendManifest
from spinta.manifests.backend.helpers import run_bootstrap


@commands.bootstrap.register(Context, BackendManifest)
def bootstrap(context: Context, manifest: BackendManifest):
    backend = manifest.backend
    if not backend.bootstrapped():
        with context:
            context.attach('transaction', manifest.backend.transaction, write=True)
            loop = asyncio.get_event_loop()
            loop.run_until_complete(run_bootstrap(context, manifest))
