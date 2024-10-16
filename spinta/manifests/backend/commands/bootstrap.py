import asyncio

from spinta import commands
from spinta.backends.helpers import validate_and_return_transaction
from spinta.components import Context
from spinta.manifests.backend.components import BackendManifest
from spinta.manifests.backend.helpers import run_bootstrap


@commands.bootstrap.register(Context, BackendManifest)
def bootstrap(context: Context, manifest: BackendManifest):
    backend = manifest.backend
    if not backend.bootstrapped():
        with context:
            context.attach('transaction', validate_and_return_transaction, context, manifest.backend, write=True)
            loop = asyncio.get_event_loop()
            loop.run_until_complete(run_bootstrap(context, manifest))
