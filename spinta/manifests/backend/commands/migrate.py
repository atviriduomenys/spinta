from spinta import commands
from spinta.components import Context
from spinta.manifests.backend.components import BackendManifest
from spinta.manifests.backend.helpers import run_bootstrap
from spinta.manifests.backend.helpers import run_migrations


@commands.migrate.register(Context, BackendManifest)
async def migrate(context: Context, manifest: BackendManifest):
    context.attach('transaction', manifest.backend.transaction, write=True)
    if manifest.backend.bootstrapped():
        await run_migrations(context, manifest)
    else:
        await run_bootstrap(context, manifest)
