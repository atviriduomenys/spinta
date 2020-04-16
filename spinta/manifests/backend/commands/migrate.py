from spinta import commands
from spinta.components import Context
from spinta.manifests.backend.components import BackendManifest
from spinta.manifests.backend.helpers import run_bootstrap
from spinta.manifests.backend.helpers import run_migrations


@commands.migrate.register()
async def migrate(context: Context, manifest: BackendManifest):
    store = manifest.store
    backend = manifest.backend

    commands.load(context, store.internal, into=manifest, freezed=True)
    commands.link(context, manifest)
    commands.prepare(context, manifest)

    if backend.bootstrapped(manifest):
        await run_migrations(context, manifest)
    else:
        await run_bootstrap(context, manifest)
