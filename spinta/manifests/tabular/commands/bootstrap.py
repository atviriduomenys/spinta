from spinta import commands
from spinta.components import Context
from spinta.manifests.tabular.components import TabularManifest


@commands.bootstrap.register(Context, TabularManifest)
def bootstrap(context: Context, manifest: TabularManifest):
    # Tabular manifest can't store state so we always run bootstrap.
    store = context.get('store')
    for backend in store.backends.values():
        commands.bootstrap(context, backend)
