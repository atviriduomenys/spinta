from spinta import commands
from spinta.components import Context
from spinta.manifests.internal_sql.components import InternalSQLManifest


@commands.bootstrap.register(Context, InternalSQLManifest)
def bootstrap(context: Context, manifest: InternalSQLManifest):
    store = context.get('store')
    for backend in store.backends.values():
        commands.bootstrap(context, backend)
