from spinta import commands
from spinta.components import Context
from spinta.manifests.yaml.components import YamlManifest


@commands.bootstrap.register(Context, YamlManifest)
def bootstrap(context: Context, manifest: YamlManifest):
    # Yaml manifest can't store state so we always run bootstrap.
    store = context.get('store')
    for backend in store.backends.values():
        commands.bootstrap(context, backend)
