from spinta.components import Context, Manifest
from spinta.core.config import RawConfig
from spinta import commands
from spinta.nodes import create_node, load_node


class InlineManifest(Manifest):

    def load(self, config):
        self.manifest = config.rc.get(
            'manifests', self.name, 'manifest',
            required=True,
        )

    def read(self, context: Context):
        for name, data in self.manifest.items():
            data = {
                **data,
                'name': name,
            }
            yield data, []


@commands.load.register()
def load(context: Context, manifest: InlineManifest, rc: RawConfig):
    config = context.get('config')
    manifest.load(config)
    for data, versions in manifest.read(context):
        node = create_node(config, manifest, data)
        load_node(context, node, data, manifest)
        node.parent = manifest
        node = load(context, node, data, manifest)
        manifest.objects[node.type][node.name] = node
    return manifest


@commands.bootstrap.register()
def bootstrap(context: Context, manifest: InlineManifest):
    store = context.get('store')
    for backend in store.backends.values():
        commands.bootstrap(context, backend)


@commands.migrate.register()
def migrate(context: Context, manifest: InlineManifest):
    raise NotImplementedError


@commands.sync.register()
def sync(context: Context, manifest: InlineManifest):
    if manifest.sync:
        raise NotImplementedError
