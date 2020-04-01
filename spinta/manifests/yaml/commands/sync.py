from spinta import commands
from spinta.components import Context
from spinta.manifests.yaml.components import YamlManifest


@commands.sync.register()
def sync(context: Context, manifest: YamlManifest):
    if manifest.sync:
        # TODO: sync YAML files from other manifests
        raise NotImplementedError
