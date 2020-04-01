import logging

from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.yaml.components import YamlManifest
from spinta.manifests.helpers import load_manifest_node

log = logging.getLogger(__name__)


@commands.load.register()
def load(context: Context, manifest: YamlManifest, rc: RawConfig):
    config = context.get('config')
    manifest.load(config)
    log.info('Loading manifest %r from %s.', manifest.name, manifest.path.resolve())
    for data, versions in manifest.read(context):
        node = load_manifest_node(context, config, manifest, data)
        manifest.objects[node.type][node.name] = node
    return manifest
