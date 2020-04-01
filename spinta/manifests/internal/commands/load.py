import logging

from spinta import commands
from spinta.core.config import RawConfig
from spinta.components import Context
from spinta.manifests.internal.components import InternalManifest

log = logging.getLogger(__name__)


@commands.load.register()
def load(context: Context, manifest: InternalManifest, rc: RawConfig):
    config = context.get('config')

    # Add all supported node types.
    manifest.objects = {}
    for name in config.components['nodes'].keys():
        manifest.objects[name] = {}

    manifest.parent = None
    manifest.endpoints = {}

    log.info(
        'Loading manifest %r from %s backend.',
        manifest.name,
        manifest.backend,
    )
    backend = manifest.store.backends[manifest.backend]
    nodes = commands.load(context, manifest, backend)
    for data in nodes:
        node = config.components['nodes'][data['type']]()
        data = {
            'parent': manifest,
            'backend': manifest.backend,
            **data,
        }
        load(context, node, data, manifest)
        manifest.objects[node.type][node.name] = node

    return manifest
