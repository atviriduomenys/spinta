import logging

from spinta.components import Context, Manifest
from spinta.config import RawConfig
from spinta import commands

log = logging.getLogger(__name__)


class SpintaManifest(Manifest):
    pass


@commands.load.register()
def load(context: Context, manifest: SpintaManifest, rc: RawConfig):
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
        manifest.backend.name,
    )

    for data in manifest.backend.query_nodes():
        node = config.components['nodes'][data['type']]()
        data = {
            'parent': manifest,
            'backend': manifest.backend,
            **data,
        }
        load(context, node, data, manifest)
        manifest.objects[node.type][node.name] = node

    return manifest


@commands.freeze.register()
def freeze(context: Context, manifest: SpintaManifest):
    # You can't freeze Spinta manifest directly, because Spinta manifest schema
    # can only be updated after migrations are applied.

    # But other manifests from which this manifests is synced, can be updated.
    for manifest_ in manifest.sync:
        commands.freeze(context, manifest_)
