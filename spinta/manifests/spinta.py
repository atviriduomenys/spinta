import asyncio
import logging

import jsonpatch

from spinta.components import Context, Manifest, Action, DataItem
from spinta.config import RawConfig
from spinta import commands
from spinta.commands.write import push_stream
from spinta import spyna

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
    for source in manifest.sync:
        commands.freeze(context, source)


@commands.sync.register()
def sync(context: Context, manifest: SpintaManifest):
    with context:
        context.attach('transaction', manifest.backend.transaction, write=True)
        asyncio.run(_sync_versions(context, manifest))


async def _sync_versions(context: Context, manifest: SpintaManifest):
    stream = _read_versions(context, manifest)
    stream = push_stream(context, stream)
    async for data in stream:
        pass


async def _read_versions(context: Context, manifest: SpintaManifest):
    model = manifest.objects['model']['_version']
    for source in manifest.sync:
        for _, versions in source.read(context):
            schema = {}
            for version in versions:
                patch = version['changes']
                patch = jsonpatch.JsonPatch(patch)
                schema = patch.apply(schema)
                yield DataItem(model, action=Action.UPSERT, payload={
                    '_op': 'upsert',
                    '_where': 'version="%s"' % version['version']['id'],
                    'version': version['version']['id'],
                    'created': version['version']['date'],
                    'parents': version['version']['parents'],
                    'model': schema['name'],
                    'schema': schema,
                    'changes': version['changes'],
                    'actions': [
                        {
                            **action,
                            'upgrade': spyna.parse(action['upgrade']),
                            'downgrade': spyna.parse(action['downgrade']),
                        }
                        for action in version['migrate']
                    ],
                })
