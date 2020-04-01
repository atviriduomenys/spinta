import asyncio

import jsonpatch

from spinta import commands
from spinta import spyna
from spinta.utils.json import fix_data_for_json
from spinta.utils.data import take
from spinta.commands.write import push_stream
from spinta.components import Context, Action, DataItem
from spinta.manifests.internal.components import InternalManifest


@commands.sync.register()
def sync(context: Context, manifest: InternalManifest):
    with context:
        context.attach('transaction', manifest.backend.transaction, write=True)
        asyncio.get_event_loop().run_until_complete(
            _sync_versions(context, manifest)
        )


async def _sync_versions(context: Context, manifest: InternalManifest):
    stream = _read_versions(context, manifest)
    stream = push_stream(context, stream)
    async for data in stream:
        pass


async def _read_versions(context: Context, manifest: InternalManifest):
    model = manifest.objects['model']['_version']
    for source in manifest.sync:
        for _, versions in source.read(context):
            schema = {}
            for version in versions:
                assert 'function' not in version['version']['id'], version['version']
                patch = version['changes']
                patch = jsonpatch.JsonPatch(patch)
                schema = patch.apply(schema)
                schema['version'] = take(['id', 'date'], version['version'])
                yield DataItem(model, action=Action.UPSERT, payload={
                    '_op': 'upsert',
                    '_where': 'version="%s"' % version['version']['id'],
                    'version': version['version']['id'],
                    'created': version['version']['date'],
                    'parents': version['version']['parents'],
                    'model': schema['name'],
                    'schema': fix_data_for_json(schema),
                    'changes': fix_data_for_json(version['changes']),
                    'actions': [
                        {
                            **action,
                            'upgrade': spyna.parse(action['upgrade']),
                            'downgrade': spyna.parse(action['downgrade']),
                        }
                        for action in version['migrate']
                    ],
                })
