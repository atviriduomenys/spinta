from typing import List


from spinta.components import Context, Config, Store, Node, Source
from spinta.utils.schema import resolve_schema
from spinta import exceptions
from spinta import commands
from spinta.utils.schema import NA


def get_manifest(config: Config, name: str):
    type_ = config.rc.get('manifests', name, 'type', required=True)
    Manifest = config.components['manifests'][type_]
    manifest = Manifest()
    manifest.name = name
    manifest.load(config)
    return manifest


def load_manifest(context: Context, store: Store, config: Config, name: str, synced: List[str] = None):
    synced = synced or []
    if name in synced:
        synced = ', '.join(synced)
        raise Exception(
            f"Circular manifest sync is detected: {synced}. Check manifest "
            f"configuration with: `spinta config manifests`."
        )
    else:
        synced += [name]

    manifest = get_manifest(config, name)
    backend = config.rc.get('manifests', name, 'backend', required=True)
    manifest.backend = store.backends[backend]

    sync = config.rc.get('manifests', name, 'sync')
    if not isinstance(sync, list):
        sync = [sync] if sync else []
    # Do not fully load manifest, because loading whole manifest might be
    # expensive and we only need to do that if it is neceserry. For now, just
    # load empty manifest class.
    manifest.sync = [load_manifest(config, store, config, x, synced) for x in sync]

    # Add all supported node types.
    manifest.objects = {}
    for name in config.components['nodes'].keys():
        manifest.objects[name] = {}

    # Set some defaults.
    manifest.parent = None
    manifest.endpoints = {}

    return manifest


def load_external_source(
    context: Context,
    node: Node,
    resource: Source,
    data: dict = None,
) -> Source:
    if data is None:
        return None

    if isinstance(data, str):
        data = {'source': data}
    else:
        data = data.copy()

    if 'type' not in data:
        data['type'] = resource.type

    config = context.get('config')
    Source = config.components['core']['source']
    dsource = Source()
    commands.load(context, node, dsource, data=data)
