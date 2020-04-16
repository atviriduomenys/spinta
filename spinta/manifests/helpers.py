from typing import List, Iterable

import jsonpatch

from spinta import commands
from spinta.nodes import get_node
from spinta.core.config import RawConfig
from spinta.components import Context, Config, Store, MetaData, EntryId
from spinta.manifests.components import Manifest
from spinta.manifests.internal.components import InternalManifest


def create_manifest(
    context: Context,
    store: Store,
    name: str,
    seen: List[str] = None,
) -> Manifest:
    rc = context.get('rc')
    config = context.get('config')
    mtype = rc.get('manifests', name, 'type', required=True)
    Manifest = config.components['manifests'][mtype]
    manifest = Manifest()
    manifest.type = mtype
    _configure_manifest(context, rc, config, store, manifest, name, seen)
    return manifest


def create_internal_manifest(context: Context, store: Store) -> InternalManifest:
    rc = context.get('rc')
    config = context.get('config')
    manifest = InternalManifest()
    manifest.type = 'yaml'
    _configure_manifest(context, rc, config, store, manifest, 'internal')
    return manifest


def _configure_manifest(
    context: Context,
    rc: RawConfig,
    config: Config,
    store: Store,
    manifest: Manifest,
    name: str,
    seen: List[str] = None,
):
    seen = seen or []
    manifest.name = name
    manifest.store = store
    manifest.parent = None
    manifest.backend = rc.get('manifests', name, 'backend', default='default')
    manifest.backend = store.backends[manifest.backend]
    manifest.endpoints = {}
    manifest.objects = {name: {} for name in config.components['nodes']}
    manifest.sync = []
    for source in rc.get('manifests', name, 'sync', default=[], cast=list):
        if source in seen:
            raise Exception("Manifest sync cycle: " + ' -> '.join(seen + [source]))
        manifest.sync.append(
            create_manifest(context, store, source, seen + [source])
        )
    commands.configure(context, manifest)


def load_manifest_nodes(
    context: Context,
    manifest: Manifest,
    schemas: Iterable[dict],
) -> None:
    config = context.get('config')
    for eid, schema in schemas:
        node = load_manifest_node(context, config, manifest, eid, schema)
        manifest.objects[node.type][node.name] = node


def load_manifest_node(
    context: Context,
    config: Config,
    manifest: Manifest,
    eid: EntryId,
    data: dict,
) -> MetaData:
    node = get_node(config, manifest, eid, data)
    node.eid = eid
    node.type = data['type']
    node.parent = manifest
    node.manifest = manifest
    commands.load(context, node, data, manifest)
    return node


def get_current_schema_changes(context: Context, manifest: Manifest, eid: EntryId) -> dict:
    freezed = commands.manifest_read_freezed(context, manifest, eid=eid)
    current = commands.manifest_read_current(context, manifest, eid=eid)
    patch = jsonpatch.make_patch(freezed, current)
    return list(patch)
