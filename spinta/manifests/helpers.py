from typing import List, Iterable, Optional
from typing import Tuple

import jsonpatch

from spinta import commands
from spinta.nodes import get_node
from spinta.core.config import RawConfig
from spinta.components import Context, Config, Store, MetaData, EntryId
from spinta.manifests.components import Manifest
from spinta.manifests.internal.components import InternalManifest
from spinta.utils.enums import enum_by_name
from spinta.core.enums import Access


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
    _configure_manifest(
        context, rc, config, store, manifest, 'internal',
        backend=store.manifest.backend.name,
    )
    return manifest


def _configure_manifest(
    context: Context,
    rc: RawConfig,
    config: Config,
    store: Store,
    manifest: Manifest,
    name: str,
    seen: List[str] = None,
    *,
    backend: str = 'default',
):
    seen = seen or []
    manifest.name = name
    manifest.store = store
    manifest.parent = None
    manifest.access = rc.get('manifests', name, 'access') or 'protected'
    manifest.access = enum_by_name(manifest, 'access', Access, manifest.access)
    manifest.keymap = rc.get('manifests', name, 'keymap', default=None)
    manifest.keymap = store.keymaps[manifest.keymap] if manifest.keymap else None
    manifest.backend = rc.get('manifests', name, 'backend', default=backend)
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
    schemas: Iterable[Tuple[int, Optional[dict]]],
    *,
    source: Manifest = None,
) -> None:
    config = context.get('config')
    for eid, schema in schemas:
        node = _load_manifest_node(context, config, manifest, source, eid, schema)
        manifest.objects[node.type][node.name] = node


def _load_manifest_node(
    context: Context,
    config: Config,
    manifest: Manifest,
    source: Optional[Manifest],
    eid: EntryId,
    data: dict,
) -> MetaData:
    node = get_node(config, manifest, eid, data)
    node.eid = eid
    node.type = data['type']
    node.parent = manifest
    node.manifest = manifest
    commands.load(context, node, data, manifest, source=source)
    return node


def get_current_schema_changes(context: Context, manifest: Manifest, eid: EntryId) -> dict:
    freezed = commands.manifest_read_freezed(context, manifest, eid=eid)
    current = commands.manifest_read_current(context, manifest, eid=eid)
    patch = jsonpatch.make_patch(freezed, current)
    return list(patch)
