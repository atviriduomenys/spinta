from typing import Any
from typing import Dict
from typing import List, Iterable, Optional
from typing import Type

import jsonpatch

from spinta import commands
from spinta.backends.components import BackendOrigin
from spinta.components import Mode
from spinta.backends.helpers import load_backend
from spinta.components import Model
from spinta.datasets.components import Dataset
from spinta.datasets.components import Entity
from spinta.datasets.components import Resource
from spinta.dimensions.enum.helpers import load_enums
from spinta.dimensions.prefix.helpers import load_prefixes
from spinta.exceptions import UnknownKeyMap
from spinta.manifests.components import ManifestSchema
from spinta.nodes import get_node
from spinta.core.config import RawConfig
from spinta.components import Context, Config, Store, MetaData, EntryId
from spinta.manifests.components import Manifest
from spinta.manifests.internal.components import InternalManifest
from spinta.types.namespace import load_namespace_from_name
from spinta.utils.enums import enum_by_name
from spinta.core.enums import Access
from spinta.utils.imports import importstr


def init_manifest(context: Context, manifest: Manifest, name: str):
    config = context.get('config')
    manifest.name = name
    manifest.store = context.get('store')
    manifest.parent = None
    manifest.endpoints = {}
    manifest.backends = {}
    manifest.objects = {name: {} for name in config.components['nodes']}
    manifest.sync = []
    manifest.prefixes = {}
    manifest.enums = {}
    manifest.access = Access.protected
    manifest.keymap = None
    manifest.backend = None
    manifest.mode = Mode.internal


def clone_manifest(context: Context, name: str = 'manifest') -> Manifest:
    manifest = Manifest()
    store: Store = context.get('store')
    init_manifest(context, manifest, name)
    if store.manifest:
        manifest.keymap = store.manifest.keymap
    return manifest


def create_manifest(
    context: Context,
    store: Store,
    name: str,
    seen: List[str] = None,
) -> Manifest:
    rc: RawConfig = context.get('rc')
    config = context.get('config')
    mtype = rc.get('manifests', name, 'type', required=True)
    Manifest_ = config.components['manifests'][mtype]
    manifest = Manifest_()
    manifest.type = mtype
    init_manifest(context, manifest, name)
    _configure_manifest(context, rc, store, manifest, seen)
    return manifest


def create_internal_manifest(context: Context, store: Store) -> InternalManifest:
    rc = context.get('rc')
    manifest = InternalManifest()
    manifest.type = 'yaml'
    init_manifest(context, manifest, 'internal')
    _configure_manifest(
        context, rc, store, manifest,
        backend=store.manifest.backend.name if store.manifest.backend else None,
    )
    return manifest


def _configure_manifest(
    context: Context,
    rc: RawConfig,
    store: Store,
    manifest: Manifest,
    seen: List[str] = None,
    *,
    backend: str = 'default',
):
    seen = seen or []
    name = manifest.name
    access = rc.get('manifests', name, 'access')
    if access:
        manifest.access = enum_by_name(manifest, 'access', Access, access)
    keymap = rc.get('manifests', name, 'keymap', default=None) or 'default'
    if keymap:
        if keymap not in store.keymaps:
            raise UnknownKeyMap(manifest, keymap=keymap)
        manifest.keymap = store.keymaps[keymap]
    backend = rc.get('manifests', name, 'backend', default=backend)
    if backend:
        manifest.backend = store.backends[backend]
    mode = rc.get('manifests', name, 'mode')
    if mode:
        manifest.mode = enum_by_name(manifest, 'mode', Mode, mode)
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
    schemas: Iterable[ManifestSchema],
    *,
    source: Manifest = None,
    link: bool = False,
) -> None:
    to_link = []
    config = context.get('config')
    for eid, schema in schemas:
        if schema.get('type') == 'manifest':
            _load_manifest(context, manifest, schema, eid)
        else:
            node = _load_manifest_node(context, config, manifest, source, eid, schema)
            manifest.objects[node.type][node.name] = node
            if link:
                to_link.append(node)

    if '' not in manifest.namespaces:
        # Root namespace must always be present in manifest event if manifest is
        # empty.
        load_namespace_from_name(context, manifest, '', drop=False)

    if to_link:
        for node in to_link:
            commands.link(context, node)


def _load_manifest_backends(
    context: Context,
    manifest: Manifest,
    backends: Dict[str, Dict[str, str]],
) -> None:
    manifest.backends = {}
    for name, data in backends.items():
        manifest.backends[name] = load_backend(
            context,
            manifest,
            name,
            BackendOrigin.manifest,
            data,
        )


def _load_manifest(
    context: Context,
    manifest: Manifest,
    data: Dict[str, Any],
    eid: EntryId,
) -> None:
    if 'backends' in data:
        _load_manifest_backends(context, manifest, data['backends'])
    if 'prefixes' in data:
        prefixes = load_prefixes(context, manifest, manifest, data['prefixes'])
        manifest.prefixes.update(prefixes)
    if 'enums' in data:
        parents = [manifest]
        enums = load_enums(context, parents, data['enums'])
        manifest.enums.update(enums)


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


def get_current_schema_changes(
    context: Context,
    manifest: Manifest,
    eid: EntryId,
) -> List[dict]:
    freezed = commands.manifest_read_freezed(context, manifest, eid=eid)
    current = commands.manifest_read_current(context, manifest, eid=eid)
    patch = jsonpatch.make_patch(freezed, current)
    return list(patch)


def entity_to_schema(entity: Optional[Entity]) -> ManifestSchema:
    if entity is None:
        return None, {
            'dataset': None,
            'resource': None,
            'name': '',
            'pk': [],
        }
    else:
        return None, {
            'dataset': entity.dataset.name if entity.dataset else None,
            'resource': entity.resource.name if entity.resource else None,
            'name': entity.name,
            'prepare': entity.prepare,
            'pk': [p.name for p in entity.pkeys],
            # 'pk': [
            #     to_property_name(p)
            #     for p in _ensure_list(schema.primary_key)
            # ],
        }


def model_to_schema(model: Optional[Model]) -> ManifestSchema:
    if model is None:
        return None, {
            'type': 'model',
            'name': '',
            'level': None,
            'access': None,
            'title': '',
            'description': '',
            'external': None,
            'properties': {},
        }
    else:
        return model.eid, {
            'type': 'model',
            'name': model.name,
            'level': model.level.name if model.level else None,
            'access': model.given.access,
            'title': model.title,
            'description': model.description,
        }


def resource_to_schema(resource: Resource) -> ManifestSchema:
    if (
        resource.backend and
        resource.backend.origin != BackendOrigin.resource
    ):
        backend = resource.backend.name
    else:
        backend = ''

    return resource.eid, {
        'type': resource.type,
        'backend': backend,
        'external': resource.external,
        'prepare': None if resource.prepare is None else str(resource.prepare),
        'level': resource.level.name if resource.level else None,
        'access': resource.given.access,
        'title': resource.title,
        'description': resource.description,
    }


def dataset_to_schema(dataset: Dataset) -> ManifestSchema:
    return dataset.eid, {
        'type': 'dataset',
        'name': dataset.name,
        'level': dataset.level.name if dataset.level else None,
        'access': dataset.given.access,
        'title': dataset.title,
        'description': dataset.description,
        'resources': {
            resource.name: {
                'type': resource.type,
                'backend': (
                    resource.backend.name
                    if (
                        resource.backend and
                        resource.backend.origin != BackendOrigin.resource
                    )
                    else ''
                ),
                'level': resource.level.name if resource.level else None,
                'access': resource.given.access,
                'title': resource.title,
                'description': resource.description,
            }
            for resource in dataset.resources.values()
        }
    }


def detect_manifest_from_path(rc: RawConfig, path: str) -> Type[Manifest]:
    for type_ in rc.keys('components', 'manifests'):
        Manifest_: Type[Manifest] = rc.get(
            'components', 'manifests', type_,
            cast=importstr,
        )
        if Manifest_.detect_from_path(path):
            return Manifest_
    raise ValueError(f"Can't find manifest type matching given path {path!r}")
