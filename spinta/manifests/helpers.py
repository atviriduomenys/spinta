from itertools import chain
from typing import Any
from typing import Callable
from typing import Dict
from typing import List, Iterable, Optional
from typing import Tuple

import jsonpatch

from spinta import commands
from spinta.backends.components import BackendOrigin
from spinta.components import Mode
from spinta.backends.helpers import load_backend
from spinta.components import Model
from spinta.components import Property
from spinta.core.ufuncs import Expr
from spinta.dimensions.prefix.helpers import load_prefixes
from spinta.nodes import get_node
from spinta.core.config import RawConfig
from spinta.components import Context, Config, Store, MetaData, EntryId
from spinta.manifests.components import Manifest
from spinta.manifests.internal.components import InternalManifest
from spinta.types.datatype import Ref
from spinta.utils.enums import enum_by_name
from spinta.core.enums import Access
from spinta.utils.naming import to_model_name
from spinta.utils.naming import to_property_name


def create_manifest(
    context: Context,
    store: Store,
    name: str,
    seen: List[str] = None,
) -> Manifest:
    rc: RawConfig = context.get('rc')
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
        backend=store.manifest.backend.name if store.manifest.backend else None,
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
    manifest.backend = store.backends[manifest.backend] if manifest.backend else None
    manifest.endpoints = {}
    manifest.backends = {}
    manifest.objects = {name: {} for name in config.components['nodes']}
    manifest.sync = []
    manifest.prefixes = {}
    manifest.mode = enum_by_name(manifest, 'mode', Mode, (
        rc.get('manifests', name, 'mode') or 'internal'
    ))
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


def _format_expr(expr: Expr, formatter: Callable[[str], str]):
    if expr.name == 'bind':
        if len(expr.args) == 1:
            expr.args = (formatter(expr.args[0]),)
    else:
        for arg in chain(expr.args, expr.kwargs.values()):
            if isinstance(arg, Expr):
                _format_expr(arg, formatter)


def _format_model_name(name: str) -> str:
    if '/' in name:
        ns, name = name.rsplit('/', 1)
        return '/'.join([ns.lower(), to_model_name(name)])
    else:
        return to_model_name(name)


def _format_property(prop: Property) -> Property:
    prop.name = to_property_name(prop.name)
    prop.place = '.'.join([to_property_name(n) for n in prop.place.split('.')])
    if prop.external and prop.external.prepare:
        _format_expr(prop.external.prepare, to_property_name)
    return prop


def _format_model(model: Model) -> Model:
    model.name = _format_model_name(model.name)
    model.properties = {
        prop.name: prop
        for prop in map(_format_property, model.properties.values())
    }
    if model.external and model.external.prepare:
        _format_expr(model.external.prepare, to_property_name)
    return model


def reformat_names(manifest: Manifest):
    manifest.objects['model'] = {
        model.name: model
        for model in map(_format_model, manifest.models.values())
    }
