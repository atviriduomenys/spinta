from typing import Optional, Type, List

import pathlib

import pkg_resources as pres

from spinta.components import Context, Config, Store, Manifest, Node, Namespace
from spinta.utils.schema import resolve_schema
from spinta import exceptions
from spinta import commands


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


def get_internal_manifest(context: Context):
    config = context.get('config')
    Manifest = config.components['manifests']['yaml']
    internal = Manifest()
    internal.name = 'internal'
    internal.path = pathlib.Path(pres.resource_filename('spinta', 'manifest'))
    return internal


def get_node(config: Config, manifest: Manifest, data: dict, check=True):
    if not isinstance(data, dict):
        raise exceptions.InvalidManifestFile(
            manifest=manifest.name,
            filename=data['path'],
            error=f"Expected dict got {data.__class__.__name__}.",
        )

    if 'type' not in data:
        raise exceptions.InvalidManifestFile(
            manifest=manifest.name,
            filename=data['path'],
            error=f"Required parameter 'type' is not defined.",
        )

    if data['type'] not in manifest.objects:
        raise exceptions.InvalidManifestFile(
            manifest=manifest.name,
            filename=data['path'],
            error=f"Unknown type {data['type']!r}.",
        )

    if check and data['name'] in manifest.objects[data['type']]:
        raise exceptions.InvalidManifestFile(
            manifest=manifest.name,
            filename=data['path'],
            error=(
                f"Node {data['type']} with name {data['name']} already defined "
                f"in {manifest.objects[data['type']][data['name']].path}."
            ),
        )

    Node = config.components['nodes'][data['type']]
    return Node()


def load_node(context: Context, node: Node, data: dict, manifest: Manifest, *, check_unknowns=True) -> Node:
    na = object()
    store = context.get('store')
    node.manifest = manifest
    node.type = data['type']
    node.path = data['path']
    node.name = data['name']
    node.parent = data['parent']

    node_schema = resolve_schema(node, Node)
    for name in set(node_schema) | set(data):
        if name not in node_schema:
            if check_unknowns:
                raise exceptions.UnknownParameter(node, param=name)
            else:
                continue
        schema = node_schema[name]
        value = data.get(name, na)
        if schema.get('inherit', False) and value is na:
            if node.parent and hasattr(node.parent, name):
                value = getattr(node.parent, name)
            else:
                value = None
        if schema.get('required', False) and (value is na or value is None):
            raise exceptions.MissingRequiredProperty(node, prop=name)
        if schema.get('type') == 'backend' and isinstance(value, str):
            value = store.backends[value]
        if value is na:
            value = schema.get('default')
        setattr(node, name, value)
    return node


def load_namespace(context: Context, manifest: Manifest, node: Node):
    parts = []
    parent = manifest
    for part in [''] + node.name.split('/'):
        parts.append(part)
        name = '/'.join(parts[1:])
        if name not in manifest.objects['ns']:
            ns = Namespace()
            data = {
                'type': 'ns',
                'name': name,
                'title': part,
                'path': manifest.path,
                'parent': parent,
                'names': {},
                'models': {},
                'backend': None,
            }
            manifest.objects['ns'][name] = load_node(context, ns, data, manifest)
        else:
            ns = manifest.objects['ns'][name]
        if part and part not in parent.names:
            parent.names[part] = ns
        parent = ns
    parent.models[node.model_type()] = node


def load_model_properties(context: Context, model: Node, Prop: Type[Node], data: Optional[dict]) -> None:
    data = data or {}

    # Add build-in properties.
    data = {
        '_op': {'type': 'string'},
        '_type': {'type': 'string'},
        '_id': {'type': 'pk', 'unique': True},
        '_revision': {'type': 'string'},
        '_transaction': {'type': 'integer'},
        '_where': {'type': 'rql'},
        **data,
    }

    model.flatprops = {}
    model.leafprops = {}
    model.properties = {}
    for name, params in data.items():
        params = {
            'name': name,
            'place': name,
            'path': model.path,
            'parent': model,
            'model': model,
            **params,
        }
        prop = commands.load(context, Prop(), params, model.manifest)
        model.properties[name] = prop
        model.flatprops[name] = prop
