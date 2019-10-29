from typing import Optional, Type

from spinta.components import Context, Manifest, Node, Namespace
from spinta.utils.schema import resolve_schema
from spinta import exceptions
from spinta import commands


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
