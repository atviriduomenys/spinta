from spinta.components import Context, Manifest, Node


def load_node(context: Context, node: Node, data: dict, manifest: Manifest) -> Node:
    na = object()
    store = context.get('store')
    node.manifest = manifest
    node.path = data['path']
    node.name = data['name']
    node.parent = data['parent']
    for name in set(node.schema) | set(data):
        if name not in node.schema:
            _load_node_error(context, node, f"Unknown option {name!r}.")
        schema = node.schema[name]
        value = data.get(name, na)
        if schema.get('inherit', False) and value is na:
            if node.parent:
                value = getattr(node.parent, name)
            else:
                value = getattr(manifest, name)
        if schema.get('required', False) and value is na:
            _load_node_error(context, node, f"Missing requied option {name!r}.")
        if schema.get('type') == 'backend' and isinstance(value, str):
            value = store.backends[value]
        if value is na:
            value = schema.get('default')
        setattr(node, name, value)
    return node


def _load_node_error(context, node, message):
    context.error(
        f"Error while loading <{node.name!r}: "
        f"{node.__class__.__module__}.{node.__class__.__name__}> from "
        f"{node.path}: {message}"
    )
