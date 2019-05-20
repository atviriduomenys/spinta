from spinta.components import Context, Manifest, Node
from spinta.utils.schema import resolve_schema


def load_node(context: Context, node: Node, data: dict, manifest: Manifest, *, check_unknowns=True) -> Node:
    na = object()
    store = context.get('store')
    node.manifest = manifest
    node.path = data['path']
    node.name = data['name']
    node.parent = data['parent']

    node_schema = resolve_schema(node, Node)
    for name in set(node_schema) | set(data):
        if name not in node_schema:
            if check_unknowns:
                _load_node_error(context, node, f"Unknown option {name!r}.")
            else:
                continue
        schema = node_schema[name]
        value = data.get(name, na)
        if schema.get('inherit', False) and value is na:
            if node.parent and hasattr(node.parent, name):
                value = getattr(node.parent, name)
            else:
                value = None
        if schema.get('required', False) and value is na:
            _load_node_error(context, node, f"Missing requied option {name!r}.")
        if schema.get('type') == 'backend' and isinstance(value, str):
            value = store.backends[value]
        if value is na:
            value = schema.get('default')
        setattr(node, name, value)
    return node


def _load_node_error(context, node, message):
    raise Exception(
        f"Error while loading <{node.name!r}: "
        f"{node.__class__.__module__}.{node.__class__.__name__}> from "
        f"{node.path}: {message}"
    )
