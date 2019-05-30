from spinta.commands import load, prepare, pull
from spinta.components import Context, Node
from spinta.types.dataset import Resource, Property


class Source:
    schema = {
        'type': {'type': str, 'required': True},
        'node': {'type': Node, 'required': True},
        'name': {'type': str, 'default': ''},
    }


@load.register()
def load(context: Context, source: Source, node: Node):
    return source


@load.register()
def load(context: Context, source: Source, node: Resource):
    config = context.get('config')
    if not source.name:
        dataset = node.parent
        manifest = dataset.parent
        source.name = config.raw.get('datasets', manifest.name, dataset.name, node.name, default=None)
    return source


@prepare.register()
def prepare(context: Context, source: Source, node: Node):
    pass


@pull.register()
def pull(context: Context, source: Source, node: Property, *, data):
    return data[source.name]
