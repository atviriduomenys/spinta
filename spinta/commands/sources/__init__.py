from spinta.commands import load, prepare, pull, error
from spinta.components import Context, Node
from spinta.types.dataset import Resource, Property
from spinta.utils.errors import format_error


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
        source.name = config.raw.get('datasets', manifest.name, dataset.name.replace('/', '_'), node.name.replace('/', '_'), default=None)
    return source


@prepare.register()
def prepare(context: Context, source: Source, node: Node):
    pass


@pull.register()
def pull(context: Context, source: Source, node: Property, *, data):
    return data[source.name]


@error.register()
def error(exc: Exception, context: Context, source: Source, node: Resource):
    message = (
        '{exc}:\n'
        '  in resource {resource.name!r} {resource}\n'
        '  in dataset {resource.parent.name!r} {resource.parent}\n'
        "  in file '{resource.path}'\n"
        '  on backend {resource.backend.name!r}\n'
    )
    raise Exception(format_error(message, {
        'exc': exc,
        'resource': node,
    }))
