from spinta.commands import load, prepare, pull
from spinta.components import Context, Node
from spinta.types.dataset import Resource, Property
from spinta.types.datatype import DataType, Integer, Number
from spinta import commands


class Source:
    schema = {
        'type': {'type': str, 'required': True},
        'node': {'type': Node, 'required': True},
        'name': {'type': str, 'default': ''},
    }


@load.register()
def load(context: Context, source: Source, node: Node):
    return source


def dataset_source_config_name(resource: Resource):
    dataset = resource.parent
    manifest = dataset.parent
    return [
        'datasets',
        manifest.name,
        dataset.name.replace('/', '_'),
        resource.name.replace('/', '_'),
    ]


def dataset_source_envvar_name(resource: Resource):
    return '_'.join(['SPINTA'] + dataset_source_config_name(resource)).upper()


@load.register()
def load(context: Context, source: Source, node: Resource):
    config = context.get('config')
    if not source.name:
        source.name = config.raw.get(*dataset_source_config_name(node), default=None)
    return source


@prepare.register()
def prepare(context: Context, source: Source, node: Node):
    pass


@pull.register()
def pull(context: Context, source: Source, node: Property, *, data):
    return data[source.name]


@commands.get_error_context.register()
def get_error_context(source: Source):
    context = commands.get_error_context[Node](source.node, prefix='this.node')
    context['source'] = 'this.type'
    return context


@commands.coerce_source_value.register()
def coerce_source_value(
    context: Context,
    source: Source,
    prop: Property,
    dtype: DataType,
    value: object,
) -> object:
    return value


@commands.coerce_source_value.register()  # noqa
def coerce_source_value(
    context: Context,
    source: Source,
    prop: Property,
    dtype: Integer,
    value: object,
) -> object:
    return int(value)


@commands.coerce_source_value.register()  # noqa
def coerce_source_value(
    context: Context,
    source: Source,
    prop: Property,
    dtype: Number,
    value: object,
) -> object:
    return float(value)
