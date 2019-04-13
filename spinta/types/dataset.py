import pathlib
import requests
import tempfile

from spinta.commands import load, prepare, check, pull, getall
from spinta.components import Context, Manifest, Node, Command, CommandList
from spinta.utils.refs import get_ref_id
from spinta.utils.url import parse_url_path
from spinta.nodes import load_node
from spinta.fetcher import Cache
from spinta.types.store import get_model_from_params


class Dataset(Node):
    schema = {
        'type': {},
        'name': {'required': True},
        'path': {'type': 'path', 'required': True},
        'source': {'type': 'command'},
        'objects': {'type': 'object', 'default': {}},
        'version': {'type': 'integer', 'required': True},
        'date': {'type': 'date', 'required': True},
        'owner': {'type': 'string'},
        'stars': {'type': 'integer'},
        'parent': {'type': 'manifest'},
        'backend': {'type': 'backend', 'inherit': True, 'required': True},
    }

    def __init__(self):
        self.source = None
        self.objects = {}
        self.version = None
        self.date = None
        self.owner = None
        self.stars = None
        self.backend = None


class Model(Node):
    schema = {
        'type': {},
        'name': {'required': True},
        'path': {'required': True},
        'source': {'type': 'command_list'},
        'identity': {'type': 'array', 'required': False},
        'properties': {'type': 'object', 'default': {}},
        'stars': {'type': 'integer'},
        'local': {'type': 'boolean'},
        'parent': {'type': 'dataset'},
        'backend': {'type': 'backend', 'inherit': True, 'required': True},
        'dependencies': {'type': 'object'},
        'extends': {'type': 'string'},
        'canonical': {'type': 'boolean', 'default': False},
    }

    property_type = 'dataset.property'

    def __init__(self):
        self.source = None
        self.identity = None
        self.properties = {}
        self.stars = None
        self.local = None
        self.backend = None
        self.dependencies = None
        self.extends = None
        self.canonical = False

    def get_type_value(self):
        return f'{self.name}/:source/{self.parent.name}'


class Property(Node):
    schema = {
        'type': {},
        'name': {'required': True},
        'path': {'required': True},
        'source': {'type': 'string'},
        'local': {'type': 'boolean'},
        'stars': {'type': 'integer'},
        'const': {'type': 'any'},
        'enum': {'type': 'array'},
        'parent': {'type': 'dataset.model'},
        'replace': {'type': 'object'},
        'ref': {'type': 'string'},
        'dependency': {'type': 'boolean'},
        'backend': {'type': 'backend', 'inherit': True, 'required': True},
    }

    def __init__(self):
        self.source = None
        self.local = None
        self.stars = None
        self.const = None
        self.enum = None
        self.replace = None
        self.ref = None
        self.dependency = None


@load.register()
def load(context: Context, dataset: Dataset, data: dict, manifest: Manifest):
    load_node(context, dataset, data, manifest)
    for name, params in (data.get('objects', {}) or None).items():
        params = {
            'path': dataset.path,
            'name': name,
            'parent': dataset,
            **(params or {}),
        }
        dataset.objects[name] = load(context, Model(), params, manifest)
    return dataset


@load.register()
def load(context: Context, model: Model, data: dict, manifest: Manifest):
    load_node(context, model, data, manifest)

    # Load source.
    if isinstance(model.source, list):
        model.source = load(
            context, CommandList(),
            [_get_source(s, model.parent.source) for s in model.source],
            scope='source',
            argname='source',
        )
    elif model.source:
        model.source = load(
            context, CommandList(),
            [_get_source(model.source, model.parent.source)],
            scope='source',
            argname='source',
        )

    # Load model properties.
    props = {'type': {'type': 'string'}}
    props.update(data.get('properties') or {})
    for name, params in props.items():
        params = {
            'name': name,
            'path': model.path,
            'parent': model,
            **(params or {}),
        }
        model.properties[name] = load(context, Property(), params, manifest)

    return model


@load.register()
def load(context: Context, prop: Property, data: dict, manifest: Manifest):
    config = context.get('config')

    load_node(context, prop, data, manifest)

    prop.type = load(context, config.types[prop.type], data)

    # Load property source.
    if isinstance(prop.source, list):
        prop.source = load(
            context, CommandList(),
            [_get_source(s, prop.parent.source) for s in prop.source],
            scope='source',
            argname='source',
        )
    elif prop.source:
        prop.source = load(
            context, Command(),
            _get_source(prop.source, prop.parent.source),
            scope='source',
            argname='source',
        )
    return prop


def _get_source(source, parent):
    if isinstance(source, str):
        if parent is None or (isinstance(parent, CommandList) and len(parent.commands) > 1):
            raise Exception(f"Command must be a dict, not an str.")
        if isinstance(parent, CommandList):
            command = parent.commands[-1].name
        else:
            command = parent.name
        return {command: source}
    return source


@prepare.register()
def prepare(context: Context, dataset: Dataset):
    for model in dataset.objects.values():
        prepare(context, model)


@check.register()
def check(context: Context, dataset: Dataset):
    _check_owner(context, dataset)
    for model in dataset.objects.values():
        _check_extends(context, dataset, model)
        for prop in model.properties.values():
            _check_ref(context, dataset, prop)


def _check_owner(context: Context, dataset: Dataset):
    if dataset.owner and dataset.owner not in dataset.manifest.objects['owner']:
        context.error(f"Can't find owner {dataset.owner!r}.")


def _check_extends(context: Context, dataset: Dataset, model: Model):
    if not model.extends:
        return

    if model.extends in dataset.objects:
        return

    if model.extends in dataset.manifest.objects['model']:
        return

    context.error(f"Can't find model {model.name!r} specified in 'extends'.")


def _check_ref(context: Context, dataset: Dataset, prop: Property):
    if prop.ref and prop.ref not in dataset.objects and prop.ref not in dataset.manifest.objects['model']:
        context.error(f"{prop.parent.name}.{prop.name} referenced an unknown object {prop.ref!r}.")


@pull.register()
def pull(context: Context, dataset: Dataset, *, models: list = None):
    with context.enter():
        tmpdir = context.attach(tempfile.TemporaryDirectory(prefix='spinta-pull-cache-'))
        context.bind('cache', Cache, path=pathlib.Path(tmpdir))
        context.bind('requests', requests.Session)

        for model in dataset.objects.values():
            if model.source is None:
                continue

            if models and model.name not in models:
                continue

            for dependency in _dependencies(context, model, model.dependencies):
                for source in model.source.commands:
                    try:
                        yield from _pull(context, model, source, dependency)
                    except Exception as e:
                        context.error(f"Error while pulling model {model.name!r}, with dependency: {dependency!r} and source: {source!r}. Error: {e}")


def _pull(context: Context, model: Model, source, dependency):
    dataset = model.parent
    rows = source(context, model, dependency=dependency)
    for row in rows:
        data = {'type': f'{model.name}/:source/{dataset.name}'}
        for prop in model.properties.values():
            if isinstance(prop.source, CommandList):
                data[prop.name] = [
                    _get_value_from_source(context, prop, prop_source, row, dependency)
                    for prop_source in prop.source.commands
                ]

            elif prop.source:
                data[prop.name] = _get_value_from_source(context, prop, prop.source, row, dependency)

            if prop.ref and prop.name in data:
                data[prop.name] = get_ref_id(data[prop.name])

        if _check_key(data.get('id')):
            yield data


def _dependencies(context: Context, model, deps):
    if deps:
        command_calls = {}
        model_names = set()
        prop_names = []
        prop_name_mapping = {}
        for name, dep in deps.items():
            if isinstance(dep, dict):
                command_calls[name] = dep
                continue

            if '.' not in dep:
                context.error(f"Dependency must be in 'object/name.property' form, got: {dep}.")
            model_name, prop_name = dep.split('.', 1)
            model_names.add(model_name)
            prop_names.append(prop_name)
            prop_name_mapping[prop_name] = name

        if len(model_names) > 1:
            names = ', '.join(sorted(model_names))
            context.error(f"Dependencies are allowed only from single model, but more than one model found: {names}.")

        if len(command_calls) > 1:
            context.error(f"Only one command call is allowed.")

        if len(command_calls) > 0:
            if len(model_names) > 0:
                context.error(f"Only one command call or one model is allowed in dependencies.")
            for name, cmd in command_calls.items():
                cmd = load(context, Command(), cmd, scope='service')
                for value in cmd(context):
                    yield {name: value}
        else:
            model_name = list(model_names)[0]
            params = parse_url_path(model_name)
            depmodel = get_model_from_params(model.manifest, params['path'], params)
            for row in getall(context, depmodel, depmodel.backend, show=prop_names):
                yield {
                    prop_name_mapping[k]: v
                    for k, v in row.items()
                }
    else:
        yield {}


def _check_key(key):
    if isinstance(key, list):
        for k in key:
            if k is None:
                return False
    elif key is None:
        return False
    return True


def _get_value_from_source(context: Context, prop: Property, source: Command, value: object, dependency: dict):
    if prop.dependency:
        return dependency.get(source.args['source'])
    else:
        return source(context, prop, value=value)
