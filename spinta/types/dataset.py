import itertools
import pathlib
import requests
import tempfile

from spinta.commands import load, prepare, check, pull, getall, authorize, error
from spinta.components import Context, Manifest, Node, Command, Action
from spinta.utils.refs import get_ref_id
from spinta.nodes import load_node
from spinta.fetcher import Cache
from spinta.types.type import Object, load_type
from spinta.auth import check_generated_scopes
from spinta.utils.errors import format_error
from spinta.utils.schema import resolve_schema, load_from_schema
from spinta.utils.tree import add_path_to_tree
from spinta.urlparams import get_model_by_name
from spinta import commands
from spinta import exceptions


class Dataset(Node):
    schema = {
        'version': {'type': 'integer', 'required': True},
        'date': {'type': 'date', 'required': True},
        'owner': {'type': 'string'},
        'stars': {'type': 'integer'},
        'website': {'type': 'url', 'description': "Website of this dataset."},
        'resources': {'type': 'object', 'description': "Dataset resources."},
        'flags': {'type': 'array'},
    }

    def __init__(self):
        self.source = None
        self.objects = {}
        self.version = None
        self.date = None
        self.owner = None
        self.stars = None
        self.backend = None
        self.website = ''
        self.resources = {}


class Resource(Node):
    schema = {
        'type': {'type': 'string'},
        'source': {'type': 'string'},
        'objects': {'type': 'object', 'default': {}},
        'flags': {'type': 'array'},
    }

    def models(self):
        for objects in self.objects.values():
            yield from objects.values()

    def get_model_origin(self, model: str):
        origins = [origin for origin, objects in self.objects.items() if model in objects]
        if len(origins) == 1:
            return origins[0]
        if len(origins) > 1:
            raise Exception(
                f"More than one origin found for {model!r} model. Found origins: " +
                ', '.join(map(repr, origins)) + '.'
            )
        raise Exception(f"Can't find model {model!r}.")


class Model(Node):
    schema = {
        'origin': {'type': 'string'},
        'source': {'type': 'array'},
        'identity': {'type': 'array', 'required': False},
        'properties': {'type': 'object', 'default': {}},
        'stars': {'type': 'integer'},
        'local': {'type': 'boolean'},
        'dependencies': {'type': 'object'},
        'extends': {'type': 'string'},
        'canonical': {'type': 'boolean', 'default': False},
        'endpoint': {},
        'flags': {'type': 'array'},
    }

    property_type = 'dataset.property'

    def __init__(self):
        self.origin = None
        self.source = None
        self.identity = None
        self.properties = {}
        self.stars = None
        self.local = None
        self.backend = None
        self.dependencies = None
        self.extends = None
        self.canonical = False

    def __repr__(self):
        return (
            f'<{self.__class__.__module__}.{self.__class__.__name__}('
            f' name={self.name!r},'
            f' dataset={self.parent.parent.name!r}'
            f' resource={self.parent.name!r},'
            f' origin={self.origin!r},'
            ')>'
        )

    def get_type_value(self):
        elements = (
            (':dataset', self.parent.parent.name),
            (':resource', self.parent.name),
            (':origin', self.origin),
        )
        return f'{self.name}/' + '/'.join(itertools.chain.from_iterable(
            (k, v) for k, v in elements if v
        ))


class Property(Node):
    schema = {
        'source': {'type': 'string'},
        'local': {'type': 'boolean'},
        'stars': {'type': 'integer'},
        'const': {'type': 'any'},
        'enum': {'type': 'array'},
        'replace': {'type': 'object'},
        'ref': {'type': 'string'},
        'dependency': {'type': 'boolean'},
        'model': {'required': True},
        'hidden': {'type': 'boolean', 'inherit': True, 'default': False},
        'place': {'required': True},
        'flags': {'type': 'array'},
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
        self.hidden = False


@load.register()
def load(context: Context, dataset: Dataset, data: dict, manifest: Manifest):
    load_node(context, dataset, data, manifest)

    # Load dataset source
    if dataset.source:
        params = dataset.source
        params = params if isinstance(params, dict) else {'type': params}
        dataset.source = load_source(context, dataset, params)

    # Load models
    for name, params in (data.get('resources') or {}).items():
        params = {
            'path': dataset.path,
            'name': name,
            'parent': dataset,
            **(params or {}),
        }
        dataset.resources[name] = load(context, Resource(), params, manifest)

    return dataset


@load.register()
def load(context: Context, resource: Resource, data: dict, manifest: Manifest):
    load_node(context, resource, data, manifest)

    # Load dataset source
    if resource.type:
        params = dict(data)
        params['name'] = params.pop('source', None)
        resource.source = load_source(context, resource, params)

    # Load models
    resource.objects = {}
    for origin, objects in (data.get('objects') or {}).items():
        if origin not in resource.objects:
            resource.objects[origin] = {}
        for name, params in (objects or {}).items():
            params = {
                'type': 'model',
                'path': resource.path,
                'name': name,
                'origin': origin,
                'parent': resource,
                **(params or {}),
            }
            resource.objects[origin][name] = load(context, Model(), params, manifest)

    return resource


@load.register()
def load(context: Context, model: Model, data: dict, manifest: Manifest):
    load_node(context, model, data, manifest)
    manifest.add_model_endpoint(model)
    add_path_to_tree(manifest.tree, model.name)

    # Load model source
    if model.source:
        sources = []
        for source in ensure_list(model.source):
            params = source if isinstance(source, dict) else {'name': source}
            params['type'] = model.parent.source.type
            sources.append(load_source(context, model, params))
        model.source = sources
    else:
        model.source = []

    props = data.get('properties') or {}

    # Add build-in properties.
    props['type'] = {'type': 'string'}
    props['revision'] = {'type': 'string'}

    # 'id' is reserved for primary key.
    if 'id' not in props:
        props['id'] = {'type': 'pk'}
    elif props['id'].get('type') != 'pk':
        raise Exception("'id' property is reserved for primary key and must be of 'pk' type.")

    # Load model properties.
    model.flatprops = {}
    model.properties = {}
    for name, params in props.items():
        params = {
            'name': name,
            'place': name,
            'path': model.path,
            'parent': model,
            'model': model,
            **(params or {}),
        }
        model.flatprops[name] = model.properties[name] = load(context, Property(), params, manifest)

    return model


@load.register()
def load(context: Context, prop: Property, data: dict, manifest: Manifest):
    prop = load_node(context, prop, data, manifest, check_unknowns=False)
    prop.type = load_type(context, prop, data, manifest)

    # Load property source.
    if prop.source:
        if isinstance(prop.source, list):
            sources = []
            for params in prop.source:
                params = params if isinstance(params, dict) else {'name': params}
                params['type'] = prop.parent.parent.source.type
                sources.append(load_source(context, prop, params))
            prop.source = sources
        else:
            params = prop.source
            params = params if isinstance(params, dict) else {'name': params}
            params['type'] = prop.parent.parent.source.type
            prop.source = load_source(context, prop, params)

    return prop


@load.register()
def load(context: Context, model: Model, data: dict) -> dict:
    return data


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
    with context:
        context.attach('tmpdir', tempfile.TemporaryDirectory, prefix='spinta-pull-cache-')
        context.bind('cache', Cache, path=pathlib.Path(context.get('tmpdir')))
        context.bind('requests', requests.Session)

        for resource in dataset.resources.values():
            prepare(context, resource.source, resource)

            for model in resource.models():
                if model.source is None:
                    continue

                if models and model.name not in models:
                    continue

                for dependency in _dependencies(context, model, model.dependencies):
                    for source in model.source:
                        try:
                            yield from _pull(context, model, source, dependency)
                        except Exception as e:
                            message = (
                                '{exc}:\n'
                                '  in dependency {dependency!r}\n'
                                '  in model {model.name!r} {model}\n'
                                '  in origin {model.origin!r}\n'
                                '  in resource {model.parent.name!r} {model.parent}\n'
                                '  in dataset {model.parent.parent.name!r} {model.parent.parent}\n'
                                "  in file '{model.path}'\n"
                                '  on backend {model.backend.name!r}\n'
                            )
                            raise Exception(format_error(message, {
                                'exc': e,
                                'model': model,
                                'dependency': dependency,
                            }))


def _pull(context: Context, model: Model, source, dependency):
    rows = pull(context, source, model, params=dependency)
    for row in rows:
        data = {'type': model.get_type_value()}
        for prop in model.properties.values():
            if isinstance(prop.source, list):
                data[prop.name] = [
                    _get_value_from_source(context, prop, prop_source, row, dependency)
                    for prop_source in prop.source
                ]

            elif prop.source:
                data[prop.name] = _get_value_from_source(context, prop, prop.source, row, dependency)

            if prop.type.name == 'ref' and prop.type.object and prop.name in data:
                data[prop.name] = get_ref_id(data[prop.name])

        if _check_key(data.get('id')):
            data['id'] = get_ref_id(data['id'])
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
                cmd = load(context, Command(), cmd, parent=model, scope='service')
                for value in cmd(context):
                    yield {name: value}
        else:
            model_name = list(model_names)[0]
            depmodel = get_model_by_name(context, model.manifest, model_name)
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


def _get_value_from_source(context: Context, prop: Property, source, data: dict, dependency: dict):
    if prop.dependency:
        return dependency.get(source.name)
    else:
        return pull(context, source, prop, data=data)


@authorize.register()
def authorize(context: Context, action: Action, model: Model):
    check_generated_scopes(context, model.get_type_value(), action.value)


@authorize.register()
def authorize(context: Context, action: Action, prop: Property):
    name = prop.model.get_type_value() + '_' + prop.place
    check_generated_scopes(context, name, action.value)


@error.register()
def error(exc: Exception, context: Context, dataset: Dataset):
    message = (
        '{exc}:\n'
        '  in dataset {dataset.name!r} {dataset}\n'
        "  in file '{dataset.path}'\n"
        '  on backend {dataset.backend.name!r}\n'
    )
    raise Exception(format_error(message, {
        'exc': exc,
        'dataset': dataset,
    }))


@error.register()
def error(exc: Exception, context: Context, dataset: Dataset, data: dict, manifest: Manifest):
    error(exc, context, dataset)


@error.register()
def error(exc: Exception, context: Context, model: Model):
    message = (
        '{exc}:\n'
        '  in model {model.name!r} {model}\n'
        '  in dataset {model.parent.name!r} {model.parent}\n'
        "  in file '{model.path}'\n"
        '  on backend {model.backend.name!r}\n'
    )
    raise Exception(format_error(message, {
        'exc': exc,
        'model': model,
    }))


@error.register()
def error(exc: Exception, context: Context, model: Model, data: dict, manifest: Manifest):
    error(exc, context, model)


@error.register()
def error(exc: Exception, context: Context, prop: Property, data: dict, manifest: Manifest):
    message = (
        '{exc}:\n'
        '  in property {prop.name!r} {prop}\n'
        '  in model {prop.parent.name!r} {prop.model}\n'
        '  in dataset {prop.model.parent.name!r} {prop.model.parent}\n'
        "  in file '{prop.path}'\n"
        '  on backend {prop.backend.name!r}\n'
    )
    raise Exception(format_error(message, {
        'exc': exc,
        'prop': prop,
    }))


def ensure_list(value):
    if isinstance(value, list):
        return value
    elif value is None:
        return []
    else:
        return [value]


def load_source(context: Context, node: Node, params: dict):
    config = context.get('config')

    # Find source component by source type.
    source_type = params['type']
    if f'{source_type}:{node.type}' in config.components['sources']:
        Source = config.components['sources'][f'{source_type}:{node.type}']
    else:
        Source = config.components['sources'][source_type]

    params = {
        'node': node,
        **params,
    }
    source = Source()
    schema = resolve_schema(source, Source)

    # FIXME: refactor components to separate python module
    from spinta.commands.sources import Source
    source = load_from_schema(Source, source, schema, params)
    return load(context, source, node)


@commands.get_referenced_model.register()
def get_referenced_model(context: Context, model: Model, ref: str):
    # Self reference.
    if model.name == ref:
        return model

    # Return model from same origin.
    if ref in model.parent.objects[model.origin]:
        return model.parent.objects[model.origin][ref]

    # Return model from same resource.
    for origin in model.parent.objects.values():
        if ref in origin:
            return origin[ref]

    # Return model from same dataset.
    for resource in model.parent.parent.resources.values():
        if resource.name == model.parent.name:
            # We already checked this resource.
            continue
        for origin in model.parent.objects.values():
            if ref in origin:
                return origin[ref]

    # Return canonical model from same manifest.
    if ref in model.manifest.objects['model']:
        return model.manifest.objects['model'][ref]

    raise exceptions.ModelReferenceNotFound(model=model.get_type_value(), ref=ref)


@commands.make_json_serializable.register()
def make_json_serializable(model: Model, value: dict) -> dict:
    return commands.make_json_serializable[Object, dict](model, value)
