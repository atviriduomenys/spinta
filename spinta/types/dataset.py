from typing import Dict

import logging
import itertools
import pathlib
import requests
import tempfile

from spinta.commands import load, prepare, check, pull, getall, authorize
from spinta.components import Context, Manifest, Node, Command, Action
from spinta.utils.refs import get_ref_id
from spinta.nodes import load_node
from spinta.fetcher import Cache
from spinta.types.datatype import DataType, Object, load_type
from spinta.auth import check_generated_scopes
from spinta.utils.schema import resolve_schema, load_from_schema, check_unkown_params
from spinta.urlparams import get_model_by_name
from spinta import commands
from spinta import exceptions
from spinta.nodes import load_namespace, load_model_properties
from spinta import components

log = logging.getLogger(__name__)


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
            f'<{self.__class__.__module__}.{self.__class__.__name__}'
            f'(name={self.name!r},'
            f' dataset={self.parent.parent.name!r}'
            f' resource={self.parent.name!r},'
            f' origin={self.origin!r}'
            ')>'
        )

    def node_type(self):
        return f'{self.type}:dataset'

    def model_specifier(self):
        elements = (
            (':dataset', self.parent.parent.name),
            (':resource', self.parent.name),
            (':origin', self.origin),
        )
        return '/'.join(itertools.chain.from_iterable(
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
        self.dependency = None
        self.hidden = False

    def __repr__(self):
        return f'<{self.__class__.__module__}.{self.__class__.__name__}(name={self.name!r}, type={self.dtype.name!r})>'


@load.register()
def load(context: Context, dataset: Dataset, data: dict, manifest: Manifest):
    load_node(context, dataset, data, manifest)

    # Load resources
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
    source_type = data.get('type')
    data['type'] = 'resource'
    load_node(context, resource, data, manifest)

    # Load dataset source
    if source_type:
        source = data.get('source')
        params = source.copy() if isinstance(source, dict) else {'name': source}
        params['type'] = source_type
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
    load_model_properties(context, model, Property, data.get('properties'))
    load_namespace(context, manifest, model)

    # Load model source
    source = data.get('source')
    if source:
        sources = []
        for params in ensure_list(source):
            params = params.copy() if isinstance(params, dict) else {'name': params}
            params['type'] = model.parent.source.type
            sources.append(load_source(context, model, params))
        model.source = sources
    else:
        model.source = []

    return model


@load.register()
def load(context: Context, prop: Property, data: dict, manifest: Manifest):
    prop = load_node(context, prop, data, manifest, check_unknowns=False)
    prop.type = 'property'
    prop.dtype = load_type(context, prop, data, manifest)
    check_unkown_params(
        [resolve_schema(prop, Node), resolve_schema(prop.dtype, DataType)],
        data, prop,
    )

    # Load property source.
    source = data.get('source')
    if source:
        if isinstance(source, list):
            sources = []
            for params in source:
                params = params.copy() if isinstance(params, dict) else {'name': params}
                params['type'] = prop.parent.parent.source.type
                sources.append(load_source(context, prop, params))
            prop.source = sources
        else:
            params = source.copy() if isinstance(source, dict) else {'name': source}
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
        raise exceptions.UnknownOwner(dataset)


def _check_extends(context: Context, dataset: Dataset, model: Model):
    if not model.extends:
        return

    if model.extends in dataset.objects:
        return

    if model.extends in dataset.manifest.objects['model']:
        return

    raise exceptions.UnknownModelReference(model, param='extends', reference=model.extends)


def _check_ref(context: Context, dataset: Dataset, prop: Property):
    if prop.ref and prop.ref not in dataset.objects and prop.ref not in dataset.manifest.objects['model']:
        raise exceptions.UnknownModelReference(prop, param='ref', reference=prop.ref)


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
                            raise exceptions.UnhandledException(
                                model,
                                error=e,
                                dependency=dependency,
                            )


def _pull(context: Context, model: Model, source, dependency):
    rows = pull(context, source, model, params=dependency)
    for row in rows:
        data = {'_type': model.model_type()}
        for prop in model.properties.values():
            if isinstance(prop.source, list):
                data[prop.name] = [
                    _get_value_from_source(context, prop, prop_source, row, dependency)
                    for prop_source in prop.source
                ]

            elif prop.source:
                data[prop.name] = _get_value_from_source(context, prop, prop.source, row, dependency)

            if prop.dtype.name == 'ref' and prop.dtype.object and prop.name in data:
                data[prop.name] = get_ref_id(data[prop.name])

        if _check_key(data.get('_id')):
            data['_id'] = get_ref_id(data['_id'])
            data['_op'] = 'upsert'
            data['_where'] = '_id=' + data['_id']
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
                raise exceptions.InvalidDependencyValue(model, dependency=dep)
            model_name, prop_name = dep.split('.', 1)
            model_names.add(model_name)
            prop_names.append(prop_name)
            prop_name_mapping[prop_name] = name

        if len(model_names) > 1:
            names = ', '.join(sorted(model_names))
            raise exceptions.MultipleModelsInDependencies(model, models=names)

        if len(command_calls) > 1:
            raise exceptions.MultipleCommandCallsInDependencies(model)

        if len(command_calls) > 0:
            if len(model_names) > 0:
                raise exceptions.MultipleCallsOrModelsInDependencies(model)
            for name, cmd in command_calls.items():
                cmd = load(context, Command(), cmd, parent=model, scope='service')
                for value in cmd(context):
                    yield {name: value}
        else:
            model_name = list(model_names)[0]
            depmodel = get_model_by_name(context, model.manifest, model_name)
            for row in getall(context, depmodel, depmodel.backend, select=prop_names):
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
        value = pull(context, source, prop, data=data)
        try:
            value = commands.coerce_source_value(context, source, prop, prop.dtype, value)
        except ValueError as e:
            log.exception(str(exceptions.InvalidValue(prop.dtype, value=value, error=str(e))))
        return value


@authorize.register()
def authorize(context: Context, action: Action, model: Model):
    check_generated_scopes(context, model.model_type(), action.value)


@authorize.register()
def authorize(context: Context, action: Action, prop: Property):
    name = prop.model.model_type() + '_' + prop.place
    check_generated_scopes(context, name, action.value)


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
    type_ = params['type']
    if f'{type_}:{node.type}' in config.components['sources']:
        Source = config.components['sources'][f'{type_}:{node.type}']
    else:
        Source = config.components['sources'][type_]

    source = Source()
    source.type = type_
    source.node = node

    # FIXME: refactor components to separate python module
    from spinta.commands.sources import Source
    source = load_from_schema(Source, source, {
        'node': node,
        **params,
    })
    return load(context, source, node)


@commands.get_referenced_model.register()
def get_referenced_model(context: Context, prop: Property, ref: str):
    # XXX: I'm not sure if this is a good idea. I think, explicit reference
    #      names would be more reliable.

    model = prop.model

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
        for origin in resource.objects.values():
            if ref in origin:
                return origin[ref]

    # Return canonical model from same manifest.
    if ref in model.manifest.objects['model']:
        return model.manifest.objects['model'][ref]

    raise exceptions.ModelReferenceNotFound(prop, ref=ref)


@commands.make_json_serializable.register()
def make_json_serializable(model: Model, value: dict) -> dict:
    return commands.make_json_serializable[Object, dict](model, value)


@commands.get_error_context.register()
def get_error_context(model: Model, *, prefix='this') -> Dict[str, str]:
    context = commands.get_error_context[Node](model, prefix=prefix)
    context['backend'] = f'{prefix}.backend.name'
    context['origin'] = f'{prefix}.origin'
    return context


@commands.get_error_context.register()  # noqa
def get_error_context(prop: Property, *, prefix='this') -> Dict[str, str]:
    context = commands.get_error_context(prop.model, prefix=f'{prefix}.model')
    context['property'] = f'{prefix}.place'
    context['backend'] = f'{prefix}.backend.name'
    return context


@prepare.register()
def prepare(context: Context, model: Model, data: dict, *, action: Action) -> dict:
    return prepare[context.__class__, components.Model, dict](context, model, data, action=action)
