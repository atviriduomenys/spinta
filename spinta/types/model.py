from typing import Dict

from spinta.auth import check_generated_scopes
from spinta.commands import load, check, authorize, prepare
from spinta.components import Context, Node, Model, Property, Action
from spinta.manifests.components import Manifest
from spinta.nodes import load_node
from spinta.utils.schema import NA
from spinta import commands
from spinta import exceptions
from spinta.nodes import load_namespace, load_model_properties
from spinta.nodes import get_node


@load.register(Context, Model, dict, Manifest)
def load(context: Context, model: Model, data: dict, manifest: Manifest) -> Model:
    model.parent = manifest
    model.manifest = manifest
    load_node(context, model, data)
    model.backend = model.backend or manifest.backend
    manifest.add_model_endpoint(model)
    load_namespace(context, manifest, model)
    load_model_properties(context, model, Property, data.get('properties'))

    if model.external:
        config = context.get('config')
        external = model.external
        model.external = get_node(config, manifest, external, group='datasets', ctype='entity', parent=model)
        model.external.model = model
        load_node(context, model.external, external, parent=model)
        commands.load(context, model.external, external, manifest)
    else:
        model.external = None

    return model


@commands.link.register()
def link(context: Context, model: Model):
    store = context.get('store')
    model.backend = store.backends[model.backend]
    for prop in model.properties.values():
        commands.link(context, prop)
    if model.external:
        commands.link(context, model.external)


@load.register(Context, Property, dict, Manifest)
def load(context: Context, prop: Property, data: dict, manifest: Manifest) -> Property:
    config = context.get('config')
    prop.type = 'property'
    prop, data = load_node(context, prop, data, mixed=True)
    prop.dtype = get_node(config, manifest, data, group='types', parent=prop)
    prop.dtype.type = 'type'
    prop.dtype.prop = prop
    load_node(context, prop.dtype, data)
    if prop.dtype.backend is None:
        prop.dtype.backend = prop.model.backend
    prop.external = _load_property_external(context, manifest, prop, prop.external)
    commands.load(context, prop.dtype, data, manifest)
    return prop


@commands.link.register()
def link(context: Context, prop: Property):
    commands.link(context, prop.dtype)
    if prop.external:
        if isinstance(prop.external, list):
            for external in prop.external:
                commands.link(context, external)
        else:
            commands.link(context, prop.external)


def _load_property_external(context, manifest, prop, data):
    if data is None:
        return None

    if isinstance(data, list):
        return [
            _load_property_external(context, manifest, prop, x)
            for x in data
        ]

    if isinstance(data, str):
        return _load_property_external(context, manifest, prop, {'name': data})

    config = context.get('config')
    external = get_node(config, manifest, data, group='datasets', ctype='attribute', parent=prop)
    load_node(context, external, data, parent=prop)
    return external


@load.register(Context, Model, dict)
def load(context: Context, model: Model, data: dict) -> dict:
    # check that given data does not have more keys, than model's schema
    non_hidden_keys = []
    for key, prop in model.properties.items():
        if not prop.hidden:
            non_hidden_keys.append(key)

    unknown_props = set(data.keys()) - set(non_hidden_keys)
    if unknown_props:
        raise exceptions.MultipleErrors(
            exceptions.FieldNotInResource(model, property=prop)
            for prop in sorted(unknown_props)
        )

    result = {}
    for name, prop in model.properties.items():
        value = data.get(name, NA)
        value = load(context, prop.dtype, value)
        if value is not NA:
            result[name] = value
    return result


@load.register(Context, Property, object)
def load(context: Context, prop: Property, value: object) -> object:
    value = _prepare_prop_data(prop.name, value)
    value[prop.name] = load(context, prop.dtype, value[prop.name])
    return value


def _prepare_prop_data(name: str, data: dict):
    return {
        **{
            k: v
            for k, v in data.items()
            if k.startswith('_') and k not in ('_id', '_content_type')
        },
        name: {
            k: v
            for k, v in data.items()
            if not k.startswith('_') or k in ('_id', '_content_type')
        }
    }


@check.register()
def check(context: Context, model: Model):
    if '_id' not in model.properties:
        raise exceptions.MissingRequiredProperty(model, prop='_id')


@prepare.register()
def prepare(context: Context, model: Model, data: dict, *, action: Action) -> dict:
    # prepares model's data for storing in Mongo
    backend = model.backend
    result = {}
    for name, prop in model.properties.items():
        value = data.get(name, NA)
        value = prepare(context, prop.dtype, backend, value)
        if action == Action.UPDATE and not name.startswith('_'):
            result[name] = None if value is NA else value
        elif value is not NA:
            result[name] = value
    return result


@prepare.register()
def prepare(context: Context, prop: Property, value: object, *, action: Action) -> object:
    value[prop.name] = prepare(context, prop.dtype, prop.dtype.backend, value[prop.name])
    return value


@authorize.register()
def authorize(context: Context, action: Action, model: Model):
    check_generated_scopes(context, model.model_type(), action.value)


@authorize.register()
def authorize(context: Context, action: Action, prop: Property):
    # if property is hidden - specific scope must be provided
    # otherwise generic model scope is also enough
    name = prop.model.model_type()
    prop_scope_name = name + '_' + prop.place
    check_generated_scopes(context, name, action.value, prop_scope_name, prop.hidden)


@commands.get_referenced_model.register()
def get_referenced_model(context: Context, prop: Property, ref: str) -> Node:
    model = prop.model

    if ref in model.manifest.objects['model']:
        return model.manifest.objects['model'][ref]

    raise exceptions.ModelReferenceNotFound(prop, ref=ref)


@commands.get_error_context.register()
def get_error_context(model: Model, *, prefix='this') -> Dict[str, str]:
    context = commands.get_error_context(model.manifest, prefix=f'{prefix}.manifest')
    context['schema'] = f'{prefix}.path.__str__()'
    context['model'] = f'{prefix}.name'
    context['dataset'] = f'{prefix}.external.dataset.name'
    context['resource'] = f'{prefix}.external.resource.name'
    context['resource.backend'] = f'{prefix}.external.resource.backend.name'
    context['entity'] = f'{prefix}.external.name'
    return context


@commands.get_error_context.register()
def get_error_context(prop: Property, *, prefix='this') -> Dict[str, str]:
    context = commands.get_error_context(prop.model, prefix=f'{prefix}.model')
    context['property'] = f'{prefix}.place'
    context['attribute'] = f'{prefix}.external.name'
    return context
