from typing import Dict

from spinta.auth import check_generated_scopes
from spinta.commands import load, check, authorize, prepare
from spinta.components import Context, Manifest, Node, Model, Property, Action
from spinta.nodes import load_node
from spinta.types.datatype import PrimaryKey, DataType, load_type
from spinta.utils.schema import resolve_schema, check_unkown_params
from spinta.common import NA
from spinta import commands
from spinta import exceptions
from spinta.nodes import load_namespace, load_model_properties


@load.register()
def load(context: Context, model: Model, data: dict, manifest: Manifest) -> Model:
    load_node(context, model, data, manifest)
    manifest.add_model_endpoint(model)
    load_namespace(context, manifest, model)
    load_model_properties(context, model, Property, data.get('properties'))
    return model


@load.register()
def load(context: Context, prop: Property, data: dict, manifest: Manifest) -> Property:
    prop = load_node(context, prop, data, manifest, check_unknowns=False)
    prop.type = 'property'
    prop.dtype = load_type(context, prop, data, manifest)
    check_unkown_params(
        [resolve_schema(prop, Node), resolve_schema(prop.dtype, DataType)],
        data, prop,
    )
    if isinstance(prop.dtype, PrimaryKey):
        prop.dtype.unique = True
    return prop


@load.register()
def load(context: Context, model: Model, data: dict) -> dict:
    # check that given data does not have more keys, than model's schema
    unknown_props = set(data.keys()) - set(model.properties.keys())
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


@load.register()
def load(context: Context, prop: Property, value: object) -> object:
    return load(context, prop.dtype, value)


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
    return prepare(context, prop.dtype, prop.backend, value)


@authorize.register()
def authorize(context: Context, action: Action, model: Model):
    check_generated_scopes(context, model.model_type(), action.value)


@authorize.register()
def authorize(context: Context, action: Action, prop: Property):
    name = prop.model.model_type() + '_' + prop.place
    check_generated_scopes(context, name, action.value)


@commands.get_referenced_model.register()
def get_referenced_model(context: Context, prop: Property, ref: str) -> Node:
    model = prop.model

    if ref in model.manifest.objects['model']:
        return model.manifest.objects['model'][ref]

    raise exceptions.ModelReferenceNotFound(prop, ref=ref)


@commands.get_error_context.register()
def get_error_context(prop: Property, *, prefix='this') -> Dict[str, str]:
    context = commands.get_error_context(prop.model, prefix=f'{prefix}.model')
    context['property'] = f'{prefix}.place'
    return context
