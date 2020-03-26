from typing import Dict

from spinta.auth import check_generated_scopes
from spinta.commands import load, check, authorize, prepare
from spinta.components import Context, Node, Model, Property, Action, Source
from spinta.utils.schema import NA
from spinta import commands
from spinta import exceptions


@load.register()
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
    value[prop.name] = prepare(context, prop.dtype, prop.backend, value[prop.name])
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
def get_error_context(prop: Property, *, prefix='this') -> Dict[str, str]:
    context = commands.get_error_context(prop.model, prefix=f'{prefix}.model')
    context['property'] = f'{prefix}.place'
    return context
