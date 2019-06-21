from spinta.auth import check_generated_scopes
from spinta.commands import load, check, error, authorize, prepare
from spinta.components import Context, Manifest, Node, Model, Property, Action
from spinta.exceptions import DataError
from spinta.nodes import load_node
from spinta.types.type import Type, load_type
from spinta.utils.errors import format_error
from spinta.utils.schema import resolve_schema
from spinta.utils.tree import add_path_to_tree
from spinta.common import NA


@load.register()
def load(context: Context, model: Model, data: dict, manifest: Manifest) -> Model:
    load_node(context, model, data, manifest)
    manifest.add_model_endpoint(model)
    add_path_to_tree(manifest.tree, model.name)

    props = data.get('properties') or {}

    # Add build-in properties.
    props['type'] = {'type': 'string'}
    props['revision'] = {'type': 'string'}

    # 'id' is reserved for primary key.
    if 'id' not in props:
        props['id'] = {'type': 'pk'}
    elif props['id'].get('type') != 'pk':
        raise Exception("'id' property is reserved for primary key and must be of 'pk' type.")

    model.flatprops = {}
    model.properties = {}
    for name, prop in props.items():
        prop = {
            'name': name,
            'place': name,
            'path': model.path,
            'parent': model,
            'model': model,
            **prop,
        }
        model.flatprops[name] = model.properties[name] = load(context, Property(), prop, manifest)

    return model


@load.register()
def load(context: Context, prop: Property, data: dict, manifest: Manifest) -> Property:
    prop = load_node(context, prop, data, manifest, check_unknowns=False)
    prop.type = load_type(context, prop, data, manifest)

    # Check if there any unknown params were given.
    known_params = set(resolve_schema(prop, Node).keys()) | set(resolve_schema(prop.type, Type).keys())
    given_params = set(data.keys())
    unknown_params = given_params - known_params
    if unknown_params:
        raise Exception("Unknown params: %s" % ', '.join(map(repr, sorted(unknown_params))))

    return prop


@load.register()
def load(context: Context, model: Model, data: dict) -> dict:
    # check that given data does not have more keys, than model's schema
    unknown_params = set(data.keys()) - set(model.properties.keys())
    if unknown_params:
        raise DataError("Unknown params: %s" % ', '.join(map(repr, sorted(unknown_params))))

    result = {}
    for name, prop in model.properties.items():
        # if private model property is not in data - ignore it's loading
        if name in ('id', 'revision', 'type') and name not in data:
            continue
        value = data.get(name, NA)
        result[name] = load(context, prop.type, value)
    return result


@check.register()
def check(context: Context, model: Model):
    if 'id' not in model.properties:
        context.error("Primary key is required, add `id` property to the model.")
    if model.properties['id'].type == 'pk':
        context.deprecation("`id` property must specify real type like 'string' or 'integer'. Use of 'pk' is deprecated.")


@prepare.register()
def prepare(context: Context, model: Model, data: dict, *, action: Action) -> dict:
    # prepares model's data for storing in Mongo
    backend = model.backend
    result = {}
    for name, prop in model.properties.items():
        value = data.get(name, NA)
        value = prepare(context, prop.type, backend, value)
        if action == Action.UPDATE:
            result[name] = None if value is NA else value
        else:
            if value is not NA:
                result[name] = value
    return result


@error.register()
def error(exc: Exception, context: Context, model: Model):
    message = (
        '{exc}:\n'
        '  in model {model.name!r} {model}\n'
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
def error(exc: Exception, context: Context, prop: Property, data: dict, manifest: Manifest) -> Property:
    parents = []
    while True:
        message = '  in property {prop.name!r} {prop}\n'
        parents.append(format_error(message, {'prop': prop}))
        if hasattr(prop, 'parent') and isinstance(prop.parent, Property):
            prop = prop.parent
        else:
            break
    message = (
        '{exc}:\n'
        '{parents}'
        '  in model {prop.model.name!r} {prop.model.model}\n'
        "  in file '{prop.model.path}'\n"
        '  on backend {prop.backend.name!r}\n'
    )
    raise Exception(format_error(message, {
        'exc': exc,
        'prop': prop,
        'parents': ''.join(parents)
    }))


@authorize.register()
def authorize(context: Context, action: Action, model: Model):
    check_generated_scopes(context, model.get_type_value(), action.value)


@authorize.register()
def authorize(context: Context, action: Action, prop: Property):
    name = prop.model.get_type_value() + '_' + prop.place
    check_generated_scopes(context, name, action.value)
