from spinta.commands import load, check, error, authorize, prepare
from spinta.components import Context, Manifest, Node, Model, Property
from spinta.nodes import load_node
from spinta.types.type import Type, load_type
from spinta.utils.errors import format_error
from spinta.auth import check_generated_scopes
from spinta.utils.schema import resolve_schema


@load.register()
def load(context: Context, model: Model, data: dict, manifest: Manifest) -> Model:
    load_node(context, model, data, manifest)

    # 'type' is reserved for object type.
    props = {'type': {'type': 'string'}}
    props.update(data.get('properties') or {})

    # 'id' is reserved for primary key.
    props['id'] = props.get('id') or {'type': 'string'}
    if props['id'].get('type') is None or props['id'].get('type') == 'pk':
        props['id'] == 'string'

    model.properties = {}
    for name, prop in props.items():
        prop = {
            'name': name,
            'path': model.path,
            'parent': model,
            **prop,
        }
        model.properties[name] = load(context, Property(), prop, manifest)

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
        raise Exception("Unknown prams: %s" % ', '.join(map(repr, sorted(unknown_params))))

    return prop


@load.register()
def load(context: Context, model: Model, data: dict) -> dict:
    for name, prop in model.properties.items():
        if name in data:
            data_value = data[name]
            # XXX: rewrite into command instead of type method?
            data[name] = prop.type.load(data_value)
    return data


@check.register()
def check(context: Context, model: Model):
    if 'id' not in model.properties:
        context.error("Primary key is required, add `id` property to the model.")
    if model.properties['id'].type == 'pk':
        context.deprecation("`id` property must specify real type like 'string' or 'integer'. Use of 'pk' is deprecated.")


@check.register()
def check(context: Context, model: Model, data: dict):
    for name, prop in model.properties.items():
        if name in data:
            data_value = data[name]
            if not prop.type.is_valid(data_value):
                raise Exception(f"{data_value} is not valid type: {prop.type}")


@prepare.register()
def prepare(context: Context, model: Model, data: dict) -> dict:
    # prepares model's data for storing in Mongo
    backend = model.backend
    for name, prop in model.properties.items():
        if name in data:
            data_value = data[name]
            data[name] = prepare(context, prop.type, backend, data_value)
    return data


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
def authorize(context: Context, action: str, model: Model, *, data=None):
    check_generated_scopes(context, model.get_type_value(), action, data=data)
