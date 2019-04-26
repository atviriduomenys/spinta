from spinta.commands import load, check, error
from spinta.components import Context, Manifest, Model, Property
from spinta.nodes import load_node
from spinta.types.type import load_type
from spinta.utils.errors import format_error


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
    prop.type = load_type(context, prop, data)

    # Check if there any unknown params were given.
    known_params = set(prop.schema.keys()) | set(prop.type.schema.keys())
    given_params = set(data.keys())
    unknown_params = given_params - known_params
    if unknown_params:
        raise Exception("Unknown prams: %s" % ', '.join(map(repr, sorted(unknown_params))))

    return prop


@check.register()
def check(context: Context, model: Model):
    if 'id' not in model.properties:
        context.error("Primary key is required, add `id` property to the model.")
    if model.properties['id'].type == 'pk':
        context.deprecation("`id` property must specify real type like 'string' or 'integer'. Use of 'pk' is deprecated.")


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
