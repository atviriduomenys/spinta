from spinta.commands import load, check
from spinta.components import Context, Manifest, Model, Property
from spinta.nodes import load_node


@load.register()
def load(context: Context, model: Model, data: dict, manifest: Manifest) -> Model:
    load_node(context, model, data, manifest)
    props = {'type': {}}
    props.update(data.get('properties') or {})
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
    return load_node(context, prop, data, manifest)


@check.register()
def check(context: Context, model: Model):
    if 'id' not in model.properties:
        context.error("Primary key is required, add `id` property to the model.")
    if model.properties['id'].type == 'pk':
        context.deprecation("`id` property must specify real type like 'string' or 'integer'. Use of 'pk' is deprecated.")
