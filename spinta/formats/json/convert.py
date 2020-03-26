from spinta import commands
from spinta.core.context import Context
from spinta.components.node import Model
from spinta.formats.component import Json


@commands.to_native.register()
def to_native(context: Context, model: Model, source: Json, data: dict) -> dict:
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
