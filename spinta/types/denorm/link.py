from spinta import commands
from spinta.components import Context, Property, Model
from spinta.exceptions import NoRefPropertyForDenormProperty, ReferencedPropertyNotFound
from spinta.types.datatype import Denorm, Ref, Object
from spinta.types.helpers import set_dtype_backend


@commands.link.register(Context, Denorm)
def link(context: Context, dtype: Denorm) -> None:
    set_dtype_backend(dtype)

    dtype.rel_prop = _get_denorm_prop(
        dtype.prop.name,
        dtype.prop,
        dtype.prop.model
    )


def _get_denorm_prop(
    name: str,
    prop: Property,
    model: Model,
) -> Property:
    models = model.manifest.models
    name_parts = name.split('.', 1)
    name = name_parts[0]
    properties = model.properties
    if len(name_parts) > 1:
        if name not in properties or not isinstance(properties[name].dtype, (Ref, Object)):
            if prop.model == model:
                raise NoRefPropertyForDenormProperty(
                    prop,
                    ref=name,
                    prop=prop.name,
                )
            else:
                raise ReferencedPropertyNotFound(
                    prop,
                    ref={'property': name, 'model': model.name},
                )
        else:
            ref_prop = properties[name]
            model = models[ref_prop.dtype.model.name]
            denorm_prop = _get_denorm_prop(name_parts[1], prop, model)
    else:
        if name not in properties:
            raise ReferencedPropertyNotFound(
                prop,
                ref={'property': name, 'model': model.name},
            )
        denorm_prop = properties[name]
    return denorm_prop
