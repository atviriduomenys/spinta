from spinta import commands
from spinta.components import Context, Property, Model
from spinta.exceptions import NoRefPropertyForDenormProperty, ReferencedPropertyNotFound
from spinta.types.datatype import Denorm, Ref, Object, Array
from spinta.types.helpers import set_dtype_backend


@commands.link.register(Context, Denorm)
def link(context: Context, dtype: Denorm) -> None:
    set_dtype_backend(dtype)

    dtype.rel_prop = _get_denorm_prop(
        context,
        dtype.prop.name,
        dtype.prop,
        dtype.prop.model
    )


# TODO: Add better support for denorm when nested (with object type, etc.)
def _get_denorm_prop(
    context: Context,
    name: str,
    prop: Property,
    model: Model,
) -> Property:
    manifest = model.manifest
    name_parts = name.split('.', 1)
    name = name_parts[0]
    properties = prop.parent.dtype.model.properties if isinstance(prop.parent.dtype, Ref) else prop.parent.model.properties
    model = commands.get_model(context, manifest, prop.parent.dtype.model.name) if isinstance(prop.parent.dtype, Ref) else model
    if len(name_parts) > 1:
        ref_prop = properties[name]
        while isinstance(ref_prop.dtype, Array):
            ref_prop = ref_prop.dtype.items

        model = commands.get_model(context, manifest, ref_prop.dtype.model.name) if isinstance(ref_prop.dtype, Ref) else model
        if name not in properties or not isinstance(ref_prop.dtype, (Ref, Object)):
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
            denorm_prop = _get_denorm_prop(context, name_parts[1], prop, model)
    else:
        if name not in properties:
            raise ReferencedPropertyNotFound(
                prop,
                ref={'property': name, 'model': model.name},
            )
        denorm_prop = properties[name]
    return denorm_prop
