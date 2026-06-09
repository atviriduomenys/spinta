from copy import copy

from spinta import commands
from spinta.components import Context, Property
from spinta.core.enums import Mode
from spinta.exceptions import PartialTypeNotFound, ParentNodeNotFound, PartialIncorrectProperty
from spinta.types.datatype import Partial, Ref


def get_ref_value(context: Context, prop: Property) -> Property | None:
    parent = prop.parent
    if not (isinstance(parent, Property) and isinstance(parent.dtype, Ref)):
        return None
    if isinstance(parent.dtype.model, str):
        parent.dtype.model = commands.get_model(context, prop.model.manifest, parent.dtype.model)
    model = parent.dtype.model

    if prop.name not in model.properties:
        return None

    model_property_value = model.properties[prop.name]
    model_dtype = model_property_value.dtype
    if hasattr(model_dtype, "model") and isinstance(model_dtype.model, str):
        model_property_value.dtype.model = commands.get_model(
            context, prop.model.manifest, model_property_value.dtype.model
        )
    return model_property_value


@commands.link.register(Context, Partial)
def link(context: Context, dtype: Partial):
    prop = dtype.prop
    parent = prop.parent
    if parent and isinstance(parent, Property):
        if isinstance(parent.dtype, Ref):
            props = dtype.properties
            result = get_ref_value(context, prop)
            if not result:
                raise PartialIncorrectProperty(dtype)
            prop.dtype = copy(result.dtype)
            prop.dtype.properties = props
            prop.dtype.inherited = True
            prop.given.explicit = False
            prop.given.name = ""
            prop.dtype.prop = prop
            if prop.level is None:
                prop.level = result.level
            # For external mode copy the external mapping
            # For internal mode leave it as is, because it breaks multi-level denormalization
            if result.external and prop.model.mode == Mode.external:
                prop.external = copy(result.external)
            if isinstance(prop.dtype, Ref):
                prop.dtype.refprops = []
            commands.link(context, prop.dtype)
        else:
            raise PartialTypeNotFound(dtype)
    else:
        raise ParentNodeNotFound(
            dtype,
            model_name=prop.model.name,
            property_names=",".join([f"{prop.name}.{i}" for i in dtype.properties.keys()]),
            missing_property_name=prop.name,
        )
