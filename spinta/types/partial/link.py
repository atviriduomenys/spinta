from copy import copy

from spinta import commands
from spinta.components import Context, Property
from spinta.exceptions import PartialTypeNotFound, ParentNodeNotFound, PartialIncorrectProperty
from spinta.types.datatype import Partial, Ref


def get_ref_value(context: Context, prop: Property):
    parent = prop.parent
    if isinstance(parent, Property) and isinstance(parent.dtype, Ref):
        if isinstance(parent.dtype.model, str):
            parent.dtype.model = commands.get_model(context, prop.model.manifest, parent.dtype.model)
        model = parent.dtype.model

        if prop.name in model.properties:
            model_dtype = model.properties[prop.name].dtype
            if hasattr(model_dtype, "model") and isinstance(model_dtype.model, str):
                model.properties[prop.name].dtype.model = commands.get_model(
                    context, prop.model.manifest, model.properties[prop.name].dtype.model
                )
            return model.properties[prop.name]


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
            if result.external:
                prop.external = copy(result.external)
            for partial_prop in props.values():
                commands.link(context, partial_prop)
        else:
            raise PartialTypeNotFound(dtype)
    else:
        raise ParentNodeNotFound(
            dtype,
            model_name=prop.model.name,
            property_names=",".join([f"{prop.name}.{i}" for i in dtype.properties.keys()]),
            missing_property_name=prop.name,
        )
