from copy import copy

from spinta import commands
from spinta.components import Context, Model, Property
from spinta.core.enums import Mode
from spinta.exceptions import ParentNodeNotFound, PartialIncorrectProperty, PartialTypeNotFound
from spinta.types.datatype import BackRef, Object, Partial, Ref


def get_referenced_properties(context: Context, node: Property) -> tuple[Model, dict[str, Property]] | None:
    """Model and properties a property declared under `node` is looked up in.

    `node` is a reference, `salis` or `miestai[]`, or an object reached through one, `adresas` of
    `salis.adresas.gatve`. Anything else is `None`.
    """
    dtype = node.dtype
    if isinstance(dtype, Ref | BackRef):
        if isinstance(dtype.model, str):
            dtype.model = commands.get_model(context, node.model.manifest, dtype.model)
        return dtype.model, dtype.model.properties
    if isinstance(dtype, Object) and isinstance(node.parent, Property):
        referenced = get_referenced_properties(context, node.parent)
        target = referenced[1].get(node.name) if referenced else None
        if target is not None and isinstance(target.dtype, Object):
            return referenced[0], target.dtype.properties
    return None


def get_ref_value(context: Context, prop: Property) -> Property | None:
    parent = prop.parent
    referenced = get_referenced_properties(context, parent) if isinstance(parent, Property) else None
    if referenced is None or prop.name not in referenced[1]:
        return None

    model_property_value = referenced[1][prop.name]
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
        if get_referenced_properties(context, parent) is not None:
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
