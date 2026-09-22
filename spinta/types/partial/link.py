from copy import copy

from spinta import commands
from spinta.components import Context, Model, Property
from spinta.core.enums import Mode
from spinta.exceptions import ParentNodeNotFound, PartialIncorrectProperty, PartialTypeNotFound
from spinta.types import TYPE_OBJECT
from spinta.types.datatype import (
    NESTING_TYPES,
    Array,
    ArrayBackRef,
    BackRef,
    DataType,
    Object,
    Partial,
    Ref,
)
from spinta.types.helpers import set_dtype_backend


def _resolve_model(context: Context, prop: Property, dtype: DataType, *, downgrade: bool = True) -> Model | None:
    """Return ``dtype.model`` as a Model, or None if it can not be resolved.

    ``dtype.model`` is still a model name, when the model owning ``dtype`` was
    not linked yet, which depends on the order models are defined in. If the
    model is not in the manifest at all and `downgrade` is set, ``dtype`` is
    linked here, so that it downgrades itself to an object, exactly as it would
    have, had its own model been linked first.
    """
    model = getattr(dtype, "model", None)
    if isinstance(model, Model):
        return model
    if not isinstance(model, str):
        return None

    manifest = prop.model.manifest
    if commands.has_model(context, manifest, model):
        dtype.model = commands.get_model(context, manifest, model)
        return dtype.model

    if downgrade:
        commands.link(context, dtype)
    return None


def get_ref_value(context: Context, prop: Property) -> Property | None:
    parent = prop.parent
    if not (isinstance(parent, Property) and isinstance(parent.dtype, NESTING_TYPES)):
        return None

    # Parent is always linked before its nested properties, so its model is
    # either resolved already, or parent was downgraded and we never get here.
    model = _resolve_model(context, prop, parent.dtype, downgrade=False)
    if model is None or prop.name not in model.properties:
        return None

    result = model.properties[prop.name]
    # Resolve the model referenced by the taken property, so that deeper levels
    # can be taken too, no matter in which order models are linked.
    _resolve_model(context, prop, result.dtype)
    return result


def _take_dtype_from(context: Context, dtype: Partial, source: Property) -> None:
    prop = dtype.prop
    prop.dtype = copy(source.dtype)
    prop.dtype.properties = dtype.properties
    prop.dtype.inherited = True
    prop.dtype.prop = prop
    prop.given.explicit = False
    prop.given.name = ""
    if prop.level is None:
        prop.level = source.level
    # For external mode copy the external mapping
    # For internal mode leave it as is, because it breaks multi-level denormalization
    if source.external and prop.model.mode == Mode.external:
        prop.external = copy(source.external)
    if isinstance(prop.dtype, Ref):
        # refprops belong to the model the property was taken from, `link`
        # resolves them again for this model.
        prop.dtype.refprops = []
    elif isinstance(prop.dtype, BackRef):
        # refprop points back to the model the property was taken from, `link`
        # has to look it up for this model instead. `_link_backref` compares a
        # given refprop by name, so a copied Property never matches.
        prop.dtype.refprop = None
        prop.dtype.explicit = False
    commands.link(context, prop.dtype)


def _replace_with_object(context: Context, dtype: Partial) -> None:
    """Parent was taken from a `ref`/`backref` whose model is not in the
    manifest and was downgraded to an object, so there is nothing left to take
    the type from. Keep the nested properties as a plain object."""
    prop = dtype.prop
    object_dtype = Object()
    object_dtype.name = TYPE_OBJECT
    object_dtype.type = TYPE_OBJECT
    object_dtype.type_args = []
    object_dtype.prop = prop
    object_dtype.properties = dtype.properties
    object_dtype.inherited = True
    prop.dtype = object_dtype
    prop.given.explicit = False
    prop.given.name = ""
    set_dtype_backend(object_dtype)
    commands.link(context, object_dtype)


def _type_repr(dtype: DataType) -> str:
    # `partial_array` is an internal type, in a manifest it is written as `array`.
    if isinstance(dtype, (Array, ArrayBackRef)):
        return "array"
    return dtype.name


@commands.link.register(Context, Partial)
def link(context: Context, dtype: Partial):
    prop = dtype.prop
    parent = prop.parent

    if not isinstance(parent, Property):
        raise ParentNodeNotFound(
            dtype,
            model_name=prop.model.name,
            property_names=",".join([f"{prop.name}.{i}" for i in dtype.properties.keys()]),
            missing_property_name=prop.name,
        )

    if isinstance(parent.dtype, Object) and parent.dtype.inherited:
        _replace_with_object(context, dtype)
        return

    if not isinstance(parent.dtype, NESTING_TYPES):
        raise PartialTypeNotFound(
            dtype,
            model_name=prop.model.name,
            parent_property_name=parent.given.name or parent.place,
            parent_property_type=_type_repr(parent.dtype),
            property_names=", ".join(
                child.given.name or f"{prop.place}.{name}" for name, child in dtype.properties.items()
            ),
            missing_property_name=prop.given.name or prop.place,
        )

    result = get_ref_value(context, prop)
    if result is None:
        referenced_model = parent.dtype.model
        raise PartialIncorrectProperty(
            dtype,
            property_name=prop.name,
            parent_property_name=parent.given.name or parent.place,
            referenced_model=(referenced_model.name if isinstance(referenced_model, Model) else referenced_model),
        )

    _take_dtype_from(context, dtype, result)
