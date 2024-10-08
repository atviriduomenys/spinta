from copy import copy

from spinta import commands
from spinta.components import Context, Property
from spinta.exceptions import PartialTypeNotFound
from spinta.types.datatype import Partial, Ref


def get_ref_value(context: Context, prop: Property):
    parent = prop.parent
    if isinstance(parent, Property) and isinstance(parent.dtype, Ref):
        parent_parent = parent.parent
        if isinstance(parent_parent, Property) and isinstance(parent_parent.dtype, Ref):
            model = parent_parent.dtype.model
            first_level = model.properties[parent.name]
            if prop.name in first_level.dtype.properties:
                return first_level.dtype.properties[prop.name]
        else:
            model = parent.dtype.model
            if prop.name in model.properties:
                return model.properties[prop.name]


@commands.link.register(Context, Partial)
def link(context: Context, dtype: Partial):
    prop = dtype.prop
    parent = prop.parent
    if parent and isinstance(parent, Property):
        if isinstance(parent.dtype, Ref):
            props = dtype.properties
            result = get_ref_value(context, prop)
            prop.dtype = copy(result.dtype)
            prop.dtype.properties = props
            prop.dtype.inherited = True
            prop.given.explicit = False
            prop.given.name = ''
            prop.dtype.prop = prop
            for partial_prop in props.values():
                commands.link(context, partial_prop)
        else:
            raise PartialTypeNotFound(dtype)
    else:
        raise PartialTypeNotFound(dtype)
