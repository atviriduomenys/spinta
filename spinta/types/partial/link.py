from copy import copy

from spinta import commands
from spinta.components import Context
from spinta.exceptions import PartialTypeNotFound
from spinta.types.datatype import Partial, Ref


@commands.link.register(Context, Partial)
def link(context: Context, dtype: Partial):
    prop = dtype.prop
    if prop.parent:
        if isinstance(prop.parent.dtype, Ref):
            props = prop.dtype.properties
            prop.dtype = copy(prop.parent.dtype.model.properties[prop.name].dtype)
            prop.dtype.properties = props
            prop.given.name = ''

            for partial_prop in props.values():
                partial_prop.model = prop.parent.dtype.model
                commands.link(context, partial_prop)

        else:
            raise PartialTypeNotFound(dtype)
    else:
        raise PartialTypeNotFound(dtype)
