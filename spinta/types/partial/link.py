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
            prop.dtype = prop.parent.dtype.models.properties[prop.name].copy()
            prop.dtype.properties = props
        else:
            raise PartialTypeNotFound(dtype)
    else:
        raise PartialTypeNotFound(dtype)
