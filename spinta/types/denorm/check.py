from spinta import commands
from spinta.components import Context, Property
from spinta.exceptions import InvalidDenormProperty
from spinta.types.datatype import Denorm, Ref, ExternalRef


@commands.check.register(Context, Denorm)
def check(context: Context, dtype: Denorm):
    parent = dtype.prop.parent
    if isinstance(parent, Property) and (
        isinstance(parent.dtype, Ref) and
        (
            isinstance(parent.dtype, ExternalRef) or parent.dtype.explicit
        )
    ):
        if dtype.rel_prop in parent.dtype.refprops:
            raise InvalidDenormProperty(dtype, denorm=dtype.prop.name, ref=parent.name, refprops=[prop.name for prop in parent.dtype.refprops])

