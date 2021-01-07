from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Object
from spinta.types.helpers import set_dtype_backend


@commands.link.register(Context, Object)
def link(context: Context, dtype: Object) -> None:
    set_dtype_backend(dtype)
    for prop in dtype.properties.values():
        commands.link(context, prop.dtype)
