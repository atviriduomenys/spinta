from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Object


@commands.link.register(Context, Object)
def link(context: Context, dtype: Object) -> None:
    for prop in dtype.properties.values():
        commands.link(context, prop.dtype)
