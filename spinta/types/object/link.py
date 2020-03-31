from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Object


@commands.link.register(Context, Object)
def link(context: Context, dtype: Object) -> None:
    store = context.get('store')
    dtype.backend = store.backends[dtype.backend]
    for prop in dtype.properties.values():
        commands.link(context, prop.dtype)
