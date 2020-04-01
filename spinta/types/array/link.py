from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Array


@commands.link.register(Context, Array)
def link(context: Context, dtype: Array) -> None:
    store = context.get('store')
    dtype.backend = store.backends[dtype.backend]
    commands.link(context, dtype.items.dtype)
