from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Array


@commands.link.register(Context, Array)
def link(context: Context, dtype: Array) -> None:
    commands.link(context, dtype.items.dtype)
