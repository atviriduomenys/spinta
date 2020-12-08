from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Array


@commands.link.register(Context, Array)
def link(context: Context, dtype: Array) -> None:
    # In case of a dynamic array, dtype of items is not known.
    if dtype.items:
        commands.link(context, dtype.items.dtype)
