from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Array
from spinta.types.helpers import set_dtype_backend


@commands.link.register(Context, Array)
def link(context: Context, dtype: Array) -> None:
    set_dtype_backend(dtype)
    # In case of a dynamic array, dtype of items is not known.
    if dtype.items:
        commands.link(context, dtype.items.dtype)
