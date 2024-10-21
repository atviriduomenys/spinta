from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Array, PartialArray, ArrayBackRef
from spinta.types.helpers import set_dtype_backend


@commands.link.register(Context, Array)
def link(context: Context, dtype: Array) -> None:
    set_dtype_backend(dtype)
    # In case of a dynamic array, dtype of items is not known.
    if dtype.items:
        commands.link(context, dtype.items.dtype)


@commands.link.register(Context, PartialArray)
def link(context: Context, dtype: PartialArray):
    dtype.prop.given.name = ''
    super_ = commands.link[Context, Array]
    return super_(context, dtype)


@commands.link.register(Context, ArrayBackRef)
def link(context: Context, dtype: ArrayBackRef):
    dtype.prop.given.name = ''
    super_ = commands.link[Context, Array]
    return super_(context, dtype)