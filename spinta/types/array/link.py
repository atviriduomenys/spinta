from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Array
from spinta.types.helpers import set_dtype_backend
from spinta.manifests.tabular.helpers import split_by_uppercase


@commands.link.register(Context, Array)
def link(context: Context, dtype: Array) -> None:
    set_dtype_backend(dtype)
    # In case of a dynamic array, dtype of items is not known.
    if dtype.items:
        commands.link(context, dtype.items.dtype)
    elif isinstance(dtype.refprops, list):
        if dtype.refprops:
            dtype.items = dtype.prop.model.manifest.models[dtype.model].flatprops.get(dtype.refprops[-1]).dtype
        else:
            refprops = split_by_uppercase(dtype.model)
            dtype.items = dtype.prop.model.manifest.models[dtype.model].flatprops.get(refprops[-1]).dtype
