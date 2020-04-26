from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Ref


@commands.link.register(Context, Ref)
def link(context: Context, dtype: Ref) -> None:
    dtype.model = dtype.prop.model.manifest.models[dtype.model]

    if dtype.rkeys:
        dtype.rkeys = [
            dtype.model.properties[rkey]
            for rkey in dtype.rkeys
        ]
    else:
        dtype.rkeys = None
