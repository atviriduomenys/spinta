from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Ref
from spinta.exceptions import ModelReferenceNotFound
from spinta.exceptions import ModelReferenceKeyNotFound


@commands.link.register(Context, Ref)
def link(context: Context, dtype: Ref) -> None:
    if dtype.model not in dtype.prop.model.manifest.models:
        raise ModelReferenceNotFound(dtype, ref=dtype.model)
    dtype.model = dtype.prop.model.manifest.models[dtype.model]

    if dtype.refprops:
        refprops = []
        for rprop in dtype.refprops:
            if rprop not in dtype.model.properties:
                raise ModelReferenceKeyNotFound(dtype, ref=rprop, model=dtype.model)
            refprops.append(dtype.model.properties[rprop])
        dtype.refprops = refprops
    else:
        dtype.refprops = [
            dtype.model.properties['_id'],
        ]
