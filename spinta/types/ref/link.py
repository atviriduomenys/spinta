from typing import List

from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Ref
from spinta.exceptions import ModelReferenceNotFound
from spinta.exceptions import ModelReferenceKeyNotFound
from spinta.types.helpers import set_dtype_backend


@commands.link.register(Context, Ref)
def link(context: Context, dtype: Ref) -> None:
    set_dtype_backend(dtype)

    # XXX: https://gitlab.com/atviriduomenys/spinta/-/issues/44
    referenced_model: str = dtype.model

    if referenced_model == dtype.prop.model.name:
        # Self reference.
        dtype.model = dtype.prop.model
    else:
        if referenced_model not in dtype.prop.model.manifest.models:
            raise ModelReferenceNotFound(dtype, ref=referenced_model)
        dtype.model = dtype.prop.model.manifest.models[referenced_model]

    if dtype.refprops:
        refprops = []
        # XXX: https://gitlab.com/atviriduomenys/spinta/-/issues/44
        raw_refprops: List[str] = dtype.refprops
        for rprop in raw_refprops:
            if rprop not in dtype.model.properties:
                raise ModelReferenceKeyNotFound(dtype, ref=rprop, model=dtype.model)
            refprops.append(dtype.model.properties[rprop])
        dtype.refprops = refprops
    elif dtype.model.external:
        dtype.refprops = [*dtype.model.external.pkeys]
    else:
        dtype.refprops = [dtype.model.properties['_id']]
