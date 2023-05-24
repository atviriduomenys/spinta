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

    # XXX: https://github.com/atviriduomenys/spinta/issues/44
    rmodel: str = dtype.model
    if rmodel == dtype.prop.model.name:
        # Self reference.
        dtype.model = dtype.prop.model
    else:
        if rmodel not in dtype.prop.model.manifest.models:
            raise ModelReferenceNotFound(dtype, ref=rmodel)
        dtype.model = dtype.prop.model.manifest.models[rmodel]

    if dtype.refprops:
        refprops = []
        # XXX: https://github.com/atviriduomenys/spinta/issues/44
        raw_refprops: List[str] = dtype.refprops
        for rprop in raw_refprops:
            if rprop not in dtype.model.properties:
                raise ModelReferenceKeyNotFound(dtype, ref=rprop, model=dtype.model)
            refprops.append(dtype.model.properties[rprop])
        dtype.refprops = refprops
        dtype.explicit = True
    elif dtype.model.external:
        dtype.refprops = [*dtype.model.external.pkeys]
    else:
        dtype.refprops = [dtype.model.properties['_id']]
