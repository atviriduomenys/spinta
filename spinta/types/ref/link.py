from typing import List

from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Ref
from spinta.exceptions import ModelReferenceNotFound, MissingRefModel
from spinta.exceptions import ModelReferenceKeyNotFound
from spinta.types.helpers import set_dtype_backend


@commands.link.register(Context, Ref)
def link(context: Context, dtype: Ref) -> None:
    set_dtype_backend(dtype)

    # XXX: https://github.com/atviriduomenys/spinta/issues/44
    rmodel: str = dtype.model
    if rmodel is None:
        raise MissingRefModel(dtype, model_name=dtype.prop.model.basename, property_name=dtype.prop.name, property_type=dtype.prop.dtype.name)
    if rmodel == dtype.prop.model.name:
        # Self reference.
        dtype.model = dtype.prop.model
    else:
        if not commands.has_model(context, dtype.prop.model.manifest, rmodel):
            raise ModelReferenceNotFound(dtype, ref=rmodel)
        dtype.model = commands.get_model(context, dtype.prop.model.manifest, rmodel)

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

    if dtype.model.external and dtype.refprops != dtype.model.external.pkeys:
        dtype.model.add_keymap_property_combination(dtype.refprops)

    if dtype.properties:
        for denorm_prop in dtype.properties.values():
            #denorm_prop.model = dtype.model
            commands.link(context, denorm_prop)

