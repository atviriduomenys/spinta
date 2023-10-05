from spinta import commands
from spinta.components import Context, Model
from spinta.exceptions import ModelReferenceNotFound, ModelReferenceKeyNotFound, NotSupportedBackRefType, \
    MultipleBackRefReferencesFound, NoBackRefReferencesFound
from spinta.types.datatype import BackRef, Ref
from spinta.types.helpers import set_dtype_backend


@commands.link.register(Context, BackRef)
def link(context: Context, dtype: BackRef) -> None:
    set_dtype_backend(dtype)
    backref_model = dtype.prop.model.name if isinstance(dtype.prop.model, Model) else dtype.prop.model
    backref_target_model = dtype.model.name if isinstance(dtype.model, Model) else dtype.model
    if backref_target_model == backref_model:
        # Self reference.
        dtype.model = dtype.prop.model
    else:
        if backref_target_model not in dtype.prop.model.manifest.models:
            raise ModelReferenceNotFound(dtype, ref=backref_target_model)
        dtype.model = dtype.prop.model.manifest.models[backref_target_model]

    if dtype.refprop:
        dtype.explicit = True
        if dtype.refprop in dtype.model.properties.keys():
            refprop = dtype.model.properties[dtype.refprop]
            if not isinstance(refprop.dtype, Ref):
                raise NotSupportedBackRefType(dtype, prop_name=refprop.name, prop_type=refprop.dtype.name)
            else:
                dtype.refprop = refprop
        else:
            raise ModelReferenceKeyNotFound(dtype, ref=dtype.refprop, model=dtype.model)
    else:
        count = 0
        for name, prop in dtype.model.properties.items():
            if isinstance(prop.dtype, Ref):
                target_model = prop.dtype.model.name if isinstance(prop.dtype.model, Model) else prop.dtype.model
                if backref_model == target_model:
                    dtype.refprop = prop
                    count += 1

        if count == 0:
            raise NoBackRefReferencesFound(dtype, model=dtype.model.name)
        elif count > 1:
            raise MultipleBackRefReferencesFound(dtype, model=dtype.model.name)

