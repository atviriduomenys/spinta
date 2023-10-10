from spinta import commands
from spinta.components import Context, Model, Property
from spinta.exceptions import ModelReferenceNotFound, ModelReferenceKeyNotFound, NotSupportedBackRefType, \
    MultipleBackRefReferencesFound, NoBackRefReferencesFound
from spinta.types.datatype import BackRef, Ref, Array, ArrayBackRef, Object, Denorm, DataType
from spinta.types.helpers import set_dtype_backend


@commands.find_backref_ref.register(Model, str)
def find_backref_ref(model: Model, backref_model: str):
    for prop in model.properties.values():
        result = commands.find_backref_ref(prop, backref_model)
        if result is not None:
            yield from result


@commands.find_backref_ref.register(Property, str)
def find_backref_ref(prop: Property, backref_model: str):
    result = commands.find_backref_ref(prop.dtype, backref_model)
    if result is not None:
        yield from result


@commands.find_backref_ref.register(DataType, str)
def find_backref_ref(dtype: DataType, backref_model: str):
    pass


@commands.find_backref_ref.register(Ref, str)
def find_backref_ref(dtype: Ref, backref_model: str):
    target_model = dtype.model.name if isinstance(dtype.model, Model) else dtype.model
    if target_model == backref_model:
        yield dtype.prop


@commands.find_backref_ref.register(Array, str)
def find_backref_ref(dtype: Array, backref_model: str):
    if dtype.items:
        result = commands.find_backref_ref(dtype.items.dtype, backref_model)
        if result is not None:
            yield from result


@commands.find_backref_ref.register(Object, str)
def find_backref_ref(dtype: Object, backref_model: str):
    for obj_prop in dtype.properties.values():
        result = commands.find_backref_ref(obj_prop.dtype, backref_model)
        if result is not None:
            yield from result


@commands.find_backref_ref.register(Denorm, str)
def find_backref_ref(dtype: Denorm, backref_model: str):
    result = commands.find_backref_ref(dtype.rel_prop.dtype, backref_model)
    if result is not None:
        yield from result


def _link_backref(context: Context, dtype: BackRef):
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
        result = commands.find_backref_ref(dtype.model, backref_model)
        if result is None:
            raise NoBackRefReferencesFound(dtype, model=dtype.model.name)
        for prop in result:
            if count < 1:
                dtype.refprop = prop
                count += 1
            else:
                raise MultipleBackRefReferencesFound(dtype, model=dtype.model.name)


@commands.link.register(Context, BackRef)
def link(context: Context, dtype: BackRef) -> None:
    _link_backref(context, dtype)

    # x to one relationship need to add unique
    dtype.refprop.dtype.unique = True


@commands.link.register(Context, ArrayBackRef)
def link(context: Context, dtype: ArrayBackRef):
    _link_backref(context, dtype)
