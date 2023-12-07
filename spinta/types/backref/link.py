from spinta import commands
from spinta.components import Context, Model, Property
from spinta.exceptions import ModelReferenceNotFound, MultipleBackRefReferencesFound, NoBackRefReferencesFound, \
    NoReferencesFound, OneToManyBackRefNotSupported
from spinta.types.datatype import BackRef, Ref, Array, ArrayBackRef, Object, Denorm, DataType
from spinta.types.helpers import set_dtype_backend


@commands.find_backref_ref.register(Model, str, object)
def find_backref_ref(model: Model, backref_model: str, given_ref: object):
    for prop in model.properties.values():
        result = commands.find_backref_ref(prop, backref_model, given_ref)
        if result is not None:
            yield from result


@commands.find_backref_ref.register(Property, str, object)
def find_backref_ref(prop: Property, backref_model: str, given_ref: object):
    result = commands.find_backref_ref(prop.dtype, backref_model, given_ref)
    if result is not None:
        yield from result


@commands.find_backref_ref.register(DataType, str, object)
def find_backref_ref(dtype: DataType, backref_model: str, given_ref: str):
    pass


@commands.find_backref_ref.register(Ref, str, object)
def find_backref_ref(dtype: Ref, backref_model: str, given_ref: object):
    target_model = dtype.model.name if isinstance(dtype.model, Model) else dtype.model
    if target_model == backref_model:
        if given_ref and given_ref == dtype.prop.name or given_ref is None:
            yield dtype.prop


@commands.find_backref_ref.register(Array, str, object)
def find_backref_ref(dtype: Array, backref_model: str, given_ref: object):
    if dtype.items:
        result = commands.find_backref_ref(dtype.items.dtype, backref_model, given_ref)
        if result is not None:
            yield from result


@commands.find_backref_ref.register(Object, str, object)
def find_backref_ref(dtype: Object, backref_model: str, given_ref: object):
    for obj_prop in dtype.properties.values():
        result = commands.find_backref_ref(obj_prop.dtype, backref_model, given_ref)
        if result is not None:
            yield from result


@commands.find_backref_ref.register(Denorm, str, object)
def find_backref_ref(dtype: Denorm, backref_model: str, given_ref: object):
    result = commands.find_backref_ref(dtype.rel_prop.dtype, backref_model, given_ref)
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
    given_refprop = dtype.refprop
    if dtype.refprop:
        dtype.explicit = True

    count = 0
    result = list(commands.find_backref_ref(dtype.model, backref_model, given_refprop))
    if not result:
        if given_refprop is not None:
            raise NoReferencesFound(dtype, prop_name=given_refprop, model=dtype.model)
        raise NoBackRefReferencesFound(dtype, model=dtype.model.name)
    for prop in result:
        if count < 1:
            if given_refprop and prop.name == given_refprop or given_refprop is None:
                dtype.refprop = prop
                count += 1
        else:
            raise MultipleBackRefReferencesFound(dtype, model=dtype.model.name)


@commands.link.register(Context, BackRef)
def link(context: Context, dtype: BackRef) -> None:
    _link_backref(context, dtype)

    if dtype.refprop.list is not None:
        raise OneToManyBackRefNotSupported(dtype)

    # relationship needs to add unique
    dtype.refprop.dtype.unique = True


@commands.link.register(Context, ArrayBackRef)
def link(context: Context, dtype: ArrayBackRef):
    _link_backref(context, dtype)
