from spinta.backends.postgresql.ufuncs.query.components import InheritForeignProperty
from spinta.components import Property, FuncProperty
from spinta.core.ufuncs import ufunc, Bind, GetAttr
from spinta.exceptions import PropertyNotFound, FieldNotInResource, LangNotDeclared
from spinta.types.datatype import DataType, Ref, BackRef, Object, Array, ExternalRef, File, Inherit
from spinta.types.text.components import Text
from spinta.ufuncs.components import ForeignProperty
from spinta.ufuncs.propertyresolver.components import PropertyResolver
from spinta.ufuncs.querybuilder.components import ReservedProperty, NestedProperty


@ufunc.resolver(PropertyResolver, Property, object)
def _resolve_property(env: PropertyResolver, prop: Property, attr: object):
    return env.call("_resolve_property", prop.dtype, attr)


@ufunc.resolver(PropertyResolver, DataType, object)
def _resolve_property(env: PropertyResolver, dtype: DataType, attr: object):
    raise Exception("Cannot map", dtype.prop, attr)


@ufunc.resolver(PropertyResolver, Ref, GetAttr)
def _resolve_property(env: PropertyResolver, dtype: Ref, attr: GetAttr):
    if attr.obj in dtype.properties:
        prop = dtype.properties[attr.obj]
    else:
        if attr.obj not in dtype.model.properties:
            raise FieldNotInResource(dtype, property=attr.obj)

        prop = dtype.model.properties[attr.obj]

    if env.ufunc_types:
        fpr = ForeignProperty(None, dtype, prop.dtype)
        return env.call("_resolve_property", fpr, prop, attr.name)
    return env.call("_resolve_property", prop, attr.name)


@ufunc.resolver(PropertyResolver, Ref, Bind)
def _resolve_property(env: PropertyResolver, dtype: Ref, attr: Bind):
    if attr.name in dtype.properties:
        prop = dtype.properties[attr.name]
        return prop

    if attr.name not in dtype.model.properties:
        raise FieldNotInResource(dtype, property=attr.name)
    prop = dtype.model.properties[attr.name]

    if env.ufunc_types:
        # Check for self reference, no need to do joins if table already contains the value
        if attr.name == "_id":
            return ReservedProperty(dtype, attr.name)

        return ForeignProperty(None, dtype, prop.dtype)

    return prop


@ufunc.resolver(PropertyResolver, ExternalRef, Bind)
def _resolve_property(env: PropertyResolver, dtype: Ref, attr: Bind):
    if attr.name in dtype.properties:
        prop = dtype.properties[attr.name]
        return prop

    if attr.name not in dtype.model.properties:
        raise FieldNotInResource(dtype, property=attr.name)
    prop = dtype.model.properties[attr.name]

    if env.ufunc_types:
        # Check for self reference, no need to do joins if table already contains the value
        for refprop in dtype.refprops:
            if refprop.name == attr.name:
                return ReservedProperty(dtype, attr.name)

        return ForeignProperty(None, dtype, prop.dtype)

    return prop


@ufunc.resolver(PropertyResolver, BackRef, GetAttr)
def _resolve_property(
    env: PropertyResolver,
    dtype: BackRef,
    attr: GetAttr,
):
    if attr.obj in dtype.properties:
        prop = dtype.properties[attr.obj]
    else:
        if attr.obj not in dtype.model.properties:
            raise FieldNotInResource(dtype, property=attr.obj)

        prop = dtype.model.properties[attr.obj]

    if env.ufunc_types:
        fpr = ForeignProperty(None, dtype, prop.dtype)
        return env.call("_resolve_property", fpr, prop.dtype, attr.name)

    return env.call("_resolve_property", prop, attr.name)


@ufunc.resolver(PropertyResolver, BackRef, Bind)
def _resolve_property(env, dtype, attr):
    if attr.obj in dtype.properties:
        return dtype.properties[attr.obj]

    if attr.obj not in dtype.model.properties:
        raise FieldNotInResource(dtype, property=attr.obj)

    prop = dtype.model.properties[attr.obj]
    if env.ufunc_types:
        fpr = ForeignProperty(None, dtype, prop.dtype)
        return env.call("_resolve_property", fpr, prop.dtype, attr.name)

    return prop


@ufunc.resolver(PropertyResolver, Object, GetAttr)
def _resolve_property(
    env: PropertyResolver,
    dtype: Object,
    attr: GetAttr,
):
    if attr.obj in dtype.properties:
        prop = dtype.properties[attr.obj]

        if env.ufunc_types:
            return NestedProperty(left=dtype, right=env.call("_resolve_property", prop.dtype, attr.name))

        return env.call("_resolve_property", prop, attr.name)
    raise FieldNotInResource(dtype, property=attr.obj)


@ufunc.resolver(PropertyResolver, Object, Bind)
def _resolve_property(env: PropertyResolver, dtype: Object, attr: Bind):
    if attr.name in dtype.properties:
        prop = dtype.properties[attr.name]

        if env.ufunc_types:
            return NestedProperty(left=dtype, right=prop.dtype)

        return prop
    else:
        raise FieldNotInResource(dtype, property=attr.name)


@ufunc.resolver(PropertyResolver, Array, (Bind, GetAttr))
def _resolve_property(env, dtype, attr):
    if env.ufunc_types:
        return NestedProperty(left=dtype, right=env.call("_resolve_property", dtype.items.dtype, attr))

    return env.call("_resolve_property", dtype.items, attr)


@ufunc.resolver(PropertyResolver, File, Bind)
def _resolve_property(env, dtype, attr):
    return ReservedProperty(dtype, attr.name)


@ufunc.resolver(PropertyResolver, Text, Bind)
def _resolve_property(env: PropertyResolver, dtype: Text, bind: Bind):
    if dtype.prop.name in env.model.properties:
        prop = env.model.properties[dtype.prop.name]
        if bind.name in prop.dtype.langs:
            return prop.dtype.langs[bind.name].dtype
        raise LangNotDeclared(dtype, lang=bind.name)


@ufunc.resolver(PropertyResolver, Inherit, Bind)
def _resolve_property(env, dtype, attr):
    return InheritForeignProperty(dtype.prop.model, attr.name, dtype.prop)


@ufunc.resolver(PropertyResolver, ForeignProperty, Property, object)
def _resolve_property(
    env: PropertyResolver,
    fpr: ForeignProperty,
    prop: Property,
    obj: object,
) -> ForeignProperty:
    return env.call("_resolve_property", fpr, prop.dtype, obj)


@ufunc.resolver(PropertyResolver, ForeignProperty, Ref, GetAttr)
def _resolve_property(
    env: PropertyResolver,
    fpr: ForeignProperty,
    dtype: Ref,
    attr: GetAttr,
) -> ForeignProperty:
    prop = dtype.model.properties[attr.obj]
    fpr = fpr.push(prop)
    return env.call("_resolve_property", fpr, prop.dtype, attr.name)


@ufunc.resolver(PropertyResolver, ForeignProperty, Ref, Bind)
def _resolve_property(
    env: PropertyResolver,
    fpr: ForeignProperty,
    dtype: Ref,
    attr: Bind,
) -> ForeignProperty:
    prop = dtype.model.properties[attr.name]
    return fpr.push(prop)


@ufunc.resolver(PropertyResolver, ForeignProperty, BackRef, GetAttr)
def _resolve_property(
    env: PropertyResolver,
    fpr: ForeignProperty,
    dtype: BackRef,
    attr: GetAttr,
) -> ForeignProperty:
    prop = dtype.model.properties[attr.obj]
    fpr = fpr.push(prop)
    return env.call("_resolve_property", fpr, prop.dtype, attr.name)


@ufunc.resolver(PropertyResolver, ForeignProperty, BackRef, Bind)
def _resolve_property(
    env: PropertyResolver,
    fpr: ForeignProperty,
    dtype: BackRef,
    attr: Bind,
) -> ForeignProperty:
    prop = dtype.model.properties[attr.name]
    return fpr.push(prop)


@ufunc.resolver(PropertyResolver, Bind)
def _resolve_property(env: PropertyResolver, bind: Bind):
    return env.call("_resolve_property", bind.name)


@ufunc.resolver(PropertyResolver, GetAttr)
def _resolve_property(env: PropertyResolver, attr: GetAttr):
    model = env.model
    place = str(attr)

    # Skip mapping if we don't need custom types
    if place in model.flatprops and not env.ufunc_types:
        return model.flatprops[place]

    prop = env.call("_resolve_property", attr.obj)
    return env.call("_resolve_property", prop, attr.name)


@ufunc.resolver(PropertyResolver, str)
def _resolve_property(env: PropertyResolver, prop: str):
    if prop in env.model.flatprops:
        return env.model.flatprops.get(prop)

    raise PropertyNotFound(env.model, property=prop)


@ufunc.resolver(PropertyResolver, DataType)
def _resolve_property(env: PropertyResolver, dtype: DataType):
    return dtype.prop


@ufunc.resolver(PropertyResolver, Property)
def _resolve_property(env: PropertyResolver, prop: Property):
    return prop


@ufunc.resolver(PropertyResolver, ForeignProperty)
def _resolve_property(env: PropertyResolver, prop: ForeignProperty):
    return prop


@ufunc.resolver(PropertyResolver, FuncProperty)
def _resolve_property(env: PropertyResolver, prop: FuncProperty):
    return prop
