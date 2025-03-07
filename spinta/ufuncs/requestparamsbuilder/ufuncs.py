from typing import Union

from spinta.components import Property, FuncProperty
from spinta.core.ufuncs import ufunc, Expr, Bind, GetAttr
from spinta.types.datatype import Integer, String, DataType, Denorm
from spinta.ufuncs.requestparamsbuilder.components import RequestParamsBuilder
from spinta.ufuncs.requestparamsbuilder.helpers import disable_params_pagination


@ufunc.resolver(RequestParamsBuilder, Bind, Bind, name='getattr')
def getattr_(
    env: RequestParamsBuilder,
    field: Bind,
    attr: Bind
):
    return GetAttr(field.name, attr)


@ufunc.resolver(RequestParamsBuilder, Bind, GetAttr, name='getattr')
def getattr_(
    env: RequestParamsBuilder,
    obj: Bind,
    attr: GetAttr
):
    return GetAttr(obj.name, attr)


@ufunc.resolver(RequestParamsBuilder, GetAttr, Bind, name='getattr')
def getattr_(
    env: RequestParamsBuilder,
    obj: GetAttr,
    attr: Bind
):
    leaf = env.call('getattr', obj.name, attr)
    return GetAttr(obj.obj, leaf)


@ufunc.resolver(RequestParamsBuilder, Expr)
def _resolve_empty_dtype(
    env: RequestParamsBuilder,
    expr: Expr,
):
    args, kwargs = expr.resolve(env)
    print(args, kwargs, args[0], type(args[0]))
    prop = env.resolve_property(*args, **kwargs)
    print(prop)
    return env.call('_resolve_empty_dtype', prop)


@ufunc.resolver(RequestParamsBuilder, Property)
def _resolve_empty_dtype(
    env: RequestParamsBuilder,
    prop: Property,
):
    return env.call('_resolve_empty_dtype', prop.dtype)


@ufunc.resolver(RequestParamsBuilder, FuncProperty)
def _resolve_empty_dtype(
    env: RequestParamsBuilder,
    prop: FuncProperty,
):
    return env.call('_resolve_empty_dtype', prop.prop)


@ufunc.resolver(RequestParamsBuilder, DataType)
def _resolve_empty_dtype(
    env: RequestParamsBuilder,
    dtype: DataType,
):
    new_dtype = dtype.__copy__()
    new_dtype.prop = None
    return new_dtype


@ufunc.resolver(RequestParamsBuilder, Denorm)
def _resolve_empty_dtype(
    env: RequestParamsBuilder,
    dtype: Denorm,
):
    return env.call('_resolve_empty_dtype', dtype.rel_prop)


#
#
# @ufunc.resolver(RequestParamsBuilder, GetAttr)
# def _resolve_property(
#     env: RequestParamsBuilder,
#     attr: GetAttr
# ):
#     model = env.params.model
#     place = str(attr)
#     print(place, model.flatprops.keys())
#     if place in model.flatprops:
#         return model.flatprops[place]
#
#     prop = env.call('_resolve_property', attr.obj)
#     return env.call('_resolve_property', prop, attr.name)
#
#
# @ufunc.resolver(RequestParamsBuilder, Property, object)
# def _resolve_property(
#     env: RequestParamsBuilder,
#     prop: Property,
#     attr: object
# ):
#     return env.call('_resolve_property', prop.dtype, attr)
#
#
# @ufunc.resolver(RequestParamsBuilder, DataType, object)
# def _resolve_property(
#     env: RequestParamsBuilder,
#     dtype: DataType,
#     attr: object
# ):
#     raise Exception("Cannot map", dtype.prop, attr)
#
#
# @ufunc.resolver(RequestParamsBuilder, Ref, GetAttr)
# def _resolve_property(
#     env: RequestParamsBuilder,
#     dtype: Ref,
#     attr: GetAttr
# ):
#     if attr.obj in dtype.properties:
#         prop = dtype.properties[attr.obj]
#     else:
#         prop = dtype.model.properties[attr.obj]
#     return env.call('_resolve_property', prop, attr.name)
#
#
# @ufunc.resolver(RequestParamsBuilder, Ref, Bind)
# def _resolve_property(
#     env: RequestParamsBuilder,
#     dtype: Ref,
#     attr: Bind
# ):
#     if attr.name in dtype.properties:
#         return dtype.properties[attr.name]
#     else:
#         return dtype.model.properties[attr.name]
#
#
# @ufunc.resolver(RequestParamsBuilder, BackRef, GetAttr)
# def _resolve_property(
#     env: RequestParamsBuilder,
#     dtype: BackRef,
#     attr: GetAttr,
# ):
#     if attr.obj in dtype.properties:
#         prop = dtype.properties[attr.obj]
#     else:
#         prop = dtype.model.properties[attr.obj]
#     return env.call('_resolve_property', prop, attr.name)
#
#
# @ufunc.resolver(RequestParamsBuilder, BackRef, Bind)
# def _resolve_property(env, dtype, attr):
#     if attr.obj in dtype.properties:
#         return dtype.properties[attr.obj]
#     else:
#         return dtype.model.properties[attr.obj]
#
#
# @ufunc.resolver(RequestParamsBuilder, Object, GetAttr)
# def _resolve_property(
#     env: RequestParamsBuilder,
#     dtype: Object,
#     attr: GetAttr,
# ):
#     if attr.obj in dtype.properties:
#         prop = dtype.properties[attr.obj]
#
#         return NestedProperty(
#             left=dtype,
#             right=env.call('_resolve_getattr', prop.dtype, attr.name)
#         )
#     raise FieldNotInResource(dtype, property=attr.obj)
#
#
# @ufunc.resolver(QueryBuilder, Object, Bind)
# def _resolve_property(env, dtype, attr):
#     if attr.name in dtype.properties:
#         return NestedProperty(
#             left=dtype,
#             right=dtype.properties[attr.name].dtype
#         )
#     else:
#         raise FieldNotInResource(dtype, property=attr.name)
#
#
# @ufunc.resolver(QueryBuilder, Array, (Bind, GetAttr))
# def _resolve_property(env, dtype, attr):
#     return NestedProperty(
#         left=dtype,
#         right=env.call('_resolve_getattr', dtype.items.dtype, attr)
#     )
#
#
# @ufunc.resolver(RequestParamsBuilder, Bind)
# def _resolve_property(
#     env: RequestParamsBuilder,
#     bind: Bind
# ):
#     return env.call('_resolve_property', bind.name)
#
#
# @ufunc.resolver(RequestParamsBuilder, str)
# def _resolve_property(
#     env: RequestParamsBuilder,
#     prop: str
# ):
#     if prop in env.params.model.flatprops:
#         return env.params.model.flatprops.get(prop)
#
#     raise PropertyNotFound(env.model, property=prop)
#
#
# @ufunc.resolver(RequestParamsBuilder, DataType)
# def _resolve_property(
#     env: RequestParamsBuilder,
#     dtype: DataType
# ):
#     return dtype.prop
#
#
# @ufunc.resolver(RequestParamsBuilder, Property)
# def _resolve_property(
#     env: RequestParamsBuilder,
#     prop: Property
# ):
#     return prop
#


@ufunc.resolver(RequestParamsBuilder, Expr)
def select(env: RequestParamsBuilder, expr: Expr):
    for arg in expr.args:
        result_name = str(arg)
        result = env.resolve(arg)
        env.call('select', result_name, result, given=arg)


@ufunc.resolver(RequestParamsBuilder, str, FuncProperty)
def select(env: RequestParamsBuilder, select_name: str, func_prop: FuncProperty, given: object = None, **kwargs):
    params = env.params

    if func_prop.func is None:
        func_prop.func = given

    if params.select_funcs is None:
        params.select_funcs = {}
    params.select_funcs[select_name] = func_prop

    if params.select_props is None:
        params.select_props = {}


@ufunc.resolver(RequestParamsBuilder, str, (GetAttr, Bind, Expr))
def select(env: RequestParamsBuilder, select_name: str, other: Union[Bind, Expr], **kwargs):
    params = env.params
    if params.select_props is None:
        params.select_props = {}
    params.select_props[select_name] = other

    if params.select_funcs is None:
        params.select_funcs = {}


@ufunc.resolver(RequestParamsBuilder)
def count(env: RequestParamsBuilder):
    disable_params_pagination(env.params)

    prop = Property()
    prop.name = 'count()'
    prop.place = 'count()'
    prop.title = ''
    prop.description = ''
    prop.model = env.params.model
    prop.dtype = Integer()
    prop.dtype.type = 'integer'
    prop.dtype.type_args = []
    prop.dtype.name = 'integer'
    prop.dtype.prop = prop

    return FuncProperty(func=None, prop=prop)


@ufunc.resolver(RequestParamsBuilder, Expr)
def checksum(env: RequestParamsBuilder, expr: Expr):
    name = str(expr)
    prop = Property()
    prop.name = name
    prop.place = name
    prop.title = ''
    prop.description = ''
    prop.model = env.params.model
    prop.dtype = String()
    prop.dtype.type = 'string'
    prop.dtype.type_args = []
    prop.dtype.name = 'string'
    prop.dtype.prop = prop

    return FuncProperty(func=None, prop=prop)


@ufunc.resolver(RequestParamsBuilder, Expr)
def flip(env: RequestParamsBuilder, expr: Expr):
    name = str(expr)
    prop = Property()
    prop.name = name
    prop.place = name
    prop.title = ''
    prop.description = ''
    prop.model = env.params.model

    dtype = env.call('_resolve_empty_dtype', expr)
    dtype.prop = prop

    prop.dtype = dtype

    return FuncProperty(func=None, prop=prop)

