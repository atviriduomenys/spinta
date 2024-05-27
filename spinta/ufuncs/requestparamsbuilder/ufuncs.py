from typing import Union

from spinta.components import Property, FuncProperty
from spinta.core.ufuncs import ufunc, Expr, Bind
from spinta.types.datatype import Integer, String
from spinta.ufuncs.requestparamsbuilder.components import RequestParamsBuilder
from spinta.ufuncs.requestparamsbuilder.helpers import disable_params_pagination


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


@ufunc.resolver(RequestParamsBuilder, str, (Bind, Expr))
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
    prop = Property()
    prop.name = 'checksum()'
    prop.place = 'checksum()'
    prop.title = ''
    prop.description = ''
    prop.model = env.params.model
    prop.dtype = String()
    prop.dtype.type = 'string'
    prop.dtype.type_args = []
    prop.dtype.name = 'string'
    prop.dtype.prop = prop

    return FuncProperty(func=None, prop=prop)
