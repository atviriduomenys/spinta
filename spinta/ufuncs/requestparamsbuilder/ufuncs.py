from spinta.components import ParamsPage, Property, FuncProperty
from spinta.core.ufuncs import ufunc, Expr
from spinta.types.datatype import Integer
from spinta.ufuncs.requestparamsbuilder.components import RequestParamsBuilder


@ufunc.resolver(RequestParamsBuilder, Expr)
def select(env: RequestParamsBuilder, expr: Expr):
    params = env.params
    for arg in expr.args:
        result_name = str(arg)
        result = env.resolve(arg)

        # If result != arg, means that new data was given
        # meaning it is a function
        # all binds and getattr are skipped and should return themselves
        if isinstance(result, FuncProperty):
            if result.func is None:
                result.func = arg

            if params.select_funcs is None:
                params.select_funcs = {}
            params.select_funcs[result_name] = result

            if params.select_props is None:
                params.select_props = {}

        else:
            if params.select_props is None:
                params.select_props = {}
            params.select_props[result_name] = result

            if params.select_funcs is None:
                params.select_funcs = {}


@ufunc.resolver(RequestParamsBuilder)
def count(env: RequestParamsBuilder):
    if env.params.page is not None:
        env.params.page.is_enabled = False
    else:
        env.params.page = ParamsPage(is_enabled=False)

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

