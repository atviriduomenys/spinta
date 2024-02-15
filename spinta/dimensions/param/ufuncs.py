from collections.abc import Generator
from typing import Any, Iterator

from spinta import commands
from spinta.components import Model
from spinta.core.ufuncs import Bind, Expr
from spinta.core.ufuncs import Env
from spinta.core.ufuncs import ufunc
from spinta.datasets.components import Param
from spinta.dimensions.param.components import ParamBuilder, ParamLoader
from spinta.utils.schema import NotAvailable


class _Loop:
    expr: Expr
    arg: Any

    def __init__(self, expr, arg):
        self.arg = arg
        self.expr = expr


@ufunc.resolver(Env, Bind)
def param(env: Env, bind: Bind) -> Any:
    return env.params[bind.name]


@ufunc.resolver(ParamBuilder, Expr)
def select(env: ParamBuilder, expr: Expr) -> Any:
    if isinstance(env.this, Model):
        data = commands.getall(env.context, env.this, env.this.backend, query=expr, resolved_params=env.params)
        return data


@ufunc.resolver(ParamBuilder, Expr, name="getattr")
def getattr_(env: ParamBuilder, expr: Expr):
    args, kwargs = expr.resolve(env)
    resolved = env.call("getattr", *args)
    yield from resolved


@ufunc.resolver(ParamBuilder, Generator, object, name="getattr")
def getattr_(env: ParamBuilder, generator: Generator, obj: object):
    for item in generator:
        yield from env.call("getattr", item, obj)


@ufunc.resolver(ParamBuilder, _Loop, Bind, name="getattr")
def getattr_(env: ParamBuilder, _loop: _Loop, bind: Bind):
    data = env.call('getattr', _loop.arg, bind)
    if isinstance(data, Iterator):
        for item in data:
            if item:
                yield item

                if item != env.params[env.target_param]:
                    env.params[env.target_param] = item
                    args, kwargs = _loop.expr.resolve(env)
                    for arg in args:
                        yield from env.call('getattr', _Loop(_loop.expr, arg), bind)


@ufunc.resolver(ParamBuilder, dict, Bind, name="getattr")
def getattr_(env: ParamBuilder, data: dict, bind: Bind):
    if bind.name in data:
        yield data[bind.name]


@ufunc.resolver(ParamBuilder, Expr)
def loop(env: ParamBuilder, expr: Expr):
    args, kwargs = expr.resolve(env)
    for arg in args:
        yield _Loop(expr, arg)


@ufunc.resolver(ParamBuilder, Generator, Bind)
def loop(env: ParamBuilder, generator: Generator, bind: Bind):
    for item in generator:
        data = env.call("getattr", item, bind)
        yield data


@ufunc.executor(ParamBuilder, NotAvailable)
def select(env: ParamBuilder, na: NotAvailable) -> Any:
    return env.this


@ufunc.resolver(ParamLoader, list)
def resolve_param(env: ParamLoader, params: list):
    for parameter in params:
        env.call("resolve_param", parameter)


@ufunc.resolver(ParamLoader, Param)
def resolve_param(env: ParamLoader, parameter: Param):
    for i, (source, formula) in enumerate(zip(parameter.sources.copy(), parameter.formulas)):
        if isinstance(source, str) and formula:
            requires_model = env.call("contains_select", formula)
            if requires_model:
                new_name = source
                if env.dataset and '/' not in source:
                    new_name = '/'.join([
                        env.dataset.name,
                        source,
                    ])
                if commands.has_model(env.context, env.manifest, new_name):
                    model = commands.get_model(env.context, env.manifest, new_name)
                    parameter.sources[i] = model
                elif source != new_name and commands.has_model(env.context, env.manifest, source):
                    model = commands.get_model(env.context, env.manifest, source)
                    parameter.sources[i] = model


@ufunc.resolver(ParamLoader, object)
def contains_select(env: ParamLoader, obj: object):
    return False


@ufunc.resolver(ParamLoader, Expr)
def contains_select(env: ParamLoader, expr: Expr):
    for arg in expr.args:
        result = env.resolve(arg)
        if isinstance(result, bool) and result is True:
            return True
    return False


@ufunc.resolver(ParamLoader, object)
def select(env: ParamLoader, any_: object):
    return True


@ufunc.resolver(ParamLoader, Expr)
def loop(env: ParamLoader, expr: Expr):
    for arg in expr.args:
        result = env.resolve(arg)
        if result:
            return result
    return False

