from collections.abc import Iterator
from typing import Any

from spinta import commands
from spinta.components import Model
from spinta.core.ufuncs import Bind, Expr
from spinta.core.ufuncs import Env
from spinta.core.ufuncs import ufunc
from spinta.datasets.components import Param
from spinta.dimensions.param.components import ParamBuilder, ParamLoader
from spinta.utils.schema import NotAvailable


class LoopExpr:
    expr: Expr
    iterator: Iterator

    def __init__(self, expr, iterator):
        self.iterator = iterator
        self.expr = expr


@ufunc.resolver(Env, Bind)
def param(env: Env, bind: Bind) -> Any:
    return env.params[bind.name]


@ufunc.resolver(ParamBuilder, Expr)
def read(env: ParamBuilder, expr: Expr) -> Any:
    args, kwargs = expr.resolve(env)
    if not args:
        if isinstance(env.this, Model):
            return commands.getall(env.context, env.this, env.this.backend, resolved_params=env.params)


@ufunc.resolver(ParamBuilder, Iterator, Bind, name="getattr")
def getattr_(env: ParamBuilder, iterator: Iterator, bind: Bind):
    for item in iterator:
        yield from env.call("getattr", item, bind)


@ufunc.resolver(ParamBuilder, LoopExpr, Bind, name="getattr")
def getattr_(env: ParamBuilder, loop_: LoopExpr, bind: Bind):
    data = env.call('getattr', loop_.iterator, bind)
    if isinstance(data, Iterator):
        for item in data:
            if item is not None:
                yield item

                if item != env.params[env.target_param]:
                    env.params[env.target_param] = item
                    new_loops = env.call('loop', loop_.expr)
                    for new_loop in new_loops:
                        if isinstance(new_loop, LoopExpr):
                            yield from env.call('getattr', new_loop, bind)


@ufunc.resolver(ParamBuilder, dict, Bind, name="getattr")
def getattr_(env: ParamBuilder, data: dict, bind: Bind):
    if bind.name in data:
        yield data[bind.name]


@ufunc.resolver(ParamBuilder, Expr)
def loop(env: ParamBuilder, expr: Expr):
    args, kwargs = expr.resolve(env)
    for arg in args:
        if isinstance(arg, Iterator):
            yield LoopExpr(expr, arg)


@ufunc.executor(ParamBuilder, NotAvailable)
def read(env: ParamBuilder, na: NotAvailable) -> Any:
    return env.this


@ufunc.resolver(ParamLoader, list)
def resolve_param(env: ParamLoader, params: list):
    for parameter in params:
        env.call("resolve_param", parameter)


@ufunc.resolver(ParamLoader, Param)
def resolve_param(env: ParamLoader, parameter: Param):
    for i, (source, formula) in enumerate(zip(parameter.sources.copy(), parameter.formulas)):
        if isinstance(source, str) and formula:
            requires_model = env.call("contains_read", formula)
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
def contains_read(env: ParamLoader, obj: object):
    return False


@ufunc.resolver(ParamLoader, Expr)
def contains_read(env: ParamLoader, expr: Expr):
    for arg in expr.args:
        result = env.resolve(arg)
        if isinstance(result, bool) and result is True:
            return True
    return False


@ufunc.resolver(ParamLoader, object)
def read(env: ParamLoader, any_: object):
    return True


@ufunc.resolver(ParamLoader, Expr)
def loop(env: ParamLoader, expr: Expr):
    for arg in expr.args:
        result = env.resolve(arg)
        if result:
            return result
    return False

