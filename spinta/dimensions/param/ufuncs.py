from collections.abc import Iterator
from typing import Any

from spinta import commands
from spinta.components import Model
from spinta.core.ufuncs import Bind, Expr
from spinta.core.ufuncs import Env
from spinta.core.ufuncs import ufunc
from spinta.datasets.components import Param
from spinta.dimensions.param.components import ParamBuilder, ParamLoader
from spinta.exceptions import PropertyNotFound, KeyNotFound, ModelNotFound, InvalidParamSource
from spinta.utils.schema import NotAvailable


@ufunc.resolver(Env, Bind)
def param(env: Env, bind: Bind) -> Any:
    return env.params[bind.name]


@ufunc.resolver(ParamBuilder, Model)
def read(env: ParamBuilder, model: Model) -> Any:
    return commands.getall(env.context, model, model.backend, resolved_params=env.params)


@ufunc.resolver(ParamBuilder, str)
def read(env: ParamBuilder, obj: str):
    new_name = obj
    if '/' not in obj:
        model_ = env.this
        if isinstance(model_, Model) and model_.external and model_.external.dataset:
            new_name = '/'.join([
                model_.external.dataset.name,
                obj,
            ])
    model = None
    if commands.has_model(env.context, env.manifest, new_name):
        model = commands.get_model(env.context, env.manifest, new_name)
    elif obj != new_name and commands.has_model(env.context, env.manifest, obj):
        model = commands.get_model(env.context, env.manifest, obj)
    if not model:
        raise ModelNotFound(model=obj)

    env.this = model
    return env.call("read", model)


@ufunc.resolver(ParamBuilder)
def read(env: ParamBuilder) -> Any:
    if isinstance(env.this, Model):
        return env.call("read", env.this)
    raise InvalidParamSource(param=env.target_param, source=env.this, given_type=type(env.this), expected_types=[type(Model)])


@ufunc.resolver(ParamBuilder, Iterator, Bind, name="getattr")
def getattr_(env: ParamBuilder, iterator: Iterator, bind: Bind):
    for item in iterator:
        yield from env.call("getattr", item, bind)


@ufunc.resolver(ParamBuilder, dict, Bind, name="getattr")
def getattr_(env: ParamBuilder, data: dict, bind: Bind):
    if bind.name in data:
        yield data[bind.name]
    else:
        raise KeyNotFound(env.this, key=bind.name, dict_keys=list(data.keys()))


@ufunc.resolver(ParamBuilder, NotAvailable, name="getattr")
def getattr_(env: ParamBuilder, _: NotAvailable):
    return env.this


# {'name': 'getattr', 'args': [{'name': 'loop', 'args': [{'name': 'read', 'args': []}], 'type': 'method'}, {'name': 'bind', 'args': ['more']}]}
# getattr [ loop(read()), bind(more) ]
#  1 -> stack = [1]
#  loop stack [1]
#  read(1) -> 2
#  pop(1) from stack
#  2 -> stack
#  loop until stack is empty


@ufunc.executor(ParamBuilder, NotAvailable)
def read(env: ParamBuilder, na: NotAvailable) -> Any:
    return env.this


@ufunc.resolver(ParamLoader, list)
def resolve_param(env: ParamLoader, params: list):
    for parameter in params:
        env.call("resolve_param", parameter)


@ufunc.resolver(ParamLoader, Param)
def resolve_param(env: ParamLoader, parameter: Param):
    for i, (source, prepare) in enumerate(zip(parameter.sources.copy(), parameter.formulas)):
        formula = None
        if isinstance(prepare, Expr):
            formula = Expr(prepare.name, *prepare.args, **prepare.kwargs)
        requires_model = env.call("contains_read", formula)
        if isinstance(source, str) and requires_model:
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
            else:
                raise ModelNotFound(model=source)

            env.call("validate_prepare", model, formula)


@ufunc.resolver(ParamLoader, Expr)
def contains_read(env: ParamLoader, expr: Expr):
    resolved, _ = expr.resolve(env)
    for arg in resolved:
        if isinstance(arg, bool) and arg is True:
            return True
    return False


@ufunc.resolver(ParamLoader, object)
def contains_read(env: ParamLoader, obj: object):
    return False


@ufunc.resolver(ParamLoader, Model, Expr)
def validate_prepare(env: ParamLoader, model: Model, expr: Expr):
    resolved, _ = expr.resolve(env)
    for arg in resolved:
        if isinstance(arg, str):
            if arg not in model.properties:
                raise PropertyNotFound(model, property=arg)
        elif isinstance(arg, Bind):
            if arg.name not in model.properties:
                raise PropertyNotFound(model, property=arg.name)


@ufunc.resolver(ParamLoader, object)
def read(env: ParamLoader, any_: object):
    return True


@ufunc.resolver(ParamLoader)
def read(env: ParamLoader):
    return True

