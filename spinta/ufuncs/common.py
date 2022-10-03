from typing import Any
from typing import Tuple
from typing import overload

from spinta.core.ufuncs import Env
from spinta.core.ufuncs import Expr
from spinta.core.ufuncs import ufunc


@overload
@ufunc.resolver(Env, object, object)
def swap(env: Env, old: Any, new: Any) -> Any:
    return env.call('swap', env.this, old, new)


@overload
@ufunc.resolver(Env, object, object, object)
def swap(env: Env, this: Any, old: Any, new: Any) -> Any:
    if this == old:
        return new
    else:
        return this


@overload
@ufunc.resolver(Env, Expr)
def group(env: Env, expr: Expr) -> Tuple[Any]:
    args, kwargs = expr.resolve(env)
    return args[0]


@overload
@ufunc.resolver(Env, object)
def group(env: Env, arg: Any):
    return arg


@overload
@ufunc.resolver(Env, int)
def negative(env: Env, arg: int):
    return -arg


@overload
@ufunc.resolver(Env, float)
def negative(env: Env, arg: int):
    return -arg
