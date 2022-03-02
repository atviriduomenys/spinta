from typing import Any
from typing import Tuple

from spinta.core.ufuncs import Env
from spinta.core.ufuncs import Expr
from spinta.core.ufuncs import ufunc


@ufunc.resolver(Env, object, object)
def swap(env: Env, old: Any, new: Any) -> Any:
    return env.call('swap', env.this, old, new)


@ufunc.resolver(Env, object, object, object)
def swap(env: Env, this: Any, old: Any, new: Any) -> Any:
    if this == old:
        return new
    else:
        return this


@ufunc.resolver(Env, Expr)
def group(env: Env, expr: Expr) -> Tuple[Any]:
    args, kwargs = expr.resolve(env)
    return args[0]
