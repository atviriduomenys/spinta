from typing import Any

from spinta.core.ufuncs import Bind
from spinta.core.ufuncs import Env
from spinta.core.ufuncs import ufunc


@ufunc.resolver(Env, Bind)
def param(env: Env, bind: Bind) -> Any:
    return env.params[bind.name]
