from spinta.core.ufuncs import Env, ufunc
from spinta.core.ufuncs import Bind, Pair
from spinta.core.ufuncs import Negative, Positive


@ufunc.resolver(Env, str)
def bind(env, name):
    return Bind(name)


@ufunc.resolver(Env, str, object)
def bind(env, name, value):
    return Pair(name, value)


@ufunc.resolver(Env, str)
def negative(env, name) -> Negative:
    return Negative(name)


@ufunc.resolver(Env, str)
def positive(env, name) -> Positive:
    return Positive(name)


@ufunc.resolver(Env, Bind)
def negative(env, bind) -> Negative:
    return Negative(bind.name)


@ufunc.resolver(Env, Bind)
def positive(env, bind) -> Positive:
    return Positive(bind.name)
