from spinta.core.ufuncs import Bind, Env, Negative, Pair, Positive, ufunc


@ufunc.resolver(Env, str)
def bind(env: Env, name: str) -> Bind:
    return Bind(name)


@ufunc.resolver(Env, str, object)
def bind(env: Env, name: str, value: object) -> Pair:
    return Pair(name, value)


@ufunc.resolver(Env, str)
def negative(env: Env, name: str) -> Negative:
    return Negative(name)


@ufunc.resolver(Env, str)
def positive(env: Env, name: str) -> Positive:
    return Positive(name)


@ufunc.resolver(Env, Bind)
def negative(env: Env, bind_: Bind) -> Negative:
    return Negative(bind_.name)


@ufunc.resolver(Env, Bind)
def positive(env: Env, bind_: Bind) -> Positive:
    return Positive(bind_.name)
