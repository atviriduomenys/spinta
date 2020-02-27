from spinta.core.ufuncs import Env, Bind, Pair, ufunc


@ufunc.resolver(Env, str)
def bind(env, name):
    return Bind(name)


@ufunc.resolver(Env, str, object)
def bind(env, name, value):  # noqa
    return Pair(name, value)
