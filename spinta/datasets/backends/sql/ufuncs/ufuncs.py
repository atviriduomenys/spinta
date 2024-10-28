from spinta.core.ufuncs import ufunc, Bind
from spinta.datasets.backends.sql.ufuncs.components import Engine
from spinta.datasets.backends.sql.ufuncs.components import SqlResource
from spinta.exceptions import UnknownBind


@ufunc.resolver(SqlResource)
def connect(env: SqlResource, **kwargs) -> Engine:
    parsed_kwargs = {}
    for key, value in kwargs.items():
        result = env.call(key, value)
        parsed_kwargs[key] = result
    return Engine(env.dsn, **parsed_kwargs)


@ufunc.resolver(SqlResource, Bind)
def schema(env: SqlResource, bind: Bind, **kwargs):
    return bind.name


@ufunc.resolver(SqlResource, Bind)
def encoding(env: SqlResource, bind: Bind, **kwargs):
    return bind.name


@ufunc.resolver(SqlResource, str)
def encoding(env: SqlResource, bind: str, **kwargs):
    return bind


@ufunc.resolver(SqlResource, Bind)
def connect(env: SqlResource, bind: Bind, **kwargs) -> Engine:
    if bind.name == 'self':
        return env.call('connect', env.dsn, **kwargs)
    else:
        raise UnknownBind(name=bind.name)


@ufunc.resolver(SqlResource, str)
def connect(env: SqlResource, dsn: str, **kwargs) -> Engine:
    return Engine(dsn, **kwargs)
