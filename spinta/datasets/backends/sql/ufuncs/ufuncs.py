from spinta.core.ufuncs import ufunc, Bind
from spinta.datasets.backends.sql.ufuncs.components import Engine
from spinta.datasets.backends.sql.ufuncs.components import SqlResource
from spinta.exceptions import UnknownBind


@ufunc.resolver(SqlResource)
def connect(env: SqlResource, **kwargs) -> Engine:
    for key in kwargs.keys():
        if key not in ("schema", "encoding"):
            raise UnknownBind(name=key)
    return Engine(env.dsn, **kwargs)


@ufunc.resolver(SqlResource, Bind)
def connect(env: SqlResource, bind: Bind, **kwargs) -> Engine:
    if bind.name == 'self':
        return env.call('connect', env.dsn, **kwargs)
    else:
        raise UnknownBind(name=bind.name)


@ufunc.resolver(SqlResource, str)
def connect(env: SqlResource, dsn: str, **kwargs) -> Engine:
    return Engine(dsn, **kwargs)
