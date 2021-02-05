from spinta.core.ufuncs import Bind
from spinta.core.ufuncs import ufunc
from spinta.datasets.backends.sql.ufuncs.components import Engine
from spinta.datasets.backends.sql.ufuncs.components import SqlResource
from spinta.exceptions import UnknownBind


@ufunc.resolver(SqlResource, Bind)
def connect(env: SqlResource, bind: Bind, **kwargs) -> Engine:
    if bind.name == 'self':
        return env.call('connect', env.dsn, **kwargs)
    else:
        raise UnknownBind(name=bind.name)


@ufunc.resolver(SqlResource, str)
def connect(env: SqlResource, dsn: str, **kwargs) -> Engine:
    return Engine(dsn, **kwargs)
