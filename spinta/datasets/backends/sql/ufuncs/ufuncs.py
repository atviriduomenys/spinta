from spinta.core.ufuncs import ufunc
from spinta.datasets.backends.sql.ufuncs.components import Engine
from spinta.datasets.backends.sql.ufuncs.components import SqlResource
from spinta.exceptions import UnknownBind


@ufunc.resolver(SqlResource)
def connect(env: SqlResource, **kwargs) -> Engine:
    for key in kwargs.keys():
        if key not in ("schema", "encoding"):
            raise UnknownBind(name=key)
    return Engine(env.dsn, **kwargs)
