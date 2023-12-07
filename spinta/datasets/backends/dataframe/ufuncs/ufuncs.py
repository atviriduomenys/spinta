from spinta.core.ufuncs import ufunc
from spinta.datasets.backends.dataframe.ufuncs.components import TabularResource
from spinta.exceptions import UnknownBind


@ufunc.resolver(TabularResource)
def tabular(env: TabularResource, **kwargs):
    for key, value in kwargs.items():
        if key == "sep":
            env.seperator = value
        else:
            raise UnknownBind(name=key)
