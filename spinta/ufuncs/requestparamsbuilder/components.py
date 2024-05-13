from spinta.components import UrlParams
from spinta.core.ufuncs import Env


class RequestParamsBuilder(Env):
    params: UrlParams

    def init(self, params: UrlParams):
        return self(
            params=params,
        )



