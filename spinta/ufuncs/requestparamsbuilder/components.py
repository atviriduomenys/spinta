from spinta.components import UrlParams, Property
from spinta.core.ufuncs import Env
from spinta.ufuncs.propertyresolver.components import PropertyResolver


class RequestParamsBuilder(Env):
    params: UrlParams

    def init(self, params: UrlParams):
        return self(params=params, property_resolver=PropertyResolver(context=self.context).init(model=params.model))

    def resolve_property(self, *args, **kwargs) -> Property:
        return self.property_resolver.resolve_property(*args, **kwargs)
