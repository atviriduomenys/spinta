from spinta.components import Model
from spinta.core.ufuncs import Env


class PropertyResolver(Env):
    model: Model

    # Return ufunc types instead of Property
    # ForeignProperty, ReservedProperty, etc.
    ufunc_types: bool

    def init(self, model: Model, ufunc_types: bool = False):
        return self(model=model, ufunc_types=ufunc_types)

    def resolve_property(self, *args, **kwargs):
        return self.call("_resolve_property", *args, **kwargs)
