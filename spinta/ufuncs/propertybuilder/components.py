from spinta.components import Model
from spinta.core.ufuncs import Env


class PropertyBuilder(Env):
    model: Model

    def init(self, model: Model):
        return self(
            model=model,
        )
