from spinta.components import Model
from spinta.core.ufuncs import Env
from spinta.exceptions import UnknownRequestQuery


class BBox:
    x_min: float
    y_min: float
    x_max: float
    y_max: float

    def __init__(self, x_min: float, y_min: float, x_max: float, y_max: float,):
        self.x_min = x_min
        self.y_min = y_min
        self.x_max = x_max
        self.y_max = y_max


class SummaryEnv(Env):

    def init(self, model: Model):
        return self(
            model=model,
            prop=None,
            bbox=None
        )

    def default_resolver(self, expr, *args, **kwargs):
        raise UnknownRequestQuery(request="summary", query=expr.name)
