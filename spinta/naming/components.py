from __future__ import annotations

from spinta.components import Model
from spinta.components import Property
from spinta.core.ufuncs import Env


class NameFormatter(Env):
    model: Model
    prop: Property

