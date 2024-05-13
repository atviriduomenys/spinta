from typing import Any

from spinta.components import Property
from spinta.core.ufuncs import Env


class ResultBuilder(Env):
    this: Any  # A selected value.
    prop: Property  # Model property.
    data: dict  # Whole row from database.

    def init(self, this: Any, prop: Property, data: dict):
        return self(
            this=this,
            prop=prop,
            data=data,
        )
