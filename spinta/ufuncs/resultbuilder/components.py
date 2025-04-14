from typing import Any

from spinta.components import Property
from spinta.core.ufuncs import Env
from spinta.dimensions.enum.components import EnumItem


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

class EnumResultBuilder(Env):
    this: EnumItem
    has_value_changed: bool = False

    def init(self, this: EnumItem):
        return self(
            this=this
        )
