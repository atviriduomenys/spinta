from typing import Any

from spinta.components import Property
from spinta.core.ufuncs import Env
from spinta.dimensions.enum.components import EnumItem


class ResultBuilder(Env):
    this: Any  # A selected value.
    prop: Property  # Model property.
    data: dict  # Whole row from database.
    params: dict  # Needed for prepare=param(..)

    def init(self, this: Any, prop: Property, data: dict, params: dict) -> "ResultBuilder":
        return self(
            this=this,
            prop=prop,
            data=data,
            params=params,
        )


class EnumResultBuilder(Env):
    this: EnumItem
    has_value_changed: bool = False

    def init(self, this: EnumItem):
        return self(this=this)
