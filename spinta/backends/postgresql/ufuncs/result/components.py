from typing import Any

from sqlalchemy.engine.row import RowProxy

from spinta.components import Property
from spinta.ufuncs.resultbuilder.components import ResultBuilder


class PgResultBuilder(ResultBuilder):
    this: Any  # A selected value.
    prop: Property  # Model property.
    data: RowProxy  # Whole row from database.

    def init(self, this: Any, prop: Property, data: RowProxy):
        return self(
            this=this,
            prop=prop,
            data=data,
        )
