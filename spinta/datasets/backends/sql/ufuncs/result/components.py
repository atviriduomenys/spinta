from typing import Any

from spinta.components import Property
from spinta.ufuncs.resultbuilder.components import ResultBuilder
from sqlalchemy.engine.row import RowProxy


class SqlResultBuilder(ResultBuilder):
    this: Any           # A selected value.
    prop: Property      # Model property.
    data: RowProxy      # Whole row from database.

    def init(self, this: Any, prop: Property, data: RowProxy):
        return self(
            this=this,
            prop=prop,
            data=data,
        )
