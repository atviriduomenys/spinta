from dataclasses import dataclass
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine.base import Engine as SaEngine
from sqlalchemy.engine.row import RowProxy

from spinta.components import Property
from spinta.core.ufuncs import Env
from spinta.utils.data import take
from spinta.utils.schema import NA


@dataclass
class Engine:
    dsn: str  # sqlalchemy engine dsn
    schema: str = None
    encoding: str = NA

    def create(self) -> SaEngine:
        return sa.create_engine(self.dsn, **take({
            'encoding': self.encoding,
        }))


class SqlResource(Env):
    dsn: str

    def init(self, dsn: str):
        return self(dsn=dsn)


class SqlResultBuilder(Env):
    this: Any           # A selected value.
    prop: Property      # Model property.
    data: RowProxy      # Whole row from database.

    def init(self, this: Any, prop: Property, data: RowProxy):
        return self(
            this=this,
            prop=prop,
            data=data,
        )
