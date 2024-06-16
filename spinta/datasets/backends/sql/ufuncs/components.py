from dataclasses import dataclass
from typing import TypedDict

import sqlalchemy as sa
from sqlalchemy.engine.base import Engine as SaEngine

from spinta.core.ufuncs import Env
from spinta.ufuncs.basequerybuilder.components import Selected
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


class FileSelected(TypedDict):
    name: Selected      # File name
    content: Selected   # File content
