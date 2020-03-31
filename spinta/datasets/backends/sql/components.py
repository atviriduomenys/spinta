import contextlib

import sqlalchemy as sa

from sqlalchemy.engine.base import Engine

from spinta.datasets.components import ExternalBackend


class Sql(ExternalBackend):
    engine: Engine = None
    schema: sa.MetaData = None

    @contextlib.contextmanager
    def begin(self):
        with self.engine.begin() as conn:
            yield conn
