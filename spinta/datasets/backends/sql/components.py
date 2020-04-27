import contextlib

import sqlalchemy as sa

from sqlalchemy.engine.base import Engine

from spinta.datasets.components import ExternalBackend


class Sql(ExternalBackend):
    engine: Engine = None
    schema: sa.MetaData = None
    dbschema: str = None  # Database schema name

    @contextlib.contextmanager
    def begin(self):
        with self.engine.begin() as conn:
            yield conn

    def get_table(self, name):
        if self.dbschema:
            key = f'{self.dbschema}.{name}'
        else:
            key = name

        if key not in self.schema.tables:
            sa.Table(name, self.schema, autoload=True)

        return self.schema.tables[key]
