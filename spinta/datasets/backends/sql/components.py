import contextlib

import sqlalchemy as sa
from sqlalchemy.engine.base import Engine

from spinta import commands
from spinta.components import Model
from spinta.components import Property
from spinta.datasets.components import ExternalBackend
from spinta.exceptions import NoExternalName
from spinta.exceptions import PropertyNotFound


class Sql(ExternalBackend):
    type: str = 'sql'
    engine: Engine = None
    schema: sa.MetaData = None
    dbschema: str = None  # Database schema name
    paginated: bool = True

    @contextlib.contextmanager
    def transaction(self, write=False):
        raise NotImplementedError

    @contextlib.contextmanager
    def begin(self):
        with self.engine.begin() as conn:
            yield conn

    def get_table(self, model: Model, name: str = None) -> sa.Table:
        name = name or model.external.name

        if self.dbschema:
            key = f'{self.dbschema}.{name}'
        else:
            key = name

        if key not in self.schema.tables:
            sa.Table(name, self.schema, autoload_with=self.engine)

        return self.schema.tables[key]

    def get_column(
        self,
        table: sa.Table,
        prop: Property,
        *,
        select=False,
        **kwargs
    ) -> sa.Column:
        column = commands.get_column(self, prop, table=table, **kwargs)
        return column




