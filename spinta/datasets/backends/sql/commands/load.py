from typing import Dict, Any

import sqlalchemy as sa

from spinta import commands
from spinta.components import Context
from spinta.datasets.backends.sql.components import Sql


@commands.load.register(Context, Sql, dict)
def load(context: Context, backend: Sql, config: Dict[str, Any]):
    dsn = config['dsn']
    schema = config.get('schema')
    backend.engine = sa.create_engine(dsn, echo=False)
    backend.schema = sa.MetaData(backend.engine, schema=schema)
    backend.dbschema = schema
