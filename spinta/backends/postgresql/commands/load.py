from typing import Dict, Any

import sqlalchemy as sa

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.name import PG_NAMING_CONVENTION
from spinta.components import Context
from spinta.utils.sqlalchemy import get_metadata_naming_convention


@commands.load.register(Context, PostgreSQL, dict)
def load(context: Context, backend: PostgreSQL, config: Dict[str, Any]):
    backend.dsn = config['dsn']
    backend.engine = sa.create_engine(backend.dsn, echo=False)
    backend.schema = sa.MetaData(backend.engine, naming_convention=get_metadata_naming_convention(
        PG_NAMING_CONVENTION
    ))
    backend.tables = {}


@commands.unload_backend.register()
def unload_backend(context: Context, backend: PostgreSQL):
    # Make sure all connections are released, since next test will create
    # another connection pool and connection pool is not reused between
    # tests. Maybe it would be a good idea to reuse same connection between
    # all tests?
    backend.engine.dispose()
