from typing import Dict, Any
import os

import sqlalchemy as sa

from spinta import commands
from spinta.components import Context
from spinta.datasets.backends.sql.components import Sql


@commands.load.register(Context, Sql, dict)
def load(context: Context, backend: Sql, config: Dict[str, Any]):
    dsn = config["dsn"]
    if dsn:
        dsn = os.path.expandvars(dsn)
    schema = config.get("schema")
    if dsn:
        # There can be a situation, when backend type was given, but dsn was
        # not. In this case we still create backend instance, but leave it
        # uninitialized.
        # For SAS dialect, include schema in DSN query parameters to ensure
        # the dialect's default_schema_name is properly set
        if schema and "sas+jdbc" in dsn:
            if "?" in dsn:
                dsn = dsn + f"&schema={schema}"
            else:
                dsn = dsn + f"?schema={schema}"

        backend.engine = sa.create_engine(dsn, echo=False)

        backend.schema = sa.MetaData(backend.engine, schema=schema)
        backend.dbschema = schema

        # For SAS dialect, ensure the engine's dialect has the correct default schema
        if schema and "sas+jdbc" in dsn:
            if hasattr(backend.engine.dialect, "default_schema_name"):
                backend.engine.dialect.default_schema_name = schema
                import logging

                logger = logging.getLogger(__name__)
                logger.debug(f"SAS backend load: Set dialect default_schema_name to: {schema}")
