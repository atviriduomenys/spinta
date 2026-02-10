import os
import logging
from typing import Dict, Any

import sqlalchemy as sa

from spinta import commands
from spinta.components import Context
from spinta.datasets.backends.sql.backends.sas.components import SAS

logger = logging.getLogger(__name__)


@commands.load.register(Context, SAS, dict)
def load(context: Context, backend: SAS, config: Dict[str, Any]):
    """
    Load and initialize the SAS backend.

    Handles SAS-specific requirements:
    1. Expands environment variables in DSN
    2. Injects schema into DSN query parameters for JDBC driver
    3. Sets dialect's default_schema_name for proper table qualification
    """
    dsn = config.get("dsn")
    schema = config.get("schema")

    # Expand environment variables in DSN
    if dsn:
        dsn = os.path.expandvars(dsn)

    if dsn:
        # Inject schema into DSN query parameters for SAS JDBC driver
        # This ensures the dialect's default_schema_name is properly set
        if schema:
            if "?" in dsn:
                dsn = dsn + f"&schema={schema}"
            else:
                dsn = dsn + f"?schema={schema}"

        # Create engine
        backend.engine = sa.create_engine(dsn, echo=False)
        backend.schema = sa.MetaData(backend.engine, schema=schema)
        backend.dbschema = schema

        # Ensure dialect's default_schema_name is set for proper table qualification
        if schema and hasattr(backend.engine.dialect, "default_schema_name"):
            backend.engine.dialect.default_schema_name = schema
            logger.debug(f"SAS backend: Set default_schema_name to '{schema}'")
