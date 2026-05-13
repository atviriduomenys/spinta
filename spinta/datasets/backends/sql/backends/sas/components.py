"""
SAS Backend Component

This module provides the main backend component class for SAS database integration
within the Spinta framework.

The SAS backend extends the generic SQL backend to provide SAS-specific functionality,
leveraging the custom SAS dialect implemented in dialect.py for SQLAlchemy integration.
"""

import logging

import sqlalchemy as sa

from spinta.components import Model
from spinta.datasets.backends.sql.components import Sql

logger = logging.getLogger(__name__)


class SAS(Sql):
    """
    SAS Backend Component for Spinta framework.

    This backend provides connectivity to SAS databases through JDBC,
    enabling data access from SAS libraries and datasets.

    Attributes:
        type: Backend type identifier ("sql/sas")
        query_builder_type: Query builder type identifier ("sql/sas")
    """

    type = "sql/sas"
    query_builder_type = "sql/sas"

    def __init__(self, **kwargs):
        """Initialize the SAS backend and extract schema from DSN URL if present."""
        super().__init__(**kwargs)

        # Extract schema from DSN URL if not already set
        if hasattr(self, "dsn") and self.dsn and not self.dbschema:
            from sqlalchemy.engine.url import make_url

            url = make_url(self.dsn)
            if schema := url.query.get("schema"):
                self.dbschema = schema

    def get_table(self, model: Model, name: str | None = None) -> sa.Table:
        """
        Get or create a SQLAlchemy Table object for a model.

        Handles SAS-specific schema resolution from the dialect's default_schema_name.

        Args:
            model: The model to get the table for
            name: Optional table name override

        Returns:
            SQLAlchemy Table object

        Raises:
            KeyError: If table not found in schema
        """
        name = name or model.external.name

        # Determine effective schema (backend's dbschema or dialect's default)
        effective_schema = self.dbschema or getattr(self.engine.dialect, "default_schema_name", None)
        key = f"{effective_schema}.{name}" if effective_schema else name

        # Create table if not in cache
        if key not in self.schema.tables:
            if hasattr(self.engine.dialect, "default_schema_name"):
                self.engine.dialect.default_schema_name = effective_schema
            sa.Table(name, self.schema, autoload_with=self.engine, schema=effective_schema)

        # Retrieve table with multiple fallback strategies
        if key in self.schema.tables:
            return self.schema.tables[key]

        if name in self.schema.tables:
            return self.schema.tables[name]

        # Last resort: search by name
        for table_obj in self.schema.tables.values():
            if table_obj.name == name:
                return table_obj

        raise KeyError(f"Table '{name}' not found in schema")
