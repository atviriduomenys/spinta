"""
SQLAlchemy Dialect for SAS Databases using JDBC.

This dialect provides connectivity to SAS databases through the SAS IOM JDBC driver,
enabling schema introspection and query execution against SAS libraries and datasets.

Configuration:
    - jdbc_db_name: "sasiom" (required for JDBC URL construction)
    - jdbc_driver_name: "com.sas.rio.MVADriver"

Example connection URL:
    sas+jdbc://user:pass@host:port/?schema=libname
"""

import logging
from pathlib import Path
from typing import Any

from sqlalchemy import pool, types as sqltypes
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.compiler import SQLCompiler

from spinta.datasets.backends.sql.backends.sas.base import BaseDialect
from spinta.datasets.backends.sql.backends.sas.identifier import SASIdentifierPreparer
from spinta.datasets.backends.sql.backends.sas.introspection import SASIntrospectionMixin
from spinta.datasets.backends.sql.backends.sas.types import (
    SASDateType,
    SASDateTimeType,
    SASTimeType,
    SASStringType,
)

logger = logging.getLogger(__name__)


class SASCompiler(SQLCompiler):
    """
    Custom SQL compiler for SAS dialect.

    Ensures that table names are always qualified with the schema (library name)
    to prevent SAS from defaulting to the WORK library.

    Also handles SAS-specific LIMIT syntax by applying (OBS=n) table options
    instead of standard LIMIT clauses.

    Also handles NULL comparisons by converting = NULL to IS NULL and != NULL to IS NOT NULL.

    CRITICAL: SAS SQL passthrough does NOT support parameterized queries with bind
    parameters (? or :name style). All values must be compiled as literal values
    directly in the SQL string.
    """

    def visit_bindparam(self, bindparam, **kw):
        """
        Render bind parameters as literal values for SAS.

        SAS SQL passthrough doesn't support JDBC-style bind parameters (?),
        so we compile all bind parameters as literal values directly in the SQL.

        This overrides SQLAlchemy's default behavior which would generate placeholders.
        """
        # Force literal rendering by delegating to the bound value's literal processor
        return self.render_literal_value(bindparam.value, bindparam.type)

    def visit_binary(self, binary, override_operator=None, **kw):
        """
        Visit a binary expression and handle NULL comparisons for SAS.

        SAS does not support 'column = NULL' syntax, requiring 'column IS NULL' instead.
        This method intercepts binary expressions and converts:
        - column = None → column IS NULL
        - column != None → column IS NOT NULL

        Args:
            binary: The binary expression object
            override_operator: Optional operator override
            **kw: Additional keyword arguments

        Returns:
            Compiled SQL string with proper NULL handling
        """
        # DEBUG: Log entry to verify this method is being called
        logger.debug(f"DEBUG: visit_binary() called with binary={binary}, operator={binary.operator}")

        # Check if either operand is None (SQL NULL)
        from sqlalchemy.sql import operators

        left_is_none = hasattr(binary.left, "value") and binary.left.value is None
        right_is_none = hasattr(binary.right, "value") and binary.right.value is None

        if left_is_none or right_is_none:
            # Determine which operand is not None
            non_none_operand = binary.right if left_is_none else binary.left

            # Convert operator: == to IS NULL, != to IS NOT NULL
            if binary.operator == operators.eq:
                logger.debug("Converting '= NULL' to 'IS NULL' for SAS compatibility")
                return "%s IS NULL" % self.process(non_none_operand, **kw)
            elif binary.operator == operators.ne:
                logger.debug("Converting '!= NULL' to 'IS NOT NULL' for SAS compatibility")
                return "%s IS NOT NULL" % self.process(non_none_operand, **kw)

        # For non-NULL comparisons, use parent's implementation
        return super().visit_binary(binary, override_operator, **kw)

    def visit_table(
        self,
        table: Any,
        asfrom: bool = False,
        iscrud: bool = False,
        ashint: bool = False,
        fromhints: Any | None = None,
        use_schema: bool = True,
        **kwargs,
    ):
        """
        Visit a Table object and compile its name, ensuring schema qualification.

        Args:
            table: The SQLAlchemy Table object
            asfrom: Boolean indicating if the table is in a FROM clause
            **kw: Additional keyword arguments

        Returns:
            The compiled table name with schema and optional OBS clause
        """
        original_schema = table.schema
        schema_modified = False

        try:
            # Ensure SAS tables are qualified with library name when in FROM clause
            if asfrom and not table.schema:
                default_schema = getattr(self.dialect, "default_schema_name", None)
                if default_schema:
                    table.schema = default_schema
                    schema_modified = True

            # Get compiled table name from parent
            result = super().visit_table(table, asfrom, iscrud, ashint, fromhints, use_schema, **kwargs)

            # Append SAS limit clause (OBS=n) when in FROM clause
            if asfrom:
                limit = getattr(self, "_sas_current_limit", None)
                if limit is not None:
                    result += f" (OBS={limit})"
                    logger.debug(f"Applied SAS limit: {result}")

            return result
        finally:
            if schema_modified:
                table.schema = original_schema

    def limit_clause(self, select, **kw):
        """
        Suppress the default LIMIT clause for SAS.

        SAS limits are applied as (OBS=n) table options in the FROM clause
        (handled in visit_table), not as a separate LIMIT clause.

        Returns:
            Empty string (suppresses LIMIT clause)
        """
        return ""


class SASDialect(SASIntrospectionMixin, BaseDialect, DefaultDialect):
    """
    SQLAlchemy dialect for SAS databases using JDBC.

    This dialect provides connectivity to SAS databases through the SAS IOM JDBC driver,
    enabling schema introspection and query execution against SAS libraries and datasets.

    The dialect inherits from:
    - SASIntrospectionMixin: Schema introspection methods (get_table_names, get_columns, etc.)
    - BaseDialect: JDBC-specific functionality (dbapi, is_disconnect, etc.)
    - DefaultDialect: SQLAlchemy's default dialect implementation
    """

    # Dialect identification
    name = "sas"
    driver = "jdbc"  # Required by SQLAlchemy for sas+jdbc:// URLs
    jdbc_db_name = "sasiom"
    jdbc_driver_name = "com.sas.rio.MVADriver"

    # SAS identifier limitation (32 characters max)
    max_identifier_length = 32

    # Custom compiler for SAS - renders bind params as literals
    statement_compiler = SASCompiler

    # IMPORTANT: SAS SQL passthrough doesn't support bind parameters
    # All values are compiled as literals in the SQL string (see SASCompiler.visit_bindparam)
    supports_native_boolean = False

    # Feature support flags
    supports_comments = True

    # Schema and transaction support
    supports_schemas = True
    supports_views = True
    requires_name_normalize = True

    # Transaction handling (SAS operates in auto-commit mode)
    supports_transactions = False
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False

    # SAS doesn't support PK auto-increment or sequences
    supports_pk_autoincrement = False
    supports_sequences = False

    # SAS does not use quoted identifiers - disable quoting
    quote_identifiers = False

    # Enable statement caching for performance
    supports_statement_cache = True

    # Type colspecs for result processing
    #
    # We use custom types because the JDBC driver is configured with
    # applyFormats="false" (see create_connect_args), which causes SAS to return
    # raw internal values (numbers) instead of formatted strings for date/time columns.
    #
    # These custom types handle the conversion from SAS epochs to Python objects:
    # - Dates: Days since 1960-01-01
    # - Datetimes: Seconds since 1960-01-01
    # - Times: Seconds since midnight
    #
    # We prefer this approach over applyFormats="true" because it ensures:
    # 1. Deterministic values (no ambiguity from locale-specific format strings)
    # 2. Type safety (dates are always numbers, not strings that need parsing)
    # 3. Performance (direct numeric conversion is faster than string parsing)
    colspecs = {
        sqltypes.Date: SASDateType,
        sqltypes.DateTime: SASDateTimeType,
        sqltypes.Time: SASTimeType,
        sqltypes.String: SASStringType,
        sqltypes.VARCHAR: SASStringType,
    }

    @classmethod
    def get_dialect_pool_class(cls, url):
        """
        Return the connection pool class to use.

        Uses QueuePool for connection pooling with SAS databases.

        Args:
            url: SQLAlchemy URL object

        Returns:
            QueuePool class
        """
        return pool.QueuePool

    @classmethod
    def get_dialect_cls(cls, url):
        """
        Return the dialect class for SQLAlchemy's dialect loading mechanism.

        This method is required by SQLAlchemy's dialect registry system.

        Args:
            url: SQLAlchemy URL object

        Returns:
            The SASDialect class
        """
        return cls

    def __init__(self, **kwargs):
        """
        Initialize the SAS dialect.

        Sets up the identifier preparer and type mapping cache.
        """
        super().__init__(**kwargs)

        # Override the identifier preparer with our custom SAS version
        self.identifier_preparer = SASIdentifierPreparer(self)

        # Initialize type mapping cache for performance optimization
        self._type_mapping_cache = {}

    def on_connect_url(self, url):
        """
        Store the URL for later use during initialization.

        Args:
            url: SQLAlchemy URL object

        Returns:
            None (no special initialization callback needed)
        """
        self.url = url
        return None

    def initialize(self, connection):
        """
        Initialize dialect with connection-specific settings.

        Extracts the default schema name from the URL query parameters if available.

        Args:
            connection: Database connection object
        """
        try:
            # Call parent initialize if it exists
            if hasattr(super(SASDialect, self), "initialize"):
                super(SASDialect, self).initialize(connection)

            # Extract schema from URL query parameters
            schema = None
            if hasattr(self, "url") and self.url and self.url.query:
                schema = self.url.query.get("schema")

            self.default_schema_name = schema or ""

        except Exception as e:
            logger.warning(f"SAS dialect initialization failed: {e}. Using fallback settings.")
            self.default_schema_name = ""

    def create_connect_args(self, url):
        """
        Parse the SQLAlchemy URL and create JDBC connection arguments.

        The SAS JDBC URL format is:
            jdbc:sasiom://host:port/?schema=libname

        Args:
            url: SQLAlchemy URL object

        Returns:
            Tuple of (args, kwargs) for JDBC connection compatible with jaydebeapi
        """
        logger.debug(f"Creating connection args for URL: {url}")

        try:
            # Build JDBC URL
            jdbc_url = f"jdbc:{self.jdbc_db_name}://{url.host}"
            if url.port:
                jdbc_url += f":{url.port}"

            logger.debug(f"Built JDBC URL: {jdbc_url}")

            # Driver arguments - all values MUST be strings for java.util.Properties
            driver_args = {
                "user": url.username or "",
                "password": url.password or "",
                "applyFormats": "false",
            }

            # Add schema from query parameters
            if url.query and (schema := url.query.get("schema")):
                driver_args["schema"] = schema
                logger.debug(f"Added schema '{schema}' to driver_args")

            # Configure log4j to suppress warnings
            kwargs = {"jclassname": self.jdbc_driver_name, "url": jdbc_url, "driver_args": driver_args}

            log4j_path = Path(__file__).parent / "log4j.properties"
            if log4j_path.exists():
                driver_args["log4j.configuration"] = f"file://{log4j_path.absolute()}"
                kwargs["jars"] = [str(log4j_path.parent)]

            logger.debug("Connection args created successfully")
            return ((), kwargs)

        except Exception as e:
            logger.error(f"Error in create_connect_args: {e}", exc_info=True)
            raise

    def do_rollback(self, dbapi_connection):
        """Handle transaction rollback (no-op for SAS auto-commit mode)."""
        pass

    def do_commit(self, dbapi_connection):
        """Handle transaction commit (no-op for SAS auto-commit mode)."""
        pass

    def normalize_name(self, name):
        """
        Normalize identifier names for SAS (uppercase, no trailing spaces).

        Args:
            name: Identifier name

        Returns:
            Normalized name in uppercase with trailing spaces stripped
        """
        return name.upper().rstrip() if name else name

    def denormalize_name(self, name):
        """
        Denormalize identifier names from SAS (lowercase for display).

        Args:
            name: Normalized name

        Returns:
            Denormalized name in lowercase
        """
        return name.lower() if name else name


def register_sas_dialect():
    """
    Register the SAS dialect with SQLAlchemy.

    This function should be called to make the dialect available
    for use with SQLAlchemy engine creation.

    Example:
        from spinta.datasets.backends.sql.backends.sas.dialect import register_sas_dialect
        register_sas_dialect()

        engine = create_engine('sas+jdbc://host:port', ...)
    """
    from sqlalchemy.dialects import registry

    logger.debug("Registering SAS dialect with SQLAlchemy")
    registry.register("sas.jdbc", "spinta.datasets.backends.sql.backends.sas.dialect", "SASDialect")
