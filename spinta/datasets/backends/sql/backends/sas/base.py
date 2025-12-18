"""
Base JDBC Dialect for SAS.

This module provides the base dialect class for JDBC-based database connectivity,
specifically designed for use with jaydebeapi.

The BaseDialect class provides common functionality that is shared across
JDBC-based dialects, including connection handling and basic feature flags.
"""


class BaseDialect:
    """
    Base class for JDBC dialects using jaydebeapi.

    This class provides common functionality for dialects that connect
    to databases via JDBC using the jaydebeapi library. It defines
    basic connection handling and feature flags.

    Attributes:
        jdbc_db_name: The JDBC database name used in connection URLs
        jdbc_driver_name: The fully qualified name of the JDBC driver class
        supports_native_decimal: Whether the database supports native decimal types
        supports_sane_rowcount: Whether accurate row counts are available
        supports_sane_multi_rowcount: Whether accurate multi-row counts are available
        supports_unicode_binds: Whether Unicode bind parameters are supported
        description_encoding: Encoding for result descriptions (None = default)
    """

    jdbc_db_name = ""
    jdbc_driver_name = ""
    supports_native_decimal = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_unicode_binds = True
    description_encoding = None

    @classmethod
    def dbapi(cls):
        """
        Return the DBAPI module (jaydebeapi) for JDBC connectivity.

        This method is called by SQLAlchemy to get the database API module.

        Returns:
            The jaydebeapi module
        """
        import jaydebeapi

        return jaydebeapi

    def is_disconnect(self, e, connection, cursor):
        """
        Determine if an exception indicates a disconnected connection.

        Args:
            e: The exception that was raised
            connection: The database connection
            cursor: The database cursor

        Returns:
            True if the exception indicates a disconnection, False otherwise
        """
        try:
            import jaydebeapi

            # Check if it's a jaydebeapi DatabaseError
            if isinstance(e, jaydebeapi.DatabaseError):
                e_str = str(e)
                return "connection is closed" in e_str or "cursor is closed" in e_str
        except (ImportError, AttributeError):
            pass

        return False

    def do_rollback(self, dbapi_connection):
        """
        Handle transaction rollback.

        This is a no-op for JDBC connections that don't support transactions.

        Args:
            dbapi_connection: The DBAPI connection object
        """
        pass
