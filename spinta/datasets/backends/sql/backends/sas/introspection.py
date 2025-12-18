"""
SAS Schema Introspection for SQLAlchemy.

This module provides schema introspection methods for SAS databases.
These methods query SAS DICTIONARY tables to retrieve metadata about:
- Schema (library) names
- Table names
- View names
- Column metadata
- Index information
- Table comments

The SASIntrospectionMixin class is designed to be mixed into the main
SASDialect class to provide these introspection capabilities.
"""

import logging

from spinta.datasets.backends.sql.backends.sas.formats import map_sas_type_to_sqlalchemy
from spinta.datasets.backends.sql.backends.sas.constants import is_sas_missing_value

logger = logging.getLogger(__name__)


class SASIntrospectionMixin:
    """
    Mixin class providing SAS schema introspection methods.

    These methods query SAS DICTIONARY tables to retrieve metadata
    about libraries, tables, columns, and indexes.

    All methods include graceful error handling that returns empty
    results rather than raising exceptions, ensuring that SQLAlchemy
    inspection commands work even with partial database access.

    Attributes:
        default_schema_name: The default schema (library) name
        _type_mapping_cache: Cache for type mapping performance
    """

    # These attributes are expected to be set by the main dialect class
    default_schema_name: str = ""
    _type_mapping_cache: dict

    def _execute_query_with_fallback(self, connection, query, process_fn, description, fallback_value, params=()):
        """
        Execute a query with error handling and fallback.

        Args:
            connection: Database connection
            query: SQL query string
            process_fn: Function to process result rows
            description: Description for logging
            fallback_value: Value to return on error
            params: Query parameters tuple

        Returns:
            Processed result or fallback_value on error
        """
        try:
            result = connection.execute(query, params) if params else connection.execute(query)
            return process_fn(result)
        except Exception as e:
            logger.error(f"Failed to retrieve {description}: {e}. Returning fallback value.")
            return fallback_value

    def _safe_value_to_str(self, value):
        """
        Safely convert SAS values to strings, handling strings and Java numeric types.

        Returns:
            String representation of the value, or None if value is None
        """
        if value is None:
            return None
        return value.strip() if isinstance(value, str) else str(value)

    def _process_sas_numeric_value(self, value):
        """
        Process SAS numeric values, handling missing values and special cases.

        Returns:
            The numeric value or None if it represents a missing value
        """
        return None if is_sas_missing_value(value) else value

    def get_schema_names(self, connection, **kw):
        """
        Retrieve list of schema (library) names from SAS.

        Returns:
            List of schema names (library names), empty list on error
        """
        if hasattr(connection, "dialect") and getattr(connection.dialect, "default_schema_name", None):
            return [connection.dialect.default_schema_name]

        return self._execute_query_with_fallback(
            connection,
            """
            SELECT DISTINCT libname
            FROM dictionary.libnames
            WHERE libname IS NOT NULL
            ORDER BY libname
            """,
            lambda rows: [row[0].strip() for row in rows],
            "schema names",
            [],
        )

    def get_table_names(self, connection, schema=None, **kw):
        """
        Retrieve list of table names from a schema.

        Returns:
            List of table names, empty list on error
        """
        schema = schema or self.default_schema_name
        logger.debug(f"get_table_names: querying schema='{schema}'")

        def process_result(rows):
            table_names = [row[0].strip() for row in rows]
            logger.debug(f"get_table_names: found {len(table_names)} tables in schema '{schema}': {table_names[:5]}...")
            return table_names

        return self._execute_query_with_fallback(
            connection,
            """
            SELECT memname
            FROM dictionary.tables
            WHERE libname = ? AND memtype = 'DATA'
            ORDER BY memname
            """,
            process_result,
            f"table names for schema {schema}",
            [],
            (schema.upper() if schema else None,),
        )

    def get_view_names(self, connection, schema=None, **kw):
        """
        Retrieve list of view names from a schema.

        Returns:
            List of view names, empty list on error
        """
        schema = schema or self.default_schema_name

        return self._execute_query_with_fallback(
            connection,
            """
            SELECT memname
            FROM dictionary.tables
            WHERE libname = ? AND memtype = 'VIEW'
            ORDER BY memname
            """,
            lambda rows: [row[0].strip() for row in rows],
            f"view names for schema {schema}",
            [],
            (schema.upper() if schema else None,),
        )

    def get_columns(self, connection, table_name, schema=None, **kw):
        """
        Retrieve column metadata for a table.

        Returns:
            List of column dictionaries with metadata, empty list on error
        """
        schema = schema or self.default_schema_name

        def process_columns(result):
            columns = []
            for row in result:
                col_name = self._safe_value_to_str(row[0])
                col_type = self._safe_value_to_str(row[1])
                col_length = self._safe_value_to_str(row[2])
                col_format = self._safe_value_to_str(row[3])
                col_label = self._safe_value_to_str(row[4])
                col_notnull = bool(row[5]) if row[5] is not None else False

                sa_type = map_sas_type_to_sqlalchemy(col_type, col_length, col_format, self._type_mapping_cache)

                column_info = {
                    "name": col_name,
                    "type": sa_type,
                    "nullable": not col_notnull,
                    "default": None,
                }

                if col_label:
                    column_info["comment"] = col_label

                columns.append(column_info)
            return columns

        return self._execute_query_with_fallback(
            connection,
            """
            SELECT name, type, length, format, label, notnull
            FROM dictionary.columns
            WHERE libname = ? AND memname = ?
            ORDER BY varnum
            """,
            process_columns,
            f"columns for table {table_name}",
            [],
            (schema.upper() if schema else None, table_name.upper()),
        )

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """SAS does not support primary key constraints."""
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """SAS does not support foreign key constraints."""
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        """
        Retrieve index information for a table.

        Returns:
            List of index dictionaries with metadata, empty list on error
        """
        schema = schema or self.default_schema_name

        def process_indexes(result):
            indexes = {}
            for row in result:
                idx_name = self._safe_value_to_str(row[0])
                col_name = self._safe_value_to_str(row[1])
                is_unique = bool(row[2]) if row[2] is not None else False

                if idx_name not in indexes:
                    indexes[idx_name] = {"name": idx_name, "column_names": [], "unique": is_unique}

                indexes[idx_name]["column_names"].append(col_name)

            return list(indexes.values())

        return self._execute_query_with_fallback(
            connection,
            """
            SELECT indxname, name, unique
            FROM dictionary.indexes
            WHERE libname = ? AND memname = ?
            ORDER BY indxname, indxpos
            """,
            process_indexes,
            f"indexes for table {table_name}",
            [],
            (schema.upper() if schema else None, table_name.upper()),
        )

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        """
        Retrieve table comment (label).

        Returns:
            Dictionary with 'text' key containing the comment
        """
        schema = schema or self.default_schema_name

        def process_comment(result):
            row = result.fetchone()
            return {"text": row[0].strip() if row and row[0] else None}

        return self._execute_query_with_fallback(
            connection,
            """
            SELECT memlabel
            FROM dictionary.tables
            WHERE libname = ? AND memname = ?
            """,
            process_comment,
            f"table comment for {table_name}",
            {"text": None},
            (schema.upper() if schema else None, table_name.upper()),
        )

    def has_table(self, connection, table_name, schema=None, **kw):
        """
        Check if a table exists in the schema.

        Returns:
            True if table exists, False otherwise (including on errors)
        """
        schema = schema or self.default_schema_name

        def check_existence(result):
            row = result.fetchone()
            return int(row[0]) > 0 if row else False

        return self._execute_query_with_fallback(
            connection,
            """
            SELECT COUNT(*)
            FROM dictionary.tables
            WHERE libname = ? AND memname = ? AND memtype = 'DATA'
            """,
            check_existence,
            f"table existence for {table_name}",
            False,
            (schema.upper() if schema else None, table_name.upper()),
        )

    def has_sequence(self, connection, sequence_name, schema=None, **kw):
        """SAS does not support sequences."""
        return False
