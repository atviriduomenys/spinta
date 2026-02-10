"""
SAS Identifier Handling for SQLAlchemy.

This module provides a custom identifier preparer for SAS SQL syntax.

SAS does not support quoted identifiers in SQL syntax. All identifiers
(table names, column names, schema names, etc.) must be returned unquoted
to prevent SQL syntax errors when executing queries.
"""

from sqlalchemy.sql.compiler import IdentifierPreparer


class SASIdentifierPreparer(IdentifierPreparer):
    """
    Custom identifier preparer for SAS that never quotes identifiers.

    SAS does not support quoted identifiers in SQL syntax. This preparer
    ensures that table names, column names, and other identifiers are never
    wrapped in quotes, preventing SQL syntax errors.

    Example:
        With default preparer: "MY_TABLE" (quoted)
        With SAS preparer: MY_TABLE (unquoted)
    """

    def quote(self, ident, force=None):
        """
        Return the identifier without quotes.

        Args:
            ident: The identifier to (not) quote
            force: Force parameter (ignored for SAS)

        Returns:
            The identifier unchanged, without any quoting
        """
        return ident

    def _requires_quotes(self, ident):
        """
        Determine if an identifier requires quotes.

        SAS never requires quotes for identifiers.

        Args:
            ident: The identifier to check

        Returns:
            Always False for SAS
        """
        return False

    def format_table(self, table, use_schema=True, name=None):
        """
        Format a table identifier for use in SQL statements.

        Ensures that table names include the schema (library) name when present.
        This is critical for SAS because without the schema, SAS defaults to the WORK library.

        Args:
            table: SQLAlchemy Table object
            use_schema: Whether to include the schema (default True)
            name: Optional name override

        Returns:
            Formatted table identifier string
        """
        # Always include schema for SAS if it's set on the table
        if table.schema:
            return f"{table.schema}.{table.name}"
        return table.name
