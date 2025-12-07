"""
Oracle LONG RAW type support for SQLAlchemy.

LONG RAW is a deprecated Oracle binary data type that can store up to 2GB of raw
binary data. While Oracle recommends using BLOB instead, LONG RAW columns still
exist in legacy databases and must be properly reflected when introspecting schemas.

This module provides:
- LONGRAW: A SQLAlchemy type class for LONG RAW columns
- Automatic registration with Oracle dialect for schema reflection
- SQL generation support for LONG RAW in DDL statements

The patches are applied at module import time to extend SQLAlchemy's Oracle
dialect functionality. The module is safe to import multiple times due to
defensive checks that prevent double-registration.
"""

from typing import Any
from sqlalchemy.sql.sqltypes import LargeBinary
from sqlalchemy.dialects.oracle import base


class LONGRAW(LargeBinary):
    """
    SQLAlchemy type for Oracle LONG RAW columns.

    LONG RAW is a legacy Oracle data type for variable-length binary data up to 2GB.
    This type extends LargeBinary to provide proper support for reflecting and
    working with existing LONG RAW columns in Oracle databases.

    Note: Oracle deprecated LONG RAW in favor of BLOB. New applications should use
    BLOB instead, but this type is needed for compatibility with legacy schemas.
    """

    __visit_name__ = "LONGRAW"


# Register LONG RAW in Oracle's type mapping for schema reflection
# This allows SQLAlchemy to properly recognize LONG RAW columns when reflecting
# table definitions from the database
if "LONG RAW" not in base.ischema_names:
    base.ischema_names["LONG RAW"] = LONGRAW

# Add SQL generation support for LONGRAW type
# This enables SQLAlchemy to generate correct DDL (CREATE TABLE statements)
# when LONGRAW type is used in table definitions
if not hasattr(base.OracleTypeCompiler, "visit_LONGRAW"):

    def visit_LONGRAW(self, type_: LONGRAW, **kw: Any) -> str:
        """
        Generate SQL representation for LONGRAW type.

        Args:
            type_: The LONGRAW type instance
            **kw: Additional keyword arguments from SQLAlchemy

        Returns:
            The SQL type string "LONG RAW"
        """
        return "LONG RAW"

    base.OracleTypeCompiler.visit_LONGRAW = visit_LONGRAW
