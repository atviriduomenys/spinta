"""
Tests for Oracle LONG RAW type support in SQLAlchemy.

These tests verify that the LONGRAW type is properly registered with
SQLAlchemy's Oracle dialect and can be used in table definitions and
schema reflection operations.
"""

import importlib
import sys
from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy.dialects.oracle import base
from spinta.datasets.backends.sql.backends.oracle.helpers import LONGRAW


def test_longraw_mapping():
    """Test that LONG RAW is properly mapped to LONGRAW type."""
    assert "LONG RAW" in base.ischema_names
    assert base.ischema_names["LONG RAW"] == LONGRAW


def test_sql_generation():
    """Test that SQL generation works for LONGRAW type."""
    dialect = base.OracleDialect()
    compiler = base.OracleTypeCompiler(dialect)

    longraw_type = LONGRAW()
    sql = compiler.process(longraw_type)

    assert sql == "LONG RAW"


def test_patch_verification():
    """Test that the OracleTypeCompiler has been properly patched."""
    assert hasattr(base.OracleTypeCompiler, "visit_LONGRAW")

    # Verify the method is callable
    dialect = base.OracleDialect()
    compiler = base.OracleTypeCompiler(dialect)
    assert callable(getattr(compiler, "visit_LONGRAW"))


def test_type_instantiation_in_table():
    """Test that LONGRAW can be used in table definitions."""
    from sqlalchemy.schema import CreateTable

    metadata = MetaData()

    # Create a table with a LONGRAW column
    test_table = Table(
        "test_longraw_table",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("binary_data", LONGRAW),
    )

    # Verify the column type is LONGRAW
    binary_column = test_table.c.binary_data
    assert isinstance(binary_column.type, LONGRAW)

    # Verify SQL generation for the table using CREATE TABLE
    dialect = base.OracleDialect()
    create_table = CreateTable(test_table)
    ddl = str(create_table.compile(dialect=dialect))

    # The DDL should contain LONG RAW
    assert "LONG RAW" in ddl
    # Verify the full structure
    assert "test_longraw_table" in ddl
    assert "binary_data" in ddl


def test_double_import_safety():
    """Test that the module can be safely imported multiple times."""
    # Get the module name
    module_name = "spinta.datasets.backends.sql.backends.oracle.helpers"

    # Store original registration state
    original_mapping = base.ischema_names.get("LONG RAW")
    original_has_visitor = hasattr(base.OracleTypeCompiler, "visit_LONGRAW")

    # Reload the module (simulates multiple imports)
    if module_name in sys.modules:
        importlib.reload(sys.modules[module_name])

    # Verify the registration is still correct
    assert "LONG RAW" in base.ischema_names
    assert base.ischema_names["LONG RAW"] == LONGRAW
    assert hasattr(base.OracleTypeCompiler, "visit_LONGRAW")

    # Verify no duplicates or errors occurred
    assert original_mapping == LONGRAW
    assert original_has_visitor is True


def test_longraw_inherits_largebinary():
    """Test that LONGRAW properly inherits from LargeBinary."""
    from sqlalchemy.sql.sqltypes import LargeBinary

    longraw_instance = LONGRAW()
    assert isinstance(longraw_instance, LargeBinary)
    assert isinstance(longraw_instance, LONGRAW)


def test_visit_name_attribute():
    """Test that LONGRAW has the correct __visit_name__ attribute."""
    assert hasattr(LONGRAW, "__visit_name__")
    assert LONGRAW.__visit_name__ == "LONGRAW"

    # Verify instance also has access to it
    longraw_instance = LONGRAW()
    assert longraw_instance.__visit_name__ == "LONGRAW"
