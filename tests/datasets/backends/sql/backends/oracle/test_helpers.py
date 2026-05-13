"""
Tests for Oracle LONG RAW and SDO_GEOMETRY type support in SQLAlchemy.

These tests verify that both LONGRAW and SDO_GEOMETRY types are properly
registered with SQLAlchemy's Oracle dialect and can be used in table
definitions and schema reflection operations.
"""

import importlib
import sys
from sqlalchemy import Column, Integer, MetaData, Table, select
from sqlalchemy.dialects.oracle import base
from spinta.datasets.backends.sql.backends.oracle.helpers import LONGRAW, SDO_GEOMETRY


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


# SDO_GEOMETRY Tests
def test_sdo_geometry_mapping():
    """Test that SDO_GEOMETRY is properly mapped."""
    assert "SDO_GEOMETRY" in base.ischema_names
    assert base.ischema_names["SDO_GEOMETRY"] == SDO_GEOMETRY


def test_sdo_geometry_sql_generation():
    """Test that SQL generation works for SDO_GEOMETRY type."""
    dialect = base.OracleDialect()
    compiler = base.OracleTypeCompiler(dialect)

    sdo_type = SDO_GEOMETRY()
    sql = compiler.process(sdo_type)

    assert sql == "SDO_GEOMETRY"


def test_sdo_geometry_column_expression():
    """
    Verify column_expression wraps with SDO_UTIL.TO_WKTGEOMETRY().

    This test ensures that SELECT queries wrap SDO_GEOMETRY columns with
    SDO_UTIL.TO_WKTGEOMETRY() to convert geometries to WKT strings at the
    database level. Invalid geometries are handled with NVL to return NULL.

    This approach avoids cx_Oracle.Object conversion issues by performing
    the conversion at the SQL level.
    """
    metadata = MetaData()
    table = Table("test", metadata, Column("geom", SDO_GEOMETRY()))
    stmt = select(table.c.geom)

    dialect = base.OracleDialect()
    compiled = str(stmt.compile(dialect=dialect))

    # The compiled SQL should contain TO_WKTGEOMETRY function call
    assert "geom" in compiled.lower()
    assert "SDO_UTIL.TO_WKTGEOMETRY" in compiled.upper()
    assert "NVL" in compiled.upper()  # NULL handling


def test_sdo_geometry_result_processor():
    """
    Verify result processor converts WKT strings to shapely geometries.

    The result_processor receives WKT strings from SDO_UTIL.TO_WKTGEOMETRY()
    and parses them to shapely geometry objects. Invalid WKT or NULL values
    return None instead of raising errors.
    """
    from shapely.geometry import Point, LineString

    sdo_type = SDO_GEOMETRY()
    dialect = base.OracleDialect()
    processor = sdo_type.result_processor(dialect, None)

    # Test with valid WKT POINT string
    wkt_point = "POINT (30 10)"
    result = processor(wkt_point)
    assert isinstance(result, Point)
    assert result.x == 30
    assert result.y == 10

    # Test with valid WKT LINESTRING
    wkt_line = "LINESTRING (30 10, 10 30, 40 40)"
    result = processor(wkt_line)
    assert isinstance(result, LineString)
    assert len(result.coords) == 3

    # Test with None
    assert processor(None) is None

    # Test with invalid WKT (should return None with warning)
    result = processor("INVALID WKT STRING")
    assert result is None  # Should return None instead of raising


def test_sdo_geometry_result_processor_clob_handling():
    """
    Verify result processor handles CLOB/LOB objects from Oracle drivers.

    SDO_UTIL.TO_WKTGEOMETRY() returns CLOB type. Depending on the driver
    (cx_Oracle/python-oracledb) and geometry size, CLOBs may arrive as:
    - str: Auto-converted by driver (small geometries)
    - LOB object: With .read() method (large geometries)

    This test ensures both cases are handled properly.
    """
    from shapely.geometry import Point, Polygon

    sdo_type = SDO_GEOMETRY()
    dialect = base.OracleDialect()
    processor = sdo_type.result_processor(dialect, None)

    # Mock LOB object that simulates cx_Oracle CLOB behavior
    class MockCLOB:
        def __init__(self, data):
            self._data = data

        def read(self):
            return self._data

    # Test with CLOB object containing valid WKT POINT
    clob_point = MockCLOB("POINT (50 20)")
    result = processor(clob_point)
    assert isinstance(result, Point)
    assert result.x == 50
    assert result.y == 20

    # Test with CLOB object containing complex geometry (POLYGON)
    clob_polygon = MockCLOB("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")
    result = processor(clob_polygon)
    assert isinstance(result, Polygon)
    assert len(result.exterior.coords) == 5

    # Test with CLOB object containing large WKT (many coordinates)
    # Simulate a large geometry that would exceed VARCHAR2 limit (4000 bytes)
    coords = ", ".join([f"{i} {i}" for i in range(1000)])
    large_wkt = f"LINESTRING ({coords})"
    clob_large = MockCLOB(large_wkt)
    result = processor(clob_large)
    from shapely.geometry import LineString

    assert isinstance(result, LineString)
    assert len(result.coords) == 1000

    # Test with CLOB containing invalid WKT (should return None with warning)
    clob_invalid = MockCLOB("INVALID WKT DATA")
    result = processor(clob_invalid)
    assert result is None

    # Test with CLOB that fails to read
    class FailingCLOB:
        def read(self):
            raise Exception("CLOB read error")

    failing_clob = FailingCLOB()
    result = processor(failing_clob)
    assert result is None  # Should handle read errors gracefully


def test_sdo_geometry_initialization():
    """Test SDO_GEOMETRY initialization with geometry_type and srid."""
    # Test default initialization
    sdo_default = SDO_GEOMETRY()
    assert sdo_default.geometry_type is None
    assert sdo_default.srid is None

    # Test with geometry_type
    sdo_point = SDO_GEOMETRY(geometry_type="POINT")
    assert sdo_point.geometry_type == "POINT"
    assert sdo_point.srid is None

    # Test with srid
    sdo_with_srid = SDO_GEOMETRY(srid=4326)
    assert sdo_with_srid.geometry_type is None
    assert sdo_with_srid.srid == 4326

    # Test with both
    sdo_full = SDO_GEOMETRY(geometry_type="POLYGON", srid=3346)
    assert sdo_full.geometry_type == "POLYGON"
    assert sdo_full.srid == 3346


def test_sdo_geometry_cache_ok():
    """Test that SDO_GEOMETRY has cache_ok set to True for SQLAlchemy caching."""
    assert hasattr(SDO_GEOMETRY, "cache_ok")
    assert SDO_GEOMETRY.cache_ok is True


def test_sdo_geometry_visit_name():
    """Test that SDO_GEOMETRY has the correct __visit_name__ attribute."""
    assert hasattr(SDO_GEOMETRY, "__visit_name__")
    assert SDO_GEOMETRY.__visit_name__ == "SDO_GEOMETRY"

    sdo_instance = SDO_GEOMETRY()
    assert sdo_instance.__visit_name__ == "SDO_GEOMETRY"


def test_sdo_geometry_in_table():
    """Test that SDO_GEOMETRY can be used in table definitions."""
    from sqlalchemy.schema import CreateTable

    metadata = MetaData()

    test_table = Table(
        "test_sdo_table",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("shape", SDO_GEOMETRY()),
    )

    # Verify the column type is SDO_GEOMETRY
    shape_column = test_table.c.shape
    assert isinstance(shape_column.type, SDO_GEOMETRY)

    # Verify SQL generation for the table
    dialect = base.OracleDialect()
    create_table = CreateTable(test_table)
    ddl = str(create_table.compile(dialect=dialect))

    # The DDL should contain SDO_GEOMETRY
    assert "SDO_GEOMETRY" in ddl
    assert "test_sdo_table" in ddl
    assert "shape" in ddl


def test_sdo_geometry_double_import_safety():
    """Test that SDO_GEOMETRY registration is safe for multiple imports."""
    module_name = "spinta.datasets.backends.sql.backends.oracle.helpers"

    # Store original state
    original_mapping = base.ischema_names.get("SDO_GEOMETRY")
    original_has_visitor = hasattr(base.OracleTypeCompiler, "visit_SDO_GEOMETRY")

    # Reload the module
    if module_name in sys.modules:
        importlib.reload(sys.modules[module_name])

    # Verify registration is still correct
    assert "SDO_GEOMETRY" in base.ischema_names
    assert base.ischema_names["SDO_GEOMETRY"] == SDO_GEOMETRY
    assert hasattr(base.OracleTypeCompiler, "visit_SDO_GEOMETRY")

    # Verify no duplicates or errors
    assert original_mapping == SDO_GEOMETRY
    assert original_has_visitor is True
