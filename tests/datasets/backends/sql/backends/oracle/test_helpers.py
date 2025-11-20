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
