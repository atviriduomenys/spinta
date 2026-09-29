import sqlalchemy as sa
from sqlalchemy.dialects import mysql, postgresql

from spinta.manifests.sql.helpers import _Column, _get_column_type


def test_char_type():
    column = {"name": "test", "type": mysql.CHAR()}
    table = "test"

    assert _get_column_type(column, table) == "string"


def test_tinyblob_type():
    column = {"name": "test", "type": mysql.TINYBLOB()}
    table = "test"

    assert _get_column_type(column, table) == "binary"


def test_blob_type():
    column = {"name": "test", "type": mysql.BLOB()}
    table = "test"

    assert _get_column_type(column, table) == "binary"


def test_mediumblob_type():
    column = {"name": "test", "type": mysql.MEDIUMBLOB()}
    table = "test"

    assert _get_column_type(column, table) == "binary"


def test_longblob_type():
    column = {"name": "test", "type": mysql.LONGBLOB()}
    table = "test"

    assert _get_column_type(column, table) == "binary"


def test_postgresql_json_type():
    """Test PostgreSQL JSON type maps to 'object' - supports issue #1701."""
    column = _Column(type=postgresql.JSON())
    table = "test"

    assert _get_column_type(column, table) == "object"


def test_postgresql_jsonb_type():
    """Test PostgreSQL JSONB type maps to 'object' - supports issue #1701."""
    column = _Column(type=postgresql.JSONB())
    table = "test"

    assert _get_column_type(column, table) == "object"


def test_mysql_json_type():
    """Test MySQL JSON type maps to 'object' - supports issue #1701."""
    column = _Column(type=mysql.JSON())
    table = "test"

    assert _get_column_type(column, table) == "object"


def test_json_type():
    # Test SQLAlchemy's generic JSON type
    column = _Column(type=sa.JSON())
    table = "test"

    assert _get_column_type(column, table) == "object"
