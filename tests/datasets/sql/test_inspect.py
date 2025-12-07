from sqlalchemy.dialects import mysql

from spinta.manifests.sql.helpers import _get_column_type


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
