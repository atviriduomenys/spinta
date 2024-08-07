from sqlalchemy.dialects import mysql

from spinta.manifests.sql.helpers import _get_column_type


def test_char_type():
    column = {
        "name": "test",
        "type": mysql.CHAR()
    }
    table = "test"

    assert _get_column_type(column, table) == 'string'
