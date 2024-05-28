from sqlalchemy.dialects import mysql

from spinta.manifests.sql.helpers import get_column_type


def test_char_type():
    assert get_column_type(mysql.CHAR()) == 'string'
