from sqlalchemy.dialects import mysql

from spinta.manifests.sql.helpers import _get_type


def test_char_type():
    assert _get_type(mysql.CHAR()) == 'string'
