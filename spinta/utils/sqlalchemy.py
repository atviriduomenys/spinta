import enum
from typing import Any

from sqlalchemy.cimmutabledict import immutabledict
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.dialects.sqlite import base

# We use SQLiteDialect_pysqlite, instead of SQLiteDialect, because most of the code is the same, we only want to change
# Where we get dbapi from
# In the future, if there are errors related with sqlean and pysqlite, we can fully create our own dialect

# You use `sqlite+spinta:///...` uri to access it explicitly
# Also because we set default to spinta you can also use `sqlite:///...`
class SQLiteDialect_spinta(SQLiteDialect_pysqlite):
    driver = 'spinta'
    supports_statement_cache = True

    def __init__(self, **kwargs):
        SQLiteDialect.__init__(self, **kwargs)

    @classmethod
    def dbapi(cls):
        try:
            from sqlean import dbapi2 as sqlite
        except ImportError:
            from sqlite3 import dbapi2 as sqlite
        return sqlite


registry.register(
    "sqlite.spinta",
    __name__,
    "SQLiteDialect_spinta"
)

# This changes default sqlite dialect from pysqlite to spinta
base.dialect = SQLiteDialect_spinta


class Convention(enum.Enum):
    UQ = "uq"
    IX = "ix"
    FK = "fk"
    CK = "ck"
    PK = "pk"


def get_metadata_naming_convention(
        naming_convention: dict[Convention, Any]
) -> immutabledict:
    return immutabledict({
        key.value: value
        for key, value in naming_convention.items()
    })
