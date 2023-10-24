from sqlalchemy.dialects import registry
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite


class SQLiteDialect_spinta_sqlite(SQLiteDialect_pysqlite):
    driver = 'spinta_sqlite'
    supports_statement_cache = True

    @classmethod
    def dbapi(cls):
        try:
            from sqlean import dbapi2 as sqlite
        except ImportError:
            from sqlite3 import dbapi2 as sqlite
        return sqlite


registry.register(
    "sqlite.spinta_sqlite",
    __name__,
    "SQLiteDialect_spinta_sqlite"
)
