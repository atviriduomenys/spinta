from __future__ import annotations

from spinta.datasets.backends.sql.backends.sqlite.components import Sqlite
from spinta.datasets.backends.sql.ufuncs.query.components import SqlQueryBuilder


class SqliteQueryBuilder(SqlQueryBuilder):
    backend: Sqlite
