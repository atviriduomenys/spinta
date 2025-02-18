from __future__ import annotations

from spinta.datasets.backends.sql.ufuncs.query.components import SqlQueryBuilder
from spinta.testing.datasets import Sqlite


class SqliteQueryBuilder(SqlQueryBuilder):
    backend: Sqlite
