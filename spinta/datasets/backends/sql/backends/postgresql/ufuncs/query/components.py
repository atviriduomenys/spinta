from __future__ import annotations

from spinta.datasets.backends.sql.backends.postgresql.components import PostgreSQL
from spinta.datasets.backends.sql.ufuncs.query.components import SqlQueryBuilder


class PostgreSQLQueryBuilder(SqlQueryBuilder):
    backend: PostgreSQL
