from __future__ import annotations

from spinta.datasets.backends.sql.backends.mysql.components import MySQL
from spinta.datasets.backends.sql.ufuncs.query.components import SqlQueryBuilder


class MySQLQueryBuilder(SqlQueryBuilder):
    backend: MySQL
