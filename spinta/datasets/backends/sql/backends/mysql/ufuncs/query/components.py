from __future__ import annotations

from spinta.datasets.backends.sql.ufuncs.query.components import SqlQueryBuilder
from spinta.datasets.backends.sql.backends.mysql.components import MySQL


class MySQLQueryBuilder(SqlQueryBuilder):
    backend: MySQL
