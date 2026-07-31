from __future__ import annotations

from spinta.datasets.backends.sql.backends.mariadb.components import MariaDB
from spinta.datasets.backends.sql.ufuncs.query.components import SqlQueryBuilder


class MariaDBQueryBuilder(SqlQueryBuilder):
    backend: MariaDB
