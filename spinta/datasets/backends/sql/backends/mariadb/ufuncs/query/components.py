from __future__ import annotations

from spinta.datasets.backends.sql.ufuncs.query.components import SqlQueryBuilder
from spinta.datasets.backends.sql.backends.mariadb.components import MariaDB


class MariaDBQueryBuilder(SqlQueryBuilder):
    backend: MariaDB
