from __future__ import annotations

from spinta.datasets.backends.sql.backends.mssql.components import MSSQL
from spinta.datasets.backends.sql.ufuncs.query.components import SqlQueryBuilder


class MSSQLQueryBuilder(SqlQueryBuilder):
    backend: MSSQL
