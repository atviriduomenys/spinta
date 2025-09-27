from __future__ import annotations

from spinta.datasets.backends.sql.ufuncs.query.components import SqlQueryBuilder
from spinta.datasets.backends.sql.backends.oracle.components import Oracle


class OracleQueryBuilder(SqlQueryBuilder):
    backend: Oracle
