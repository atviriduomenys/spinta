from __future__ import annotations

from spinta.datasets.backends.sql.ufuncs.query.components import SqlQueryBuilder

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from spinta.datasets.backends.sql.backends.oracle.components import Oracle


class OracleQueryBuilder(SqlQueryBuilder):
    backend: Oracle

