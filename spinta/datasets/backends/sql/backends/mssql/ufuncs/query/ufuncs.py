from typing import Any

from spinta.core.ufuncs import ufunc
from spinta.datasets.backends.sql.backends.mssql.helpers import group_array
from spinta.datasets.backends.sql.backends.mssql.ufuncs.query.components import MSSQLQueryBuilder


@ufunc.resolver(MSSQLQueryBuilder, object)
def _group_array(env: MSSQLQueryBuilder, columns: Any):
    return group_array(columns)
