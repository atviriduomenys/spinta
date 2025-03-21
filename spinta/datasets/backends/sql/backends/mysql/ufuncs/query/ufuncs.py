from typing import Any

from spinta.core.ufuncs import ufunc
from spinta.datasets.backends.sql.backends.mysql.helpers import group_array
from spinta.datasets.backends.sql.backends.mysql.ufuncs.query.components import MySQLQueryBuilder


@ufunc.resolver(MySQLQueryBuilder, object)
def _group_array(env: MySQLQueryBuilder, columns: Any):
    return group_array(columns)
