from typing import Any

from spinta.core.ufuncs import ufunc
from spinta.datasets.backends.sql.backends.mariadb.helpers import group_array
from spinta.datasets.backends.sql.backends.mariadb.ufuncs.query.components import MariaDBQueryBuilder


@ufunc.resolver(MariaDBQueryBuilder, object)
def _group_array(env: MariaDBQueryBuilder, columns: Any):
    return group_array(columns)
