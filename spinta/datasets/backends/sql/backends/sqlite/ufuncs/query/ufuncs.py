from typing import Any

from spinta.core.ufuncs import ufunc
from spinta.datasets.backends.sql.backends.sqlite.helpers import group_array
from spinta.datasets.backends.sql.backends.sqlite.ufuncs.query.components import SqliteQueryBuilder
from spinta.datasets.backends.sql.backends.helpers import nulls_last_asc, nulls_first_desc

import sqlalchemy as sa


@ufunc.resolver(SqliteQueryBuilder, object)
def _group_array(env: SqliteQueryBuilder, columns: Any):
    return group_array(columns)


@ufunc.resolver(SqliteQueryBuilder, sa.sql.expression.ColumnElement)
def asc(env, column):
    return nulls_last_asc(column)


@ufunc.resolver(SqliteQueryBuilder, sa.sql.expression.ColumnElement)
def desc(env, column):
    return nulls_first_desc(column)
