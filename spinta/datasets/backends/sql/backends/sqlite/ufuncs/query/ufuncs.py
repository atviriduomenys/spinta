from typing import Any

import sqlalchemy as sa

from spinta.core.ufuncs import ufunc
from spinta.datasets.backends.sql.backends.helpers import nulls_first_desc, nulls_last_asc
from spinta.datasets.backends.sql.backends.sqlite.helpers import group_array
from spinta.datasets.backends.sql.backends.sqlite.ufuncs.query.components import SqliteQueryBuilder


@ufunc.resolver(SqliteQueryBuilder, object)
def _group_array(env: SqliteQueryBuilder, columns: Any):
    return group_array(columns)


@ufunc.resolver(SqliteQueryBuilder, sa.sql.expression.ColumnElement)
def asc(env, column):
    return nulls_last_asc(column)


@ufunc.resolver(SqliteQueryBuilder, sa.sql.expression.ColumnElement)
def desc(env, column):
    return nulls_first_desc(column)
