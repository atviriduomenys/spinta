from spinta.core.ufuncs import ufunc
from spinta.datasets.backends.sql.backends.oracle.ufuncs.query.components import OracleQueryBuilder

import sqlalchemy as sa

from spinta.datasets.backends.sql.backends.helpers import nulls_last_asc, nulls_first_desc


@ufunc.resolver(OracleQueryBuilder, sa.sql.expression.ColumnElement)
def asc(env, column):
    return nulls_last_asc(column)


@ufunc.resolver(OracleQueryBuilder, sa.sql.expression.ColumnElement)
def desc(env, column):
    return nulls_first_desc(column)
