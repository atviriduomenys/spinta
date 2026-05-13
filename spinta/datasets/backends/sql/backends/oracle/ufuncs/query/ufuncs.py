from spinta.core.ufuncs import ufunc
from spinta.datasets.backends.sql.backends.oracle.helpers import flip_geometry_oracle
from spinta.datasets.backends.sql.backends.oracle.ufuncs.query.components import OracleQueryBuilder
from spinta.types.geometry.components import Geometry
from spinta.ufuncs.components import ForeignProperty
from spinta.ufuncs.querybuilder.components import Flip, Selected

import sqlalchemy as sa

from spinta.datasets.backends.sql.backends.helpers import nulls_last_asc, nulls_first_desc


@ufunc.resolver(OracleQueryBuilder, sa.sql.expression.ColumnElement)
def asc(env, column):
    return nulls_last_asc(column)


@ufunc.resolver(OracleQueryBuilder, sa.sql.expression.ColumnElement)
def desc(env, column):
    return nulls_first_desc(column)


@ufunc.resolver(OracleQueryBuilder, Geometry, Flip)
def select(env: OracleQueryBuilder, dtype: Geometry, func_: Flip):
    table = env.backend.get_table(env.model)

    if dtype.prop.list is None:
        column = env.backend.get_column(table, dtype.prop, select=True)
    else:
        column = env.backend.get_column(table, dtype.prop.list, select=True)

    column = flip_geometry_oracle(column)
    return Selected(
        item=env.add_column(column),
        prop=dtype.prop,
    )


@ufunc.resolver(OracleQueryBuilder, ForeignProperty, Geometry, Flip)
def select(env: OracleQueryBuilder, fpr: ForeignProperty, dtype: Geometry, func_: Flip):
    table = env.joins.get_table(env, fpr)

    if dtype.prop.list is None:
        column = env.backend.get_column(table, dtype.prop, select=True)
    else:
        column = env.backend.get_column(table, dtype.prop.list, select=True)

    column = flip_geometry_oracle(column)
    return Selected(
        item=env.add_column(column),
        prop=dtype.prop,
    )
