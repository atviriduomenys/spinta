from typing import Any

from spinta.core.ufuncs import ufunc
from spinta.datasets.backends.sql.backends.postgresql.helpers import flip_geometry_postgis, group_array
from spinta.datasets.backends.sql.backends.postgresql.ufuncs.query.components import PostgreSQLQueryBuilder
from spinta.datasets.backends.sql.backends.helpers import nulls_last_asc, nulls_first_desc
from spinta.types.geometry.components import Geometry
from spinta.ufuncs.components import ForeignProperty
from spinta.ufuncs.querybuilder.components import Flip, Selected

import sqlalchemy as sa


@ufunc.resolver(PostgreSQLQueryBuilder, Geometry, Flip)
def select(env: PostgreSQLQueryBuilder, dtype: Geometry, func_: Flip):
    table = env.backend.get_table(env.model)

    if dtype.prop.list is None:
        column = env.backend.get_column(table, dtype.prop, select=True)
    else:
        column = env.backend.get_column(table, dtype.prop.list, select=True)

    column = flip_geometry_postgis(column)
    return Selected(
        item=env.add_column(column),
        prop=dtype.prop,
    )


@ufunc.resolver(PostgreSQLQueryBuilder, ForeignProperty, Geometry, Flip)
def select(env: PostgreSQLQueryBuilder, fpr: ForeignProperty, dtype: Geometry, func_: Flip):
    table = env.joins.get_table(env, fpr)
    if dtype.prop.list is None:
        column = env.backend.get_column(table, dtype.prop, select=True)
    else:
        column = env.backend.get_column(table, dtype.prop.list, select=True)

    column = flip_geometry_postgis(column)
    return Selected(
        item=env.add_column(column),
        prop=dtype.prop,
    )


@ufunc.resolver(PostgreSQLQueryBuilder, object)
def _group_array(env: PostgreSQLQueryBuilder, columns: Any):
    return group_array(columns)


@ufunc.resolver(PostgreSQLQueryBuilder, sa.sql.expression.ColumnElement)
def asc(env, column):
    return nulls_last_asc(column)


@ufunc.resolver(PostgreSQLQueryBuilder, sa.sql.expression.ColumnElement)
def desc(env, column):
    return nulls_first_desc(column)
