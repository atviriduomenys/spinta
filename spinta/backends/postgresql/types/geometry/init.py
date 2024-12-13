import geoalchemy2 as ga
import sqlalchemy as sa

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_column_name, get_pg_name
from spinta.components import Context
from spinta.types.geometry.components import Geometry


@commands.prepare.register(Context, PostgreSQL, Geometry)
def prepare(
    context: Context,
    backend: PostgreSQL,
    dtype: Geometry,
    **kwargs
) -> list:
    prop = dtype.prop
    name = get_column_name(prop)

    # Have to disable spatial index and create our own, because Geoalchemy2 uses their own naming convention
    kwargs = {
        'geometry_type': dtype.geometry_type,
        'srid': dtype.srid,
        'spatial_index': False
    }
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    column_type = ga.Geometry(**kwargs)
    nullable = not dtype.required
    columns = [
        column := sa.Column(name, column_type, nullable=nullable),
        sa.Index(None, column, postgresql_using='GIST')
    ]

    # Technically speaking we shouldn't create UniqueConstraint and Index at the same time
    # behind the scenes UniqueConstraint is an Index
    # but this uses GIST for Index, not BTREE
    # need to investigate this further
    if dtype.unique:
        columns.append(
            sa.UniqueConstraint(column)
        )
    return columns
