import geoalchemy2 as ga
import sqlalchemy as sa

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_column_name
from spinta.components import Context
from spinta.types.geometry.components import Geometry


@commands.prepare.register(Context, PostgreSQL, Geometry)
def prepare(
    context: Context,
    backend: PostgreSQL,
    dtype: Geometry,
) -> sa.Column:
    prop = dtype.prop
    name = get_column_name(prop)
    kwargs = {
        'geometry_type': dtype.geometry_type,
        'srid': dtype.srid,
    }
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    column_type = ga.Geometry(**kwargs)
    return sa.Column(name, column_type, unique=dtype.unique)
