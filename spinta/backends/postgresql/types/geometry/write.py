from spinta import commands
from spinta.components import Context, DataSubItem
from spinta.backends.postgresql.components import PostgreSQL
from spinta.types.geometry.components import Geometry
from spinta.utils.data import take


@commands.before_write.register(Context, Geometry, PostgreSQL)
def before_write(
    context: Context,
    dtype: Geometry,
    backend: PostgreSQL,
    *,
    data: DataSubItem
):
    value = data.patch
    if (
        dtype.srid is not None and
        value and
        "SRID" not in value
    ):
        value = f"SRID={dtype.srid};{value}"
    return take({dtype.prop.place: value})
