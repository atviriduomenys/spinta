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
    data: DataSubItem):
    patch = take(all, {dtype.prop.place: data.patch})
    if patch.get(dtype.prop.place) is not None and 'SRID' not in data.patch and dtype.srid is not None:
        patch[dtype.prop.place] = f"SRID={dtype.srid};{data.patch}"
    return patch


