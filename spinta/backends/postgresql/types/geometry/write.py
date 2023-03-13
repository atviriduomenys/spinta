from spinta import commands
from spinta.components import Context, DataSubItem
from spinta.backends.postgresql.components import PostgreSQL
from spinta.types.geometry.components import Geometry

@commands.before_write.register(Context, Geometry, PostgreSQL)
def before_write(
    context: Context,
    dtype: Geometry,
    backend: PostgreSQL,
    *,
    data: DataSubItem):
    patch = {}
    if 'SRID' not in data.patch and dtype.srid is not None:
        patch = {dtype.prop.name: 'SRID='+str(dtype.srid)+';'+str(data.patch)}
    else:
        patch = {dtype.prop.name: str(data.patch)}
    return patch


