from geoalchemy2 import WKBElement
from geoalchemy2.shape import to_shape

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.components import Context
from spinta.components import Model
from spinta.types.geometry.components import Geometry


@commands.prepare_dtype_for_response.register(Context, PostgreSQL, Model, Geometry, WKBElement)
def prepare_dtype_for_response(
    context: Context,
    backend: PostgreSQL,
    model: Model,
    dtype: Geometry,
    value: WKBElement,
    *,
    select: dict = None,
):
    shape = to_shape(value)
    return shape.wkt
