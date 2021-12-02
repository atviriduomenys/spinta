from geoalchemy2 import WKBElement
from geoalchemy2.shape import to_shape

from spinta import commands
from spinta.formats.components import Format
from spinta.components import Action
from spinta.components import Context
from spinta.types.geometry.components import Geometry


@commands.prepare_dtype_for_response.register(Context, Geometry, Format, WKBElement)
def prepare_dtype_for_response(
    context: Context,
    dtype: Geometry,
    fmt: Format,
    value: WKBElement,
    *,
    action: Action,
    select: dict = None,
):
    shape = to_shape(value)
    return shape.wkt
