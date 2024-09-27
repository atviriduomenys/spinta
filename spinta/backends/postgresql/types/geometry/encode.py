from typing import Any
from typing import Dict
from typing import overload

from geoalchemy2 import WKBElement
from geoalchemy2.shape import to_shape


from spinta import commands
from spinta.backends.postgresql.types.geometry.helpers import get_display_value
from spinta.backends.postgresql.types.geometry.helpers import get_osm_link
from spinta.formats.components import Format
from spinta.components import Action
from spinta.components import Context
from spinta.formats.html.components import Cell
from spinta.formats.html.components import Html
from spinta.types.geometry.components import Geometry


@overload
@commands.prepare_dtype_for_response.register(Context, Format, Geometry, WKBElement)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: Geometry,
    value: WKBElement,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    shape = to_shape(value)
    return shape.wkt


@overload
@commands.prepare_dtype_for_response.register(Context, Html, Geometry, WKBElement)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: Geometry,
    value: WKBElement,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    shape = to_shape(value)
    display_value = get_display_value(shape)
    osm_link = get_osm_link(shape, dtype)
    return Cell(display_value, link=osm_link)
