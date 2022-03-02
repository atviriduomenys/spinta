from typing import Any
from typing import Dict
from urllib.parse import urlencode

from geoalchemy2 import WKBElement
from geoalchemy2.shape import to_shape

from spinta import commands
from spinta.formats.components import Format
from spinta.components import Action
from spinta.components import Context
from spinta.formats.html.components import Cell
from spinta.formats.html.components import Html
from spinta.types.geometry.components import Geometry


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
    point = shape.centroid
    params = urlencode({'mlat': point.x, 'mlon': point.y})
    link = f'https://www.openstreetmap.org/?{params}#map=19/{point.x}/{point.y}'
    return Cell(shape.wkt, link=link)
