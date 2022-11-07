from typing import Any
from typing import Dict
from urllib.parse import urlencode

from geoalchemy2 import WKBElement
from geoalchemy2.shape import to_shape

from pyproj import Transformer

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

    if WKBElement.srid in [4326, -1]:
        x, y = point.x, point.y
    else:
        proj = Transformer.from_proj(WKBElement.srid, 4326, always_xy=True)
        x, y = proj.transform(point.x, point.y)

    params = urlencode({'mlat': x, 'mlon': y})
    link = f'https://www.openstreetmap.org/?{params}#map=19/{x}/{y}'

    return Cell(shape.type == 'Point' and shape.wkt or shape.type.upper(), link=link)
