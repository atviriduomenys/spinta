from typing import Any
from typing import Dict
from urllib.parse import urlencode

from pyproj import CRS, Transformer
from shapely.ops import transform

from geoalchemy2 import WKBElement
from geoalchemy2.shape import to_shape

from shapely.geometry import Point

from spinta import commands
from spinta.formats.components import Format
from spinta.components import Action
from spinta.components import Context
from spinta.formats.html.components import Cell
from spinta.formats.html.components import Html
from spinta.types.geometry.components import Geometry
from spinta.types.geometry.constants import WGS84


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
    shape_origin = to_shape(value)
    if value.srid not in [WGS84, -1]:
        wgs_crs = CRS(f'EPSG:{WGS84}')
        value_crs = CRS(f'EPSG:{value.srid}')
        project = Transformer.from_crs(crs_from=value_crs, crs_to=wgs_crs, always_xy=True).transform
        shape = transform(project, shape_origin)
    else:
        shape = shape_origin

    point = shape.centroid
    x, y = point.x, point.y

    params = urlencode({'mlat': x, 'mlon': y})
    link = f'https://www.openstreetmap.org/?{params}#map=19/{x}/{y}'

    if isinstance(shape_origin, Point):
        value = shape_origin.wkt
    else:
        # Shorten possibly long WKT values by reducing it to shape type.
        value = shape_origin.type.upper()
    return Cell(value, link=link)
