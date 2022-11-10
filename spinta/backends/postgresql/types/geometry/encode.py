from typing import Any
from typing import Dict
from urllib.parse import urlencode

from pyproj import CRS, Transformer
from shapely.ops import transform

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
    if WKBElement.srid not in [4326, -1]:
        wgs_crs = CRS('EPSG:4326')
        srid_crs = CRS(f'EPSG:{WKBElement.srid}')
        project = Transformer.from_crs(crs_from=srid_crs, crs_to=wgs_crs, always_xy=True).transform
        shape = transform(project, to_shape(value))
    else:
        shape = to_shape(value)

    point = shape.centroid
    x, y = point.x, point.y
    params = urlencode({'mlat': x, 'mlon': y})
    link = f'https://www.openstreetmap.org/?{params}#map=19/{x}/{y}'
    shape_type = shape.type == 'Point' and shape.wkt or shape.type.upper()
    return Cell(shape_type, link=link)
