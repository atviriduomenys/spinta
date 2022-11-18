from typing import Union
from urllib.parse import urlencode

from geoalchemy2 import WKBElement
from geoalchemy2.shape import to_shape

from pyproj import CRS, Transformer
from shapely.ops import transform

from shapely.geometry import Point

from spinta.types.geometry.constants import WGS84


def _get_osm_link(value: WKBElement) -> str:
    shape = to_shape(value)
    srid_value_is_not_defined = -1
    if value.srid not in [WGS84, srid_value_is_not_defined]:
        wgs_crs = CRS(f'EPSG:{WGS84}')
        value_crs = CRS(f'EPSG:{value.srid}')
        project = Transformer.from_crs(crs_from=value_crs, crs_to=wgs_crs, always_xy=True).transform
        centroid = transform(project, shape).centroid
    else:
        centroid = shape.centroid
    x, y = centroid.x, centroid.y
    params = urlencode({'mlat': x, 'mlon': y})
    return f'https://www.openstreetmap.org/?{params}#map=19/{x}/{y}'


def _get_display_value(shape) -> Union[object, str]:
    if isinstance(shape, Point):
        value = shape.wkt
    else:
        # Shorten possibly long WKT values by reducing it to shape type.
        value = shape.type.upper()
    return value
