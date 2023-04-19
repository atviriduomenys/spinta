from typing import Optional
from typing import Union
from urllib.parse import urlencode

from geoalchemy2 import WKBElement
from geoalchemy2.shape import to_shape

from pyproj import CRS, Transformer
from shapely.ops import transform

from shapely.geometry import Point

from spinta.types.geometry.constants import WGS84


def get_osm_link(value: WKBElement, srid: Optional[int]) -> Optional[str]:
    if srid is None:
        # If we don't know how to correctly convert to WGS84, then we
        # can't return a link OSM.
        return None

    shape = to_shape(value)

    if srid and srid != WGS84:
        transformer = Transformer.from_crs(
            crs_from=CRS(f'EPSG:{srid}'),
            crs_to=CRS(f'EPSG:{WGS84}'),
        )
        shape = transform(transformer.transform, shape)

    centroid = shape.centroid
    lat, lon = centroid.x, centroid.y
    params = urlencode({'mlat': lat, 'mlon': lon})

    return f'https://www.openstreetmap.org/?{params}#map=19/{lat}/{lon}'


def get_display_value(shape) -> Union[object, str]:
    if isinstance(shape, Point):
        value = shape.wkt
    else:
        # Shorten possibly long WKT values by reducing it to shape type.
        value = shape.type.upper()
    return value
