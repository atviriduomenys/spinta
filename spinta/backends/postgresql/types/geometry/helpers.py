from typing import Optional
from typing import Union
from urllib.parse import urlencode

from geoalchemy2 import WKBElement
from geoalchemy2.shape import to_shape

from pyproj import CRS, Transformer
from shapely.ops import transform

from shapely.geometry import Point

from spinta.types.geometry.constants import WGS84
from spinta.exceptions import LatitudeOutOfRange, LongitudeOutOfRange


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
    if srid and srid != WGS84:
        if 'bbox' in transformer.to_json_dict():
            south_lat = transformer.to_json_dict().get('bbox')['south_latitude']
            north_lat = transformer.to_json_dict().get('bbox')['north_latitude']
            east_lon = transformer.to_json_dict().get('bbox')['east_longitude']
            west_lon = transformer.to_json_dict().get('bbox')['west_longitude']
            if not south_lat < lat < north_lat:
                raise LatitudeOutOfRange
            if not west_lon < lon < east_lon:
                raise LongitudeOutOfRange

    return f'https://www.openstreetmap.org/?{params}#map=19/{lat}/{lon}'


def get_display_value(shape) -> Union[object, str]:
    if isinstance(shape, Point):
        value = shape.wkt
    else:
        # Shorten possibly long WKT values by reducing it to shape type.
        value = shape.type.upper()
    return value
