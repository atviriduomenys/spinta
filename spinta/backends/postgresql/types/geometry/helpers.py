from typing import Optional
from typing import Union
from urllib.parse import urlencode

from shapely.geometry.base import BaseGeometry

from pyproj import CRS, Transformer
from shapely.ops import transform

from shapely.geometry import Point

from spinta.exceptions import SRIDNotSetForGeometry
from spinta.types.geometry.components import Geometry
from spinta.types.geometry.constants import WGS84


def get_osm_link(value: BaseGeometry, srid: Optional[Union[int, Geometry]]) -> Optional[str]:
    if isinstance(srid, Geometry):
        if srid.srid is None:
            raise SRIDNotSetForGeometry(srid)
        srid = srid.srid

    if srid and srid != WGS84:
        transformer = Transformer.from_crs(
            crs_from=CRS(f'EPSG:{srid}'),
            crs_to=CRS(f'EPSG:{WGS84}'),
        )
        value = transform(transformer.transform, value)
    centroid = value.centroid

    # According to WGS84 (4326) `axis_info`
    # Axis order is:
    # x: Axis(name=Geodetic latitude, abbrev=Lat, direction=north, unit_auth_code=EPSG, unit_code=9122, unit_name=degree)
    # y: Axis(name=Geodetic longitude, abbrev=Lon, direction=east, unit_auth_code=EPSG, unit_code=9122, unit_name=degree)
    # Meaning x - lat, y - lon
    lat, lon = centroid.x, centroid.y
    params = urlencode({'mlat': lat, 'mlon': lon})

    return f'https://www.openstreetmap.org/?{params}#map=19/{lat}/{lon}'


def get_display_value(shape) -> Union[object, str]:
    if isinstance(shape, Point):
        value = shape.wkt
    else:
        # Shorten possibly long WKT values by reducing it to shape type.
        value = shape.geom_type.upper()
    return value
