from functools import lru_cache
from typing import Optional, Union
from urllib.parse import urlencode

import shapely
from pyproj import CRS, Transformer
from shapely.geometry import Point
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform

from spinta.exceptions import SRIDNotSetForGeometry
from spinta.types.geometry.components import Geometry
from spinta.types.geometry.constants import OPENSTREETMAP_URL
from spinta.types.geometry.constants import WGS84


@lru_cache
def is_crs_always_xy(srid: int) -> bool:
    crs = CRS.from_user_input(srid)
    # Pseudo-Mercator WGS 84, always east, north
    crs_wgs84 = CRS.from_user_input(3857)

    transformer = Transformer.from_crs(crs, crs_wgs84)
    transformer_xy = Transformer.from_crs(crs, crs_wgs84, always_xy=True)

    transformed = transformer.transform(10, 20)
    transformer_xy = transformer_xy.transform(10, 20)

    return transformed == transformer_xy


@lru_cache
def get_crs_bounding_area(srid: int) -> shapely.geometry.Polygon:
    crs = CRS.from_user_input(srid)
    transformer = Transformer.from_crs(crs.geodetic_crs, crs, always_xy=True)
    west, south, east, north = transformer.transform_bounds(*crs.area_of_use.bounds)

    is_lon_lat = is_crs_always_xy(srid)

    if is_lon_lat:
        return shapely.geometry.box(minx=west, maxx=east, miny=south, maxy=north)

    return shapely.geometry.box(minx=south, maxx=north, miny=west, maxy=east)


def get_osm_link(value: BaseGeometry, srid: Optional[Union[int, Geometry]]) -> Optional[str]:
    if isinstance(srid, Geometry):
        if srid.srid is None:
            raise SRIDNotSetForGeometry(srid, property=srid.prop)
        srid = srid.srid

    if srid and srid != WGS84:
        transformer = Transformer.from_crs(
            crs_from=CRS(f"EPSG:{srid}"),
            crs_to=CRS(f"EPSG:{WGS84}"),
        )
        value = transform(transformer.transform, value)
    centroid = value.centroid

    # According to WGS84 (4326) `axis_info`
    # Axis order is:
    # x: Axis(name=Geodetic latitude, abbrev=Lat, direction=north, unit_auth_code=EPSG, unit_code=9122, unit_name=degree)
    # y: Axis(name=Geodetic longitude, abbrev=Lon, direction=east, unit_auth_code=EPSG, unit_code=9122, unit_name=degree)
    # Meaning x - lat, y - lon
    lat, lon = centroid.x, centroid.y
    params = urlencode({"mlat": lat, "mlon": lon})

    return f"{OPENSTREETMAP_URL}/?{params}#map=19/{lat}/{lon}"


def get_display_value(shape) -> Union[object, str]:
    if isinstance(shape, Point):
        value = shape.wkt
    else:
        # Shorten possibly long WKT values by reducing it to shape type.
        value = shape.geom_type.upper()
    return value
