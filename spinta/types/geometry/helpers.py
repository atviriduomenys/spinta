from functools import lru_cache

import shapely
from pyproj import CRS, Transformer


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
        return shapely.geometry.box(
            minx=west,
            maxx=east,
            miny=south,
            maxy=north
        )

    return shapely.geometry.box(
        minx=south,
        maxx=north,
        miny=west,
        maxy=east
    )
