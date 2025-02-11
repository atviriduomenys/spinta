from collections.abc import Sequence
from typing import Union

import sqlalchemy as sa
import geoalchemy2


def flip_geometry_postgis(column: sa.Column):
    return geoalchemy2.functions.ST_FlipCoordinates(column)


def group_array(column: Union[sa.Column, Sequence[sa.Column]]):
    if isinstance(column, Sequence) and not isinstance(column, str):
        column = sa.sql.func.jsonb_build_array(*column)
    return sa.sql.func.jsonb_agg(column)
