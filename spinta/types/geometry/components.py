from typing import Optional

from spinta.types.datatype import DataType


class Geometry(DataType):
    geometry_type: Optional[str] = None
    srid: int = None


class Spatial(Geometry):
    pass


