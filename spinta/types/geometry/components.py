from spinta.types.datatype import DataType


class Geometry(DataType):
    geometry_type: str | None = None
    srid: int = None


class Spatial(Geometry):
    pass
