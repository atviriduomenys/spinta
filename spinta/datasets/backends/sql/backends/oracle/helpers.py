from typing import Any, Optional, Callable
from sqlalchemy.sql.sqltypes import LargeBinary
from sqlalchemy.types import UserDefinedType
from sqlalchemy.dialects.oracle import base
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy import Column
import sqlalchemy as sa


class LONGRAW(LargeBinary):
    __visit_name__ = "LONGRAW"


class SDO_GEOMETRY(UserDefinedType):
    cache_ok = True
    __visit_name__ = "SDO_GEOMETRY"

    def __init__(self, geometry_type: Optional[str] = None, srid: Optional[int] = None):
        self.geometry_type = geometry_type
        self.srid = srid

    def get_col_spec(self, **kw: Any) -> str:
        return "SDO_GEOMETRY"

    def column_expression(self, col: Column) -> ColumnElement:
        wkt_expr = sa.func.SDO_UTIL.TO_WKTGEOMETRY(col)
        return sa.func.NVL(wkt_expr, None)

    def result_processor(self, dialect: Any, coltype: Any) -> Optional[Callable]:
        def process(value: Optional[Any]) -> Optional[Any]:
            if value is None:
                return None

            # Handle CLOB/LOB objects from cx_Oracle/python-oracledb
            # LOB objects have a .read() method to extract the string content
            if hasattr(value, "read"):
                try:
                    value = value.read()
                except Exception as e:
                    import warnings

                    warnings.warn(f"Failed to read CLOB/LOB for SDO_GEOMETRY: {e}")
                    return None

            if isinstance(value, str):
                try:
                    from shapely import from_wkt

                    return from_wkt(value)
                except Exception as e:
                    import warnings

                    warnings.warn(f"Failed to parse WKT geometry: {e}")
                    return None

            # Unexpected type - log warning and return None
            import warnings

            warnings.warn(f"Expected WKT string or LOB for SDO_GEOMETRY but got {type(value).__name__}")
            return None

        return process


def flip_geometry_oracle(column: Column) -> ColumnElement:
    return sa.func.SDO_UTIL.REVERSE_LINESTRING(column)


if "LONG RAW" not in base.ischema_names:
    base.ischema_names["LONG RAW"] = LONGRAW

if not hasattr(base.OracleTypeCompiler, "visit_LONGRAW"):

    def visit_LONGRAW(self, type_: LONGRAW, **kw: Any) -> str:
        return "LONG RAW"

    base.OracleTypeCompiler.visit_LONGRAW = visit_LONGRAW


if "SDO_GEOMETRY" not in base.ischema_names:
    base.ischema_names["SDO_GEOMETRY"] = SDO_GEOMETRY

if not hasattr(base.OracleTypeCompiler, "visit_SDO_GEOMETRY"):

    def visit_SDO_GEOMETRY(self, type_: SDO_GEOMETRY, **kw: Any) -> str:
        return "SDO_GEOMETRY"

    base.OracleTypeCompiler.visit_SDO_GEOMETRY = visit_SDO_GEOMETRY
