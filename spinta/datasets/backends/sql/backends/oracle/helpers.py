from sqlalchemy.sql.sqltypes import LargeBinary
from sqlalchemy.dialects.oracle import base


class LONGRAW(LargeBinary):
    __visit_name__ = "LONGRAW"


if "LONG RAW" not in base.ischema_names:
    base.ischema_names["LONG RAW"] = LONGRAW

if not hasattr(base.OracleTypeCompiler, "visit_LONGRAW"):

    def visit_LONGRAW(self, type_, **kw):
        return "LONG RAW"

    base.OracleTypeCompiler.visit_LONGRAW = visit_LONGRAW
