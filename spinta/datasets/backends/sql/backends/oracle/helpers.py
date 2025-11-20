from sqlalchemy.sql.sqltypes import LargeBinary
from sqlalchemy.dialects.oracle import base


class LONGRAW(LargeBinary):
    __visit_name__ = "LONGRAW"


if "LONG RAW" not in base.ischema_names:
    base.ischema_names["LONG RAW"] = LONGRAW
