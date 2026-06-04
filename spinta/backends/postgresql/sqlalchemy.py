import sqlalchemy as sa

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import FunctionElement
from sqlalchemy.dialects.mssql.base import ischema_names
from geoalchemy2.types import Geometry


ischema_names["geometry"] = Geometry


class utcnow(FunctionElement):
    inherit_cache = True
    type = sa.DateTime()


@compiles(utcnow, "postgresql")
def pg_utcnow(element, compiler, **kw):
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"


def create_postgresql_engine(uri: str, connect_args: dict | None = None, **kwargs) -> sa.engine.Engine:
    default_kwargs = {
        "pool_pre_ping": True,
        "pool_recycle": 900,
    }

    default_kwargs.update(kwargs)
    default_connect_args = {
        "keepalives": 1,
        "keepalives_idle": 60,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    }
    if connect_args:
        default_connect_args.update(connect_args)

    return sa.create_engine(uri, connect_args=default_connect_args, **default_kwargs)
