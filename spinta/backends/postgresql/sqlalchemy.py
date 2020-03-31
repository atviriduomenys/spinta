import sqlalchemy as sa

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import FunctionElement


class utcnow(FunctionElement):
    type = sa.DateTime()


@compiles(utcnow, 'postgresql')
def pg_utcnow(element, compiler, **kw):
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"
