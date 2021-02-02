import sqlalchemy as sa
import sqlalchemy.exc

from spinta import commands
from spinta.components import Context
from spinta.datasets.backends.sql.components import Sql


@commands.wait.register(Context, Sql)
def wait(context: Context, backend: Sql, *, fail: bool = False) -> bool:
    rc = context.get('rc')
    dsn = rc.get('backends', backend.name, 'dsn', default=None)
    if dsn is None:
        return True
    engine = sa.create_engine(dsn)
    try:
        conn = engine.connect()
    except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.DBAPIError):
        if fail:
            raise
        else:
            return False
    else:
        conn.close()
        engine.dispose()
        return True
