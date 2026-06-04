import sqlalchemy.exc

from spinta import commands
from spinta.components import Context
from spinta.backends.postgresql.components import PostgreSQL
from spinta.utils.sqlalchemy import create_configured_engine


@commands.wait.register(Context, PostgreSQL)
def wait(context: Context, backend: PostgreSQL, *, fail: bool = False) -> bool:
    rc = context.get("rc")
    dsn = rc.get("backends", backend.name, "dsn", required=True)
    engine = create_configured_engine(dsn, connect_args={"connect_timeout": 0})
    try:
        conn = engine.connect()
    except sqlalchemy.exc.OperationalError:
        return False
    else:
        conn.close()
        engine.dispose()
        return True
