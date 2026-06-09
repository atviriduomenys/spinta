import sqlalchemy.exc

from spinta import commands
from spinta.backends.postgresql.sqlalchemy import create_postgresql_engine
from spinta.components import Context
from spinta.backends.postgresql.components import PostgreSQL


@commands.wait.register(Context, PostgreSQL)
def wait(context: Context, backend: PostgreSQL, *, fail: bool = False) -> bool:
    rc = context.get("rc")
    dsn = rc.get("backends", backend.name, "dsn", required=True)
    engine = create_postgresql_engine(dsn, connect_args={"connect_timeout": 0})
    try:
        conn = engine.connect()
    except sqlalchemy.exc.OperationalError:
        return False
    else:
        conn.close()
        engine.dispose()
        return True
