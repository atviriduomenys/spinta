import sqlalchemy.exc

from spinta import commands
from spinta.backends.constants import WAIT_CONNECT_TIMEOUT
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.sqlalchemy import create_postgresql_engine
from spinta.components import Context


@commands.wait.register(Context, PostgreSQL)
def wait(context: Context, backend: PostgreSQL, *, fail: bool = False) -> bool:
    rc = context.get("rc")
    dsn = rc.get("backends", backend.name, "dsn", required=True)

    # `libpq` treats `connect_timeout=0` as "wait indefinitely", which is what
    # waiting for backends to come up wants: a host that is not answering yet
    # gets as long as the caller is willing to wait. Callers that need an answer
    # in bounded time, such as the `/health` probe, ask for a timeout instead.
    timeout = context.get(WAIT_CONNECT_TIMEOUT) if context.has(WAIT_CONNECT_TIMEOUT) else 0

    engine = create_postgresql_engine(dsn, connect_args={"connect_timeout": timeout})
    try:
        conn = engine.connect()
    except sqlalchemy.exc.DBAPIError:
        # `OperationalError` covers an unreachable server, but a driver can
        # reject the connection with other `DBAPIError` subclasses as well, and
        # all of them mean the same thing here: the backend is not usable.
        return False
    else:
        conn.close()
        return True
    finally:
        engine.dispose()
