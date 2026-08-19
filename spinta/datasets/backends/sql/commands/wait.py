import sqlalchemy as sa
import sqlalchemy.exc

from spinta import commands
from spinta.backends.constants import WAIT_CONNECT_TIMEOUT
from spinta.components import Context
from spinta.datasets.backends.sql.components import Sql

# How each driver names its connect timeout argument. Only drivers known to
# accept one can be bounded, everything else keeps the driver default, because
# an unsupported argument would fail the connection instead of speeding it up.
# `sqlite` is absent on purpose: it opens a local file, and its `timeout`
# argument means how long to wait for a lock, which is something else entirely.
CONNECT_TIMEOUT_ARGS = {
    "psycopg2": "connect_timeout",
    "psycopg": "connect_timeout",
    "pymysql": "connect_timeout",
    "mysqldb": "connect_timeout",
    "pyodbc": "timeout",
    "oracledb": "tcp_connect_timeout",
}


def get_connect_args(dsn: str, timeout: float | None) -> dict:
    """Connect arguments bounding how long the driver of `dsn` waits.

    Without a timeout the driver keeps its own default, which for `psycopg2` is
    to wait indefinitely. That is what waiting for backends to come up wants;
    callers that need an answer in bounded time pass a timeout.
    """
    if not timeout:
        return {}

    try:
        driver = sa.engine.make_url(dsn).get_driver_name()
    except sa.exc.ArgumentError:
        # Let `create_engine` report what is wrong with the DSN.
        return {}

    if name := CONNECT_TIMEOUT_ARGS.get(driver):
        return {name: timeout}
    return {}


@commands.wait.register(Context, Sql)
def wait(context: Context, backend: Sql, *, fail: bool = False) -> bool:
    rc = context.get("rc")
    dsn = rc.get("backends", backend.name, "dsn", default=None)
    if dsn is None:
        return True

    timeout = context.get(WAIT_CONNECT_TIMEOUT) if context.has(WAIT_CONNECT_TIMEOUT) else None
    engine = sa.create_engine(dsn, connect_args=get_connect_args(dsn, timeout))
    try:
        conn = engine.connect()
    except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.DBAPIError):
        if fail:
            raise
        else:
            return False
    else:
        conn.close()
        return True
    finally:
        engine.dispose()
