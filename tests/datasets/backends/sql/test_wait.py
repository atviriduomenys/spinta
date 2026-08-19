import pytest
import sqlalchemy as sa

from spinta.datasets.backends.sql.commands.wait import get_connect_args

TIMEOUT = 5


@pytest.mark.parametrize(
    "dsn, expected",
    [
        ("postgresql://u:p@host/db", {"connect_timeout": TIMEOUT}),
        ("postgresql+psycopg2://u:p@host/db", {"connect_timeout": TIMEOUT}),
        ("mysql+pymysql://u:p@host/db", {"connect_timeout": TIMEOUT}),
        ("mariadb://u:p@host/db", {"connect_timeout": TIMEOUT}),
        ("mssql+pyodbc://u:p@host/db", {"timeout": TIMEOUT}),
        ("oracle+oracledb://u:p@host/db", {"tcp_connect_timeout": TIMEOUT}),
        # Drivers that do not take a connect timeout must be left alone, an
        # unsupported argument would fail the connection instead of bounding it.
        ("oracle+cx_oracle://u:p@host/db", {}),
        ("sqlite:///db.sqlite", {}),
        ("sas+sas://host", {}),
        ("this is not a dsn", {}),
    ],
)
def test_get_connect_args(dsn: str, expected: dict):
    assert get_connect_args(dsn, TIMEOUT) == expected


@pytest.mark.parametrize("timeout", [None, 0])
def test_get_connect_args_without_timeout(timeout):
    # Waiting for backends to come up keeps the driver defaults, so that a
    # backend that is still starting gets as long as the caller allows.
    assert get_connect_args("postgresql://u:p@host/db", timeout) == {}


def test_get_connect_args_are_accepted_by_sqlite():
    # Guards against bounding a driver that does not take the argument.
    dsn = "sqlite://"
    engine = sa.create_engine(dsn, connect_args=get_connect_args(dsn, TIMEOUT))
    try:
        engine.connect().close()
    finally:
        engine.dispose()
