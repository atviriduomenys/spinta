from unittest.mock import MagicMock

import pytest
import sqlalchemy.exc

from spinta import commands
from spinta.backends.constants import WAIT_CONNECT_TIMEOUT
from spinta.backends.postgresql.components import PostgreSQL
from spinta.components import Context

WAIT = "spinta.backends.postgresql.commands.wait.create_postgresql_engine"


@pytest.fixture
def backend() -> PostgreSQL:
    backend = PostgreSQL()
    backend.name = "default"
    return backend


def _engine(error: Exception | None = None) -> MagicMock:
    engine = MagicMock()
    if error is not None:
        engine.connect.side_effect = error
    return engine


def _dbapi_error(orig: Exception) -> sqlalchemy.exc.DBAPIError:
    return sqlalchemy.exc.DBAPIError("SELECT 1", {}, orig)


def test_wait(context: Context, backend: PostgreSQL, mocker):
    engine = _engine()
    mocker.patch(WAIT, return_value=engine)

    assert commands.wait(context, backend) is True
    engine.connect.return_value.close.assert_called_once()
    engine.dispose.assert_called_once()


def test_wait_without_timeout_lets_the_driver_wait(context: Context, backend: PostgreSQL, mocker):
    # `libpq` reads 0 as "wait indefinitely", which is what waiting for a backend
    # to come up wants: it gets as long as the caller is willing to wait.
    create_engine = mocker.patch(WAIT, return_value=_engine())

    commands.wait(context, backend)

    assert create_engine.call_args.kwargs["connect_args"] == {"connect_timeout": 0}


def test_wait_bounds_the_driver_when_asked(context: Context, backend: PostgreSQL, mocker):
    create_engine = mocker.patch(WAIT, return_value=_engine())

    with context.fork("health") as fork:
        fork.set(WAIT_CONNECT_TIMEOUT, 3)
        commands.wait(fork, backend)

    assert create_engine.call_args.kwargs["connect_args"] == {"connect_timeout": 3}


@pytest.mark.parametrize(
    "error",
    [
        sqlalchemy.exc.OperationalError("SELECT 1", {}, Exception("unreachable")),
        # Not every driver rejects a connection with an `OperationalError`, but
        # every rejection means the same thing here: the backend is not usable.
        _dbapi_error(Exception("driver rejected the connection")),
    ],
)
def test_wait_unavailable(context: Context, backend: PostgreSQL, error: Exception, mocker):
    engine = _engine(error)
    mocker.patch(WAIT, return_value=engine)

    assert commands.wait(context, backend) is False
    # The engine must be disposed on the failure path too, otherwise every failed
    # check leaks it together with its connection pool.
    engine.dispose.assert_called_once()
