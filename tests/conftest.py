import pytest
import sqlalchemy_utils as su
from responses import RequestsMock


@pytest.fixture()
def postgresql():
    dsn = 'postgresql:///spinta_tests'
    assert not su.database_exists(dsn), 'Test database already exists. Aborting tests.'
    su.create_database(dsn)
    yield dsn
    su.drop_database(dsn)


@pytest.fixture
def responses():
    with RequestsMock() as mock:
        yield mock
