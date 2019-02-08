import pytest
import sqlalchemy_utils as su


@pytest.fixture(scope="session")
def postgresql():
    dsn = 'postgresql:///spinta_tests'
    assert not su.database_exists(dsn), 'Test database already exists. Aborting tests.'
    su.create_database(dsn)
    yield dsn
    su.drop_database(dsn)
