import os
import pathlib

import pytest
import sqlalchemy_utils as su
from responses import RequestsMock
from click.testing import CliRunner

from spinta import api
from spinta.testing.context import create_test_context
from spinta.testing.client import TestClient
from spinta.config import RawConfig


@pytest.fixture(scope='session')
def spinta_test_config():
    return {
        'backends': {
            'default': {
                'backend': 'spinta.backends.postgresql:PostgreSQL',
                'dsn': 'postgresql://admin:admin123@localhost:54321/spinta_tests',
            },
            'mongo': {
                'backend': 'spinta.backends.mongo:Mongo',
                'dsn': 'mongodb://admin:admin123@localhost:27017/',
                'db': 'spinta_tests',
            },
            'fs': {
                'backend': 'spinta.backends.fs:FileSystem',
                'path': pathlib.Path() / 'var/files',
            },
        },
    }


@pytest.fixture(scope='session')
def _config(spinta_test_config):
    config = RawConfig()
    config.read(
        hardset={
            'env': 'test',
        },
        config={
            'environments': {
                'test': spinta_test_config,
            }
        },
    )
    return config


@pytest.fixture()
def config(_config, request):
    request.addfinalizer(_config.restore)
    return _config


@pytest.fixture(scope='session')
def postgresql(_config):
    dsn = _config.get('backends', 'default', 'dsn', required=True)
    if su.database_exists(dsn):
        yield dsn
    else:
        su.create_database(dsn)
        yield dsn
        su.drop_database(dsn)


@pytest.fixture(scope='session')
def mongo(_config):
    yield
    dsn = _config.get('backends', 'mongo', 'dsn', required=False)
    db = _config.get('backends', 'mongo', 'db', required=False)
    if dsn and db:
        import pymongo
        client = pymongo.MongoClient(dsn)
        client.drop_database(db)


@pytest.fixture(scope='session')
def _context(_config, postgresql, mongo):
    context = create_test_context(_config)
    context.load()
    yield context


@pytest.fixture
def context(_context, mocker, tmpdir, request):
    mocker.patch.dict(os.environ, {
        'AUTHLIB_INSECURE_TRANSPORT': '1',
    })

    with _context.fork('test') as context:
        mocker.patch('spinta.config._create_context', return_value=context)

        store = context.get('store')
        if 'fs' in store.backends:
            mocker.patch.object(store.backends['fs'], 'path', pathlib.Path(tmpdir))

        yield context

        # At this point, transaction must be closed, if it is not, then something is
        # wrong. Find out why transaction was not property closed.
        assert context.has('transaction') is False

        # If context was not loaded, then it means, that database was not touched.
        # All database operations require fully loaded context.
        if context.loaded:
            # XXX: Maybe instead of deleting everythin, we could rollback
            #      transactions, once this kind of functionality will be
            #      available? This should be more efficient.
            store = context.get('store')
            context.wipe_all()


@pytest.fixture
def responses():
    with RequestsMock() as mock:
        yield mock


@pytest.fixture
def app(context, mocker):
    mocker.patch('spinta.api._load_context')
    context.attach('client', TestClient, context, api.app)
    return context.get('client')


@pytest.fixture
def cli(context, mocker):
    mocker.patch('spinta.cli._load_context')
    runner = CliRunner()
    return runner


def pytest_addoption(parser):
    parser.addoption(
        "--model",
        action="append",
        default=[],
        help="run tests only for particular model ['postgres', 'mongo', 'postgres/datasets']",
    )


def pytest_configure(config):
    # https://docs.pytest.org/en/latest/mark.html#registering-marks
    config.addinivalue_line(
        "markers", "models(*models): mark test to run multiple times with each model specified"
    )


def pytest_generate_tests(metafunc):
    # Get model markers from test, if markers are set - leave test as is
    models = metafunc.definition.get_closest_marker('models')
    if not models:
        return

    # If there are markers, get them, together with model CLI options
    models = set(models.args)
    model_cli_options = set(metafunc.config.getoption('model'))

    # If model CLI options are not empty
    # then get common markers from test and CLI options
    if model_cli_options:
        models = models.intersection(model_cli_options)

    # Parametrize our test with calculated models.
    # If we pass to CLI model option, which does not have a test marker,
    # then pytest will skip the test all together.
    metafunc.parametrize('model', models)
