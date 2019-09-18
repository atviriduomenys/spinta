import os

import pytest
import sqlalchemy_utils as su
from responses import RequestsMock
from click.testing import CliRunner

from spinta import api
from spinta.testing.context import ContextForTests
from spinta.testing.client import TestClient
from spinta.config import RawConfig, load_commands
from spinta.utils.imports import importstr
from spinta import commands


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
            },
        },
    }


@pytest.fixture(scope='session')
def config(spinta_test_config):
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
    load_commands(config.get('commands', 'modules', cast=list))
    return config


@pytest.fixture(scope='session')
def postgresql(config):
    dsn = config.get('backends', 'default', 'dsn', required=True)
    if su.database_exists(dsn):
        yield dsn
    else:
        su.create_database(dsn)
        yield dsn
        su.drop_database(dsn)


@pytest.fixture(scope='session')
def mongo(config):
    yield
    dsn = config.get('backends', 'mongo', 'dsn', required=False)
    db = config.get('backends', 'mongo', 'db', required=False)
    if dsn and db:
        import pymongo
        client = pymongo.MongoClient(dsn)
        client.drop_database(db)


@pytest.fixture
def context(request, mocker, tmpdir, config, postgresql, mongo):
    mocker.patch.dict(os.environ, {
        'AUTHLIB_INSECURE_TRANSPORT': '1',
        'SPINTA_BACKENDS_FS_PATH': str(tmpdir),
    })

    Context = config.get('components', 'core', 'context', cast=importstr)
    Context = type('ContextForTests', (ContextForTests, Context), {})
    context = Context('pytest')
    context.set('config.raw', config)
    mocker.patch('spinta.config._create_context', return_value=context)

    with context:

        yield context

        # At this point, transaction must be closed, if it is not, then something is
        # wrong. Find out why transaction was not property closed.
        assert context.has('transaction') is False

        # If context was not loaded, then it means, that database was not touched.
        # All database operations require fully loaded context.
        if context.loaded:
            context.wipe_all()

        if context.has('store'):
            for backend in context.get('store').backends.values():
                commands.unload_backend(context, backend)

    # In `context.load` if config overrides are provided, config is modified,
    # we need to restore it.
    config.restore()


@pytest.fixture
def responses():
    with RequestsMock() as mock:
        yield mock


@pytest.fixture
def app(context, mocker):
    mocker.patch('spinta.api._load_context', lambda context: context)
    client = TestClient(context, api.app)
    # Attach test client in order to execute startup and shutdown events. These
    # events will be triggered in `context.load()`. Starlette TestClient
    # triggers startup and shutdown events with TestClient.__enter__ and
    # TestClient.__exit__. That is why, we need to attach TestClient to the
    # context and trigger it later, on context load.
    context.attach('client', lambda: client)
    return client


@pytest.fixture
def cli(context, mocker):
    mocker.patch('spinta.cli._load_context', lambda context, rc: context.load_if_not_loaded())
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
