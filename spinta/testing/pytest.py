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
def context(mocker, tmpdir, config, postgresql, mongo):
    mocker.patch.dict(os.environ, {
        'AUTHLIB_INSECURE_TRANSPORT': '1',
        'SPINTA_BACKENDS_FS_PATH': str(tmpdir),
    })

    Context = config.get('components', 'core', 'context', cast=importstr)
    Context = type('ContextForTests', (ContextForTests, Context), {})
    context = Context()
    context.set('config.raw', config)
    mocker.patch('spinta.config._create_context', return_value=context)

    yield context

    config.restore()
    if context.loaded:
        context.wipe_all()


@pytest.fixture
def responses():
    with RequestsMock() as mock:
        yield mock


@pytest.fixture
def app(context, mocker):
    mocker.patch('spinta.api.context', context)
    return TestClient(context, api.app)


@pytest.fixture
def cli(context, mocker):
    def _load_context(context, rc):
        if not context.loaded:
            context.load()
    mocker.patch('spinta.cli._load_context', _load_context)
    runner = CliRunner()
    return runner


def pytest_addoption(parser):
    parser.addoption(
        "--backend",
        action="append",
        default=[],
        help="run tests only for particular database backend ['postgres', 'mongo']",
    )


def pytest_generate_tests(metafunc):
    # Get backend markers from test, if markers are set - leave test as is
    backends = metafunc.definition.get_closest_marker('backends')
    if not backends:
        return

    # If there are markers, get them, together with backend CLI options
    backends = set(backends.args)
    backend_cli_options = set(metafunc.config.getoption("backend"))

    # If backend CLI options are not empty
    # then get common markers from test and CLI options
    if backend_cli_options:
        backends = backends.intersection(backend_cli_options)

    # Parametrize our test with calculated backends.
    # If we pass to CLI backend option, which does not have a test marker,
    # then pytest will skip the test all together.
    metafunc.parametrize('backend', backends)
