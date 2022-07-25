import os
import pathlib
import tempfile
from difflib import Differ
from typing import Any, Iterator

import pprintpp
import pytest
import sqlalchemy as sa
import sqlalchemy_utils as su
from responses import RequestsMock

from spinta.core.config import RawConfig
from spinta.core.config import read_config
from spinta.manifests.components import Manifest
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.client import TestClient
from spinta.testing.client import create_test_client
from spinta.testing.context import ContextForTests
from spinta.testing.context import create_test_context
from spinta.testing.datasets import Sqlite
from spinta.testing.manifest import compare_manifest


def _remove_push_state(rc: RawConfig) -> None:
    data_dir = rc.get('data_path')
    for file in (data_dir / 'push').glob('*.db'):
        file.unlink()


@pytest.fixture(scope='session')
def rc():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = pathlib.Path(tmpdir)
        data_dir = tmpdir / '.local/share'
        rc = read_config()
        rc.add('pytest', {
            'env': 'test',
            'data_path': data_dir,
            'keymaps.default': {
                'type': 'sqlalchemy',
                'dsn': 'sqlite:///{data_dir}/keymap.db',
            },
        })
        rc.lock()
        yield rc
        _remove_push_state(rc)


@pytest.fixture()
def sqlite():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Sqlite('sqlite:///' + os.path.join(tmpdir, 'db.sqlite'))


def _prepare_postgresql(dsn: str) -> None:
    engine = sa.create_engine(dsn)
    with engine.connect() as conn:
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS postgis"))
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS postgis_topology"))
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch"))
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder"))


@pytest.fixture(scope='session')
def postgresql(rc) -> str:
    dsn: str = rc.get('backends', 'default', 'dsn', required=True)
    if su.database_exists(dsn):
        _prepare_postgresql(dsn)
        yield dsn
    else:
        su.create_database(dsn)
        _prepare_postgresql(dsn)
        yield dsn
        su.drop_database(dsn)


@pytest.fixture(scope='session')
def mongo(rc):
    yield
    dsn = rc.get('backends', 'mongo', 'dsn', required=False)
    db = rc.get('backends', 'mongo', 'db', required=False)
    if dsn and db:
        import pymongo
        client = pymongo.MongoClient(dsn)
        client.drop_database(db)


@pytest.fixture(scope='session')
def backends(postgresql, mongo):
    yield {
        'postgresql': postgresql,
        'mongo': mongo,
    }


@pytest.fixture(scope='session')
def _context(rc: RawConfig, postgresql, mongo):
    context: ContextForTests = create_test_context(rc)
    context.load()
    yield context


@pytest.fixture
def context(_context, mocker, tmpdir, request):
    with _context.fork('test') as context:
        store = context.get('store')
        if 'fs' in store.backends:
            # XXX: There must be a better way to provide tmpdir to fs backend.
            mocker.patch.object(store.backends['fs'], 'path', pathlib.Path(tmpdir))

        # In-memory accesslog used with spinta.accesslog.python.
        context.set('accesslog.stream', [])

        yield context

        # At this point, transaction must be closed, if it is not, then something is
        # wrong. Find out why transaction was not property closed.
        assert context.has('transaction') is False

        # If context was not loaded, then it means, that database was not touched.
        # All database operations require fully loaded context.
        if context.loaded:
            # XXX: Maybe instead of deleting everything, we could rollback
            #      transactions, once this kind of functionality will be
            #      available? This should be more efficient.
            context.wipe_all()


@pytest.fixture
def responses() -> Iterator[RequestsMock]:
    with RequestsMock() as mock:
        yield mock


@pytest.fixture
def app(context) -> TestClient:
    context.attach('client', create_test_client, context)
    return context.get('client')


@pytest.fixture
def cli(rc: RawConfig):
    yield SpintaCliRunner(mix_stderr=False)
    _remove_push_state(rc)


def pytest_addoption(parser):
    # TODO: Switch back to backend pytest param.
    #       We want to tests multiple backends and one backend can use
    #       multiple models.
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


def _diff_line(line: str) -> str:
    if line.startswith('- '):
        return '< ' + line[2:]
    if line.startswith('+ '):
        return '> ' + line[2:]
    return line


@pytest.hookimpl(tryfirst=True)
def pytest_assertrepr_compare(op: str, left: Any, right: Any):
    if op == '==' and isinstance(left, Manifest) and isinstance(right, str):
        left, right = compare_manifest(left, right)
        return ['not equal'] + [
            _diff_line(line)
            for line in Differ().compare(
                left.splitlines(),
                right.splitlines(),
            )
        ]
    types = (dict, list)
    if op == '==' and isinstance(left, types) and isinstance(right, types):
        left = pprintpp.pformat(left, indent=2, width=40).splitlines()
        right = pprintpp.pformat(right, indent=2, width=40).splitlines()
        return ['not equal'] + [
            _diff_line(line)
            for line in Differ().compare(left, right)
        ]
