import os
import pathlib
import tempfile
from typing import Any

import boto3
import moto
import pytest
import sqlalchemy_utils as su
from responses import RequestsMock

from spinta.core.config import RawConfig
from spinta.core.config import read_config
from spinta.manifests.components import Manifest
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.client import create_test_client
from spinta.testing.context import ContextForTests
from spinta.testing.context import create_test_context
from spinta.testing.datasets import Sqlite
from spinta.testing.manifest import compare_manifest


@pytest.fixture(scope='session')
def rc():
    with tempfile.TemporaryDirectory() as tmpdir:
        rc = read_config()
        rc.add('pytest', {
            'env': 'test',
            'keymaps.default': {
                'type': 'sqlalchemy',
                'dsn': 'sqlite:////' + os.path.join(tmpdir, 'keymaps.db'),
            },
        })
        rc.lock()
        yield rc


@pytest.fixture()
def sqlite():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Sqlite('sqlite:///' + os.path.join(tmpdir, 'db.sqlite'))


@pytest.fixture(scope='session')
def postgresql(rc):
    dsn = rc.get('backends', 'default', 'dsn', required=True)
    if su.database_exists(dsn):
        yield dsn
    else:
        su.create_database(dsn)
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
def s3(rc):
    with moto.mock_s3():
        yield
        bucket_name = rc.get('backends', 's3', 'bucket', required=False)
        if bucket_name:
            s3 = boto3.resource('s3')
            s3_client = boto3.client('s3')
            try:
                objs = s3_client.list_objects(Bucket=bucket_name)
                contents = objs.get('Contents')
                if contents:
                    obj_keys = {'Objects': [
                        {'Key': obj['Key'] for obj in contents}
                    ]}
                    bucket = s3.Bucket(bucket_name)
                    bucket.delete_objects(Delete=obj_keys)
                    bucket.delete()
            except s3_client.exceptions.NoSuchBucket:
                pass


@pytest.fixture(scope='session')
def backends(postgresql, mongo, s3):
    yield {
        'postgresql': postgresql,
        'mongo': mongo,
        's3': s3,
    }


@pytest.fixture(scope='session')
def _context(rc: RawConfig, postgresql, mongo, s3):
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
            # XXX: Maybe instead of deleting everythin, we could rollback
            #      transactions, once this kind of functionality will be
            #      available? This should be more efficient.
            context.wipe_all()


@pytest.fixture
def responses():
    with RequestsMock() as mock:
        yield mock


@pytest.fixture
def app(context):
    context.attach('client', create_test_client, context)
    return context.get('client')


@pytest.fixture
def cli():
    return SpintaCliRunner(mix_stderr=False)


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


@pytest.hookimpl(tryfirst=True)
def pytest_assertrepr_compare(op: str, left: Any, right: Any):
    if op == '==' and isinstance(left, Manifest) and isinstance(right, str):
        left, right = compare_manifest(left, right)
        return [f'{left!r} {op} {right!r}']
