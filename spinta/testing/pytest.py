import collections
import os

import pytest
import sqlalchemy_utils as su
from responses import RequestsMock
from toposort import toposort
from starlette.testclient import TestClient

from spinta import api
from spinta.components import Config, Store
from spinta.commands import load, check, prepare, migrate
from spinta.utils.commands import load_commands
from spinta.testing.context import ContextForTests


@pytest.fixture
def context(config, postgresql):
    context = ContextForTests()

    context.set('config', Config())
    store = context.set('store', Store())

    load_commands(config.get('commands', 'modules', cast=list))
    load(context, context.get('config'), config)
    load(context, store, config)
    check(context, store)
    prepare(context, store.internal)
    migrate(context, store)
    prepare(context, store)
    migrate(context, store)

    yield context

    # Remove all data after each test run.
    graph = collections.defaultdict(set)
    for model in store.manifests['default'].objects['model'].values():
        if model.name not in graph:
            graph[model.name] = set()
        for prop in model.properties.values():
            if prop.type.name == 'ref':
                graph[prop.type.object].add(model.name)

    for models in toposort(graph):
        for name in models:
            context.wipe(name)

    # Datasets does not have foreign kei constraints, so there is no need to
    # topologically sort them. At least for now.
    for dataset in store.manifests['default'].objects['dataset'].values():
        for model in dataset.objects.values():
            context.wipe(model)

    context.wipe(store.internal.objects['model']['transaction'])


@pytest.fixture(scope='session')
def postgresql():
    if 'SPINTA_TEST_DATABASE' in os.environ:
        yield os.environ['SPINTA_TEST_DATABASE']
    else:
        if 'SPINTA_TEST_DATABASE_DSN' in os.environ:
            dsn = os.environ['SPINTA_TEST_DATABASE_DSN']
        else:
            dsn = 'postgresql:///spinta_tests'
        assert not su.database_exists(dsn), 'Test database already exists. Aborting tests.'
        su.create_database(dsn)
        yield dsn
        su.drop_database(dsn)


@pytest.fixture
def responses():
    with RequestsMock() as mock:
        yield mock


@pytest.fixture
def app(context, mocker):
    mocker.patch('spinta.api.context', context)
    return TestClient(api.app)
