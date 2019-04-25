import collections

import pymongo
import pytest
import sqlalchemy_utils as su
from responses import RequestsMock
from toposort import toposort
from starlette.testclient import TestClient

from spinta import api
from spinta.components import Store
from spinta.commands import load, check, prepare, migrate
from spinta.utils.commands import load_commands
from spinta.testing.context import ContextForTests
from spinta import components
from spinta.config import Config


@pytest.fixture(scope='session')
def config():
    config = Config()
    config.read({
        'env': 'test',
    })
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
    dsn = config.get('backends', 'mongo', 'dsn', required=True)
    db = config.get('backends', 'mongo', 'db', required=True)
    client = pymongo.MongoClient(dsn)
    client.drop_database(db)


@pytest.fixture
def context(config, postgresql, mongo):
    context = ContextForTests()

    context.set('config', components.Config())
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


@pytest.fixture
def responses():
    with RequestsMock() as mock:
        yield mock


@pytest.fixture
def app(context, mocker):
    mocker.patch('spinta.api.context', context)
    return TestClient(api.app)
