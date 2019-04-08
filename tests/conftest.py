import os
import collections
import pathlib

import pytest
import sqlalchemy_utils as su
import sqlalchemy.exc
from responses import RequestsMock
from toposort import toposort
from starlette.testclient import TestClient

from spinta import api
from spinta.store import Store


@pytest.fixture
def store(postgresql):
    config = {
        'backends': {
            'default': {
                'type': 'postgresql',
                'dsn': postgresql,
            },
        },
        'manifests': {
            'default': {
                'path': pathlib.Path(__file__).parent / 'manifest',
            },
        },
        'ignore': [],
    }

    store = Store()
    store.add_types()
    store.add_commands()
    store.configure(config)
    store.prepare(internal=True)
    store.migrate(internal=True)
    store.prepare()
    store.migrate()

    yield store

    # Remove all data after each test run.
    graph = collections.defaultdict(set)
    for model in store.objects['default']['model'].values():
        graph[model.name] = set()
        for prop in model.properties.values():
            if prop.type == 'ref':
                graph[prop.object].add(model.name)
    for models in toposort(graph):
        for model in models:
            if model:
                store.wipe(model)

    for dataset in store.objects['default']['dataset'].values():
        for model in dataset.objects.values():
            store.wipe(f'{model.name}/:source/{dataset.name}')

    store.wipe('transaction', ns='internal')


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
def app(store, mocker):
    mocker.patch('spinta.api.store', store)
    return TestClient(api.app)
