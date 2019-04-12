import os
import collections
import pathlib

import pytest
import sqlalchemy_utils as su
from responses import RequestsMock
from toposort import toposort
from starlette.testclient import TestClient

from spinta import api
from spinta.components import Context, Config, Store
from spinta.commands import load, check, prepare, migrate, wipe
from spinta.utils.commands import load_commands
from spinta.config import CONFIG


@pytest.fixture
def context(postgresql):
    context = Context()
    config = Config()
    store = Store()

    context.set('config', config)
    context.set('store', store)

    load_commands([
        'spinta.types',
        'spinta.backends',
    ])

    load(context, config, {
        **CONFIG,
        'backends': {
            'default': {
                'backend': 'spinta.backends.postgresql:PostgreSQL',
                'dsn': postgresql,
            },
        },
        'manifests': {
            'default': {
                'backend': 'default',
                'path': pathlib.Path(__file__).parent / 'manifest',
            },
        },
        'ignore': [],
    })

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
        graph[model.name] = set()
        for prop in model.properties.values():
            if prop.type == 'ref':
                graph[prop.object].add(model.name)

    with context.enter():
        context.bind('transaction', store.backends['default'].transaction)
        for models in toposort(graph):
            for name in models:
                if name:
                    model = store.manifests['default'].objects['model'][name]
                    wipe(context, model, model.backend)

        for dataset in store.manifests['default'].objects['dataset'].values():
            for model in dataset.objects.values():
                wipe(context, model, model.backend)

        model = store.internal.objects['model']['transaction']
        wipe(context, model, model.backend)


@pytest.fixture(scope='session')
def postgresql():
    if 'SPINTA_TEST_DATABASE' in os.environ:
        yield os.environ['SPINTA_TEST_DATABASE']
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
