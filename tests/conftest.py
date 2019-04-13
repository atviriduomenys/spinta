import collections
import contextlib
import os
import pathlib
import typing

import pytest
import sqlalchemy_utils as su
from responses import RequestsMock
from toposort import toposort
from starlette.testclient import TestClient

from spinta import api
from spinta.components import Context, Config, Store, Node
from spinta.commands import load, check, prepare, migrate, wipe
from spinta.utils.commands import load_commands
from spinta.config import CONFIG
from spinta import commands


class ContextForTests(Context):

    def _get_model(self, model: typing.Union[str, Node], dataset: str):
        if isinstance(model, str):
            store = self.get('store')
            if dataset:
                return store.manifests['default'].objects['dataset'][dataset].objects[model]
            else:
                return store.manifests['default'].objects['model'][model]
        else:
            return model

    @contextlib.contextmanager
    def transaction(self, *, write=False):
        store = self.get('store')
        if self.has('transaction'):
            yield self.get('transaction')
        else:
            with self.enter():
                self.bind('transaction', store.backends['default'].transaction, write=write)
                yield self.get('transaction')

    def pull(self, dataset: str, *, models: list = None, push: bool = True):
        store = self.get('store')
        dataset = store.manifests['default'].objects['dataset'][dataset]
        models = models or []
        try:
            with self.transaction(write=push):
                data = commands.pull(self, dataset, models=models)
                if push:
                    yield from commands.push(self, store, data)
                else:
                    yield from data
        except Exception:
            raise Exception(f"Error while processing '{dataset.path}'.")

    def push(self, data):
        store = self.get('store')
        with self.transaction(write=True):
            yield from commands.push(self, store, data)

    def getone(self, model: str, id, *, dataset: str = None):
        model = self._get_model(model, dataset)
        with self.transaction():
            return commands.get(self, model, model.backend, id)

    def getall(self, model: str, *, dataset: str = None):
        model = self._get_model(model, dataset)
        with self.transaction():
            return list(commands.getall(self, model, model.backend))

    def wipe(self, model: str, *, dataset: str = None):
        model = self._get_model(model, dataset)
        with self.transaction():
            wipe(self, model, model.backend)


@pytest.fixture
def context(postgresql):
    context = ContextForTests()
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
            if prop.type.name == 'ref':
                graph[prop.object].add(model.name)

    for models in toposort(graph):
        for name in models:
            if name:
                context.wipe(model)

    for dataset in store.manifests['default'].objects['dataset'].values():
        for model in dataset.objects.values():
            context.wipe(model)

    context.wipe(store.internal.objects['model']['transaction'])


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
