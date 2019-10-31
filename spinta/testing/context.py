import asyncio
import contextlib
import typing
import collections

from toposort import toposort

from spinta import commands
from spinta.components import Node
from spinta.auth import AdminToken
from spinta import components
from spinta.auth import AuthorizationServer, ResourceProtector, BearerTokenValidator
from spinta.utils.imports import importstr
from spinta.config import load_commands
from spinta.utils.aiotools import alist, aiter
from spinta.commands.write import push_stream, dataitem_from_payload


def create_test_context(config, *, name='pytest'):
    Context = config.get('components', 'core', 'context', cast=importstr)
    Context = type('ContextForTests', (ContextForTests, Context), {})
    context = Context(name)
    context.set('config.raw', config)

    load_commands(config.get('commands', 'modules', cast=list))

    return context


class ContextForTests:

    def __init__(self, name, parent=None):
        super().__init__(name, parent)
        self.loaded = parent.loaded if parent else False

    def _get_model(self, model: typing.Union[str, Node], dataset: str, resource: str, origin: str = None):
        if isinstance(model, str):
            store = self.get('store')
            if resource:
                resource = store.manifests['default'].objects['dataset'][dataset].resources[resource]
                origin = resource.get_model_origin(model) if origin is None else origin
                return resource.objects[origin][model]
            else:
                return store.manifests['default'].objects['model'][model]
        else:
            return model

    @contextlib.contextmanager
    def transaction(self, *, write=False):
        if self.has('transaction'):
            yield self
        else:
            with self:
                store = self.get('store')
                self.set('auth.token', AdminToken())
                self.attach('transaction', store.manifests['default'].backend.transaction, write=write)
                yield self

    def pull(self, dataset: str, *, models: list = None, push: bool = True):
        store = self.get('store')
        dataset = store.manifests['default'].objects['dataset'][dataset]
        models = models or []
        with self.transaction(write=push) as context:
            try:
                stream = list(commands.pull(context, dataset, models=models))
            except Exception:
                raise Exception(f"Error while processing '{dataset.path}'.")
            if push:
                stream = push_stream(context, aiter(
                    dataitem_from_payload(context, dataset, data)
                    for data in stream
                ))
                # XXX: I don't like that, pull should be a coroutine.
                stream = asyncio.get_event_loop().run_until_complete(alist(stream))
                stream = [{**(d.saved or {}), **d.patch} for d in stream if d.patch]
        return stream

    def wipe(self, model: str, *, dataset: str = None, resource: str = None):
        model = self._get_model(model, dataset, resource)
        with self.transaction() as context:
            commands.wipe(context, model, model.backend)

    def wipe_all(self):
        store = self.get('store')
        with self.transaction() as context:
            store = context.get('store')

            # Remove all data after each test run.
            graph = collections.defaultdict(set)
            for model in store.manifests['default'].objects['model'].values():
                if model.name not in graph:
                    graph[model.name] = set()
                for prop in model.properties.values():
                    if prop.dtype.name == 'ref':
                        graph[prop.dtype.object].add(model.name)

            for models in toposort(graph):
                for name in models:
                    context.wipe(name)

            # Datasets does not have foreign kei constraints, so there is no need to
            # topologically sort them. At least for now.
            for dataset in store.manifests['default'].objects['dataset'].values():
                for resource in dataset.resources.values():
                    for model in resource.models():
                        context.wipe(model)

            context.wipe(store.internal.objects['model']['transaction'])

    def load(self, overrides=None):
        # We pass context to tests unloaded, by doing this, we give test
        # functions opportunity to call `context.load` manually and provide
        # `overrides` for config, this way each test can configure context in
        # anyway they want.
        #
        # If test function does not explicitly call `context.load`, then it will
        # be called implicitly on `app.request` and on some context methods,
        # that run database queries.

        if self.loaded:
            raise Exception("test context is already loaded")

        config = self.get('config.raw')

        if overrides:
            config.hardset({
                'environments': {
                    'test': overrides,
                }
            })

        self.set('config', components.Config())
        store = self.set('store', components.Store())

        commands.load(self, self.get('config'), config)
        commands.check(self, self.get('config'))
        commands.load(self, store, config)
        commands.check(self, store)

        commands.prepare(self, store.internal)
        commands.migrate(self, store)
        commands.prepare(self, store)
        commands.migrate(self, store)

        self.bind('auth.server', AuthorizationServer, self)
        self.bind('auth.resource_protector', ResourceProtector, self, BearerTokenValidator)

        self.loaded = True

        return self
