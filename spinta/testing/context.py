import asyncio
import contextlib
import typing

from spinta import commands
from spinta.components import Node
from spinta.auth import AdminToken
from spinta.utils.imports import importstr
from spinta.core.context import create_context
from spinta.utils.aiotools import alist, aiter
from spinta.commands.write import push_stream, dataitem_from_payload
from spinta.core.config import RawConfig


def create_test_context(rc: RawConfig, *, name: str = 'pytest'):
    rc = rc.fork()
    Context = rc.get('components', 'core', 'context', cast=importstr)
    Context = type('ContextForTests', (ContextForTests, Context), {})
    context = Context(name)
    return create_context(name, rc, context)


class ContextForTests:

    def __init__(self, name, parent=None):
        super().__init__(name, parent)
        self.loaded = parent.loaded if parent else False

    def _get_model(self, model: typing.Union[str, Node], dataset: str, resource: str, origin: str = None):
        if isinstance(model, str):
            store = self.get('store')
            if resource:
                manifest = store.manifest
                resource = manifest.objects['dataset'][dataset].resources[resource]
                origin = resource.get_model_origin(model) if origin is None else origin
                return resource.objects[origin][model]
            else:
                return store.manifest.objects['model'][model]
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
                backend = store.manifest.backend
                self.attach('transaction', backend.transaction, write=write)
                yield self

    def pull(self, dataset: str, *, models: list = None, push: bool = True):
        store = self.get('store')
        dataset = store.manifest.objects['dataset'][dataset]
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
        self.wipe(store.manifest.objects['ns'][''])

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

        rc = self.get('rc')

        if overrides:
            rc.add('test', {
                'environments': {
                    'test': overrides,
                }
            })

        config = self.get('config')
        commands.load(self, config, rc)
        commands.check(self, config)

        store = self.get('store')
        commands.load(self, store, config)
        commands.check(self, store)

        commands.prepare(self, store)
        commands.bootstrap(self, store)

        self.loaded = True

        return self
