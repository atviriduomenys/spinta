from typing import Union

import contextlib

from spinta import commands
from spinta.components import Node
from spinta.auth import AdminToken
from spinta.utils.imports import importstr
from spinta.core.context import create_context
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

    def wipe(self, model: Union[str, Node]):
        if isinstance(model, str):
            store = self.get('store')
            model = store.manifest.models[model]
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
        commands.load(self, config)
        commands.check(self, config)

        store = self.get('store')
        commands.load(self, store)
        commands.load(self, store.manifest)
        commands.link(self, store.manifest)
        commands.check(self, store.manifest)
        commands.wait(self, store)
        commands.prepare(self, store.manifest)
        commands.bootstrap(self, store.manifest)

        self.loaded = True

        return self
