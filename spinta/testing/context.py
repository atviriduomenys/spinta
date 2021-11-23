from __future__ import annotations

from typing import Any
from typing import Dict
from typing import Optional
from typing import Union

import contextlib

from pytest import FixtureRequest

from spinta import commands
from spinta.cli.helpers.store import prepare_manifest
from spinta.components import Node
from spinta.components import Context
from spinta.auth import AdminToken
from spinta.utils.imports import importstr
from spinta.core.context import create_context
from spinta.core.config import RawConfig


def create_test_context(
    rc: RawConfig,
    request: FixtureRequest = None,
    *,
    name: str = 'pytest',
) -> TestContext:
    rc = rc.fork()
    Context_ = rc.get('components', 'core', 'context', cast=importstr)
    Context_ = type('ContextForTests', (ContextForTests, Context_), {})
    context = Context_(name)
    context = create_context(name, rc, context)
    if request:
        request.addfinalizer(context.wipe_all)
    return context


class ContextForTests:
    loaded: bool = False

    def __init__(
        self: TestContext,
        name: str,
        parent: TestContext = None,
    ):
        super().__init__(name, parent)
        self.loaded = parent.loaded if parent else False

    @contextlib.contextmanager
    def transaction(self: TestContext, *, write=False):
        if self.has('transaction'):
            yield self
        else:
            with self:
                store = self.get('store')
                self.set('auth.token', AdminToken())
                backend = store.manifest.backend
                self.attach('transaction', backend.transaction, write=write)
                yield self

    def wipe(self: TestContext, model: Union[str, Node]):
        if isinstance(model, str):
            store = self.get('store')
            model = store.manifest.models[model]
        with self.transaction() as context:
            commands.wipe(context, model, model.backend)

    def wipe_all(self: TestContext):
        store = self.get('store')
        self.wipe(store.manifest.objects['ns'][''])

    def load(
        self: TestContext,
        overrides: Optional[Dict[str, Any]] = None,
    ) -> TestContext:
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

        rc: RawConfig = self.get('rc')

        if overrides:
            rc.add('test', {
                'environments': {
                    'test': overrides,
                }
            })

        store = prepare_manifest(self)
        commands.bootstrap(self, store.manifest)

        self.loaded = True

        return self


TestContext = Union[Context, ContextForTests]
