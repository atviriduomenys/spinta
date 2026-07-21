from __future__ import annotations

import contextlib
import weakref
from typing import Any, Dict, Optional, Union

from pytest import FixtureRequest

from spinta import commands
from spinta.auth import AdminToken
from spinta.backends.helpers import validate_and_return_transaction
from spinta.cli.helpers.store import prepare_manifest
from spinta.components import Context, Node
from spinta.core.config import RawConfig
from spinta.core.context import create_context
from spinta.utils.imports import importstr

# Every short-lived test context is tracked here so that the engines (and their
# connection pools) it created can be disposed after each test, see
# `close_test_context_engines`.
_test_contexts: "weakref.WeakSet[TestContext]" = weakref.WeakSet()


def create_test_context(
    rc: RawConfig, request: FixtureRequest = None, *, name: str = "pytest", wipe_data: bool = True, track: bool = True
) -> TestContext:
    rc = rc.fork()
    Context_ = rc.get("components", "core", "context", cast=importstr)
    Context_ = type("ContextForTests", (ContextForTests, Context_), {})
    context = Context_(name)
    context = create_context(name, rc, context)
    if track:
        _test_contexts.add(context)
    if request and wipe_data:
        request.addfinalizer(context.wipe_all)
    return context


def close_context_engines(context: TestContext) -> None:
    """Dispose SQLAlchemy engines created by a single test context.

    Backends can be owned by the store, declared by the manifest, or attached to
    individual dataset resources, and each can own its own engine. This mirrors
    the runtime backend collection in :func:`spinta.types.store.wait` so that
    engines from every source (plus keymaps) are disposed, not just the ones in
    ``store.backends``.
    """
    if not context.has("store", value=True):
        return
    store = context.get("store")

    seen: set[int] = set()
    engines = []

    def collect(backend) -> None:
        engine = getattr(backend, "engine", None)
        if engine is not None and id(engine) not in seen:
            seen.add(id(engine))
            engines.append(engine)

    for backend in (getattr(store, "backends", None) or {}).values():
        collect(backend)

    manifest = getattr(store, "manifest", None)
    if manifest is not None:
        # Some manifests (e.g. InternalSQLManifest) own their engine directly,
        # separately from `manifest.backends`.
        collect(manifest)
        for backend in (getattr(manifest, "backends", None) or {}).values():
            collect(backend)
        try:
            datasets = commands.get_datasets(context, manifest)
        except Exception:
            datasets = {}
        for dataset in datasets.values():
            for resource in dataset.resources.values():
                if resource.backend is not None:
                    collect(resource.backend)

    for keymap in (getattr(store, "keymaps", None) or {}).values():
        collect(keymap)

    for engine in engines:
        engine.dispose()


def close_test_context_engines() -> None:
    """Dispose engines of all tracked test contexts.

    Each test context creates its own backend (and keymap) engines together with
    their connection pools, which are otherwise only released when the context is
    garbage collected. On CPython 3.14 the cyclic garbage collector reclaims them
    late enough that idle pooled connections pile up across the suite and exhaust
    the PostgreSQL ``max_connections`` limit (``FATAL: sorry, too many clients
    already``). Disposing them after every test keeps the open connection count
    bounded regardless of garbage collection timing. Disposing an engine does not
    invalidate it -- it simply drops idle connections and reconnects on next use --
    so this is safe even for a context that is reused by a later test.
    """
    for context in list(_test_contexts):
        close_context_engines(context)


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
        if self.has("transaction"):
            yield self
        else:
            with self:
                store = self.get("store")
                self.set("auth.token", AdminToken())
                backend = store.manifest.backend
                self.attach("transaction", validate_and_return_transaction, self, backend, write=write)
                yield self

    def wipe(self: TestContext, model: Union[str, Node]):
        if isinstance(model, str):
            store = self.get("store")
            model = commands.get_model(self, store.manifest, model)
        with self.transaction() as context:
            commands.wipe(context, model, model.backend)

    def wipe_all(self: TestContext):
        store = self.get("store")
        self.wipe(commands.get_namespace(self, store.manifest, ""))

    def load(
        self: TestContext, overrides: Optional[Dict[str, Any]] = None, ensure_config_dir: bool = True
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

        rc: RawConfig = self.get("rc")

        if overrides:
            rc.add(
                "test",
                {
                    "environments": {
                        "test": overrides,
                    }
                },
            )

        store = prepare_manifest(self, ensure_config_dir=ensure_config_dir)
        commands.bootstrap(self, store.manifest)

        self.loaded = True

        return self


TestContext = Union[Context, ContextForTests]
