import inspect
import importlib
import pathlib
import time
import types

import pkg_resources as pres

from spinta.commands import load, wait, prepare, migrate, check, push
from spinta.components import Context, Store, Manifest
from spinta.utils.imports import importstr
from spinta.config import RawConfig
from spinta import commands
from spinta.urlparams import get_model_by_name


@load.register()
def load(context: Context, store: Store, config: RawConfig) -> Store:
    """Load backends and manifests from configuration."""

    # Load backends.
    store.backends = {}
    for name in config.keys('backends'):
        Backend = config.get('backends', name, 'backend', cast=importstr)
        backend = store.backends[name] = Backend()
        backend.name = name
        load(context, backend, config)

    # Load intrnal manifest.
    internal = store.internal = Manifest()
    internal.name = 'internal'
    internal.path = pathlib.Path(pres.resource_filename('spinta', 'manifest'))
    internal.backend = store.backends['default']
    load(context, internal, config)

    # Load manifests
    store.manifests = {}
    for name in config.keys('manifests'):
        manifest = store.manifests[name] = Manifest()
        manifest.name = name
        manifest.path = config.get('manifests', name, 'path', cast=pathlib.Path, required=True)
        manifest.backend = store.backends[config.get('manifests', name, 'backend', required=True)]
        load(context, manifest, config)

    if 'default' not in store.manifests:
        raise Exception("'default' manifest must be set in the configuration.")

    return store


@wait.register()
def wait(context: Context, store: Store, config: RawConfig):
    # Wait while all backends are up.
    seconds = config.get('wait', cast=int, required=True)
    for backend in store.backends.values():
        for i in range(1, seconds + 1):
            if wait(context, backend, config):
                break
            time.sleep(1)
            print(f"Waiting for {backend.name!r} backend %s..." % i)
        else:
            wait(context, backend, config, fail=True)


@check.register()
def check(context: Context, store: Store):
    check(context, store.internal)
    for manifest in store.manifests.values():
        check(context, manifest)


@prepare.register()
def prepare(context: Context, store: Store):
    for manifest in store.manifests.values():
        prepare(context, manifest)


@migrate.register()
def migrate(context: Context, store: Store):
    for backend in store.backends.values():
        migrate(context, backend)


@push.register()
def push(context: Context, store: Store, stream: types.GeneratorType):
    manifest = store.manifests['default']
    for data in stream:
        data = dict(data)
        model_name = data.pop('type', None)
        assert model_name is not None, data
        model = get_model_by_name(context, manifest, model_name)
        if 'id' in data:
            id_ = commands.upsert(context, model, model.backend, key=['id'], data=data)
        else:
            id_ = commands.insert(context, model, model.backend, data=data)
        if id_ is not None:
            yield id_


@push.register()
def push(context: Context, store: Store, stream: list):
    yield from push(context, store, (x for x in stream))


def find_subclasses(Class, modules):
    for module_path in modules:
        module = importlib.import_module(module_path)
        path = pathlib.Path(module.__file__).parent
        base = path.parents[module_path.count('.')]
        for path in path.glob('**/*.py'):
            if path.name == '__init__.py':
                module_path = path.parent.relative_to(base)
            else:
                module_path = path.relative_to(base).with_suffix('')
            module_path = '.'.join(module_path.parts)
            module = importlib.import_module(module_path)
            for obj_name in dir(module):
                obj = getattr(module, obj_name)
                if inspect.isclass(obj) and issubclass(obj, Class) and obj is not Class and obj.__module__ == module_path:
                    yield obj
