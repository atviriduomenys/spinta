import inspect
import importlib
import pathlib
import time
import types

from spinta import commands
from spinta.commands import load, wait, prepare, migrate, check, push
from spinta.components import Context, Store, Config
from spinta.urlparams import get_model_by_name
from spinta.nodes import load_manifest, get_internal_manifest, get_node
from spinta.core.config import RawConfig


@load.register()
def load(context: Context, store: Store, config: Config) -> Store:
    """Load backends and manifests from configuration."""

    rc = config.rc

    # Load backends
    store.backends = {}
    for name in rc.keys('backends'):
        type_ = rc.get('backends', name, 'type', required=True)
        Backend = config.components['backends'][type_]
        backend = store.backends[name] = Backend()
        backend.name = name
        load(context, backend, rc)

    # Load default manifest
    manifest = rc.get('manifest', required=True)
    manifest = store.manifest = load_manifest(context, store, config, manifest)
    commands.load(context, manifest, rc)

    # Load internal manifest into default manifest
    internal = get_internal_manifest(context)
    for data, versions in internal.read(context):
        node = get_node(config, manifest, data)
        node = load(context, node, data, manifest)
        manifest.objects[node.type][node.name] = node

    return store


@wait.register()
def wait(
    context: Context,
    store: Store,
    config: RawConfig,
    *,
    seconds: int = None,
):
    if seconds is None:
        seconds = config.get('wait', cast=int, required=True)

    # Wait while all backends are up.
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
    check(context, store.manifest)


@prepare.register()
def prepare(context: Context, store: Store):
    prepare(context, store.manifest)


@commands.freeze.register()
def freeze(context: Context, store: Store) -> bool:
    commands.freeze(context, store.manifest)


@commands.bootstrap.register()
def bootstrap(context: Context, store: Store) -> bool:
    commands.bootstrap(context, store.manifest)


@commands.sync.register()
def sync(context: Context, store: Store):
    commands.sync(context, store.manifest)


@migrate.register()
def migrate(context: Context, store: Store):
    commands.migrate(context, store.manifest)


@push.register()
def push(context: Context, store: Store, stream: types.GeneratorType):
    manifest = store.manifest
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
