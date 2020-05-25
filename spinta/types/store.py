import inspect
import importlib
import pathlib
import time
import types

from spinta import commands
from spinta.components import Context, Store
from spinta.urlparams import get_model_by_name
from spinta.manifests.helpers import create_manifest
from spinta.manifests.helpers import create_internal_manifest


@commands.load.register(Context, Store)
def load(context: Context, store: Store) -> Store:
    """Load backends and manifests from configuration."""

    rc = context.get('rc')
    config = context.get('config')

    # Load keymaps
    store.keymaps = {}
    for name in rc.keys('keymaps'):
        keymap_type = rc.get('keymaps', name, 'type', required=True)
        KeyMap = config.components['keymaps'][keymap_type]
        keymap = store.keymaps[name] = KeyMap()
        keymap.name = name
        commands.configure(context, keymap)

    # Load backends
    store.backends = {}
    for name in rc.keys('backends'):
        btype = rc.get('backends', name, 'type', required=True)
        Backend = config.components['backends'][btype]
        backend = store.backends[name] = Backend()
        backend.name = name
        commands.load(context, backend, rc)

    # Create default manifest instance
    manifest = rc.get('manifest', required=True)
    manifest = create_manifest(context, store, manifest)
    store.manifest = manifest

    # Load internal manifest nodes into default manifest instance
    store.internal = create_internal_manifest(context, store)

    # Load accesslog
    store.accesslog = commands.load(context, config.AccessLog(), config)

    return store


@commands.wait.register(Context, Store)
def wait(context: Context, store: Store, *, seconds: int = None):
    rc = context.get('rc')

    if seconds is None:
        seconds = rc.get('wait', cast=int, required=True)

    # Wait while all backends are up.
    for backend in store.backends.values():
        for i in range(1, seconds + 1):
            if wait(context, backend):
                break
            time.sleep(1)
            print(f"Waiting for {backend.name!r} backend {i}...")
        else:
            wait(context, backend, fail=True)


@commands.push.register(Context, Store, types.GeneratorType)
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


@commands.push.register(Context, Store, list)
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
