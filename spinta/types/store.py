import inspect
import importlib
import pathlib
import time
import types

import itertools

from spinta import commands
from spinta.backends.components import BackendOrigin
from spinta.backends.helpers import load_backend
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
    # Backends can also be loaded from manifests, see:
    # `spinta.manifests.helpers._load_manifest_backends`.
    store.backends = {}
    for name in rc.keys('backends'):
        data = rc.to_dict('backends', name)
        store.backends[name] = load_backend(
            context,
            config,
            name,
            BackendOrigin.config,
            data,
        )

    # Create default manifest instance
    manifest = rc.get('manifest', required=True)
    manifest = create_manifest(context, store, manifest)
    store.manifest = manifest

    # Create internal manifest instance
    store.internal = create_internal_manifest(context, store)

    # Load accesslog
    store.accesslog = commands.load(context, config.AccessLog(), config)

    return store


@commands.wait.register(Context, Store)
def wait(
    context: Context,
    store: Store,
    *,
    seconds: int = None,
    verbose: bool = False,
) -> bool:
    rc = context.get('rc')

    if seconds is None:
        seconds = rc.get('wait', cast=int, required=True)

    # Collect all backends
    backends = set(itertools.chain(
        store.backends.values(),
        store.manifest.backends.values(),
        (
            resource.backend
            for dataset in store.manifest.datasets.values()
            for resource in dataset.resources.values()
            if resource.backend
        )
    ))

    # XXX: Probably whole this has to be moved to cli
    # Wait while all backends are up.
    i = 0
    fail = False
    start = time.time()
    while backends:
        i += 1
        fail = time.time() - start > seconds
        if verbose:
            print(f"Waiting for backends, attempt #{i}:")
        for backend in sorted(backends, key=lambda b: b.name):
            if verbose:
                print(f"  {backend.name}...")
            if commands.wait(context, backend, fail=fail):
                backends.remove(backend)
        if fail:
            break
        else:
            time.sleep(1)

    if verbose:
        if fail:
            print("Timeout. Not all backends are ready in given time.")
        else:
            print("All backends are up.")

    return True


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
