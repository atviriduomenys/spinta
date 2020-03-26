import time

from spinta import commands
from spinta.core.context import Context
from spinta.core.components import create_component
from spinta.components.core import Store
from spinta.helpers.manifests import load_internal_manifest


@commands.load.register()
def load(context: Context, store: Store) -> Store:
    rc = context.get('rc')
    config = context.get('config')

    # Load backends
    store.backends = {}
    for name in rc.keys('backends'):
        store.backends[name] = backend = create_component(
            context,
            store,
            ctype=rc.get('backends', name, 'type', required=True),
            group='backends',
        )
        backend.name = name
        load(context, backend)

    # Load default manifest
    store.manifest = manifest = create_component(
        context,
        store,
        ctype=rc.get('manifest', required=True),
        group='manifests',
    )
    commands.load(context, manifest)

    # Load internal manifest into default manifest
    load_internal_manifest(context)

    # Load accesslog
    store.accesslog = commands.load(context, config.AccessLog(), config)

    return store


@commands.wait.register()
def wait(context: Context, store: Store, *, seconds: int = None):
    if seconds is None:
        config = context.get('config')
        seconds = config.wait
        seconds = config.get('wait', cast=int, required=True)

    # Wait while all backends are up.
    for backend in store.backends.values():
        for i in range(1, seconds + 1):
            if wait(context, backend):
                break
            time.sleep(1)
            print(f"Waiting for {backend.name!r} backend %s..." % i)
        else:
            wait(context, backend, config, fail=True)
