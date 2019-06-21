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
from spinta.exceptions import NotFound
from spinta.utils.url import parse_url_path
from spinta import commands


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


def pull(self, dataset_name, params: dict = None, *, backend='default', ns='default'):
    params = params or {}
    dataset = self.objects[ns]['dataset'][dataset_name]
    push = params.pop('push', True)
    params = {
        'models': params.get('models'),
    }
    data = self.run(dataset, {'pull': params}, backend=None, ns=ns)
    if push:
        return self.push(data, backend=backend, ns=ns)
    else:
        return data

def get(self, model_name: str, object_id, params: dict = None, backend='default', ns='default'):
    params = params or {}
    model = get_model_from_params(self.objects, ns, model_name, params)
    with self.config.backends[backend].transaction() as transaction:
        params = {
            'transaction': transaction,
            'id': object_id,
            'show': params.get('show'),
        }
        return self.run(model, {'get': params}, backend=backend, ns=ns)

def getall(self, model_name: str, params: dict = None, *, backend='default', ns='default'):
    params = params or {}
    model = get_model_from_params(self.objects, ns, model_name, params)
    with self.config.backends[backend].transaction() as transaction:
        params = {
            'transaction': transaction,
            'sort': params.get('sort', [{'name': 'id', 'ascending': True}]),
            'limit': params.get('limit'),
            'offset': params.get('offset'),
            'show': params.get('show'),
            'count': params.get('count'),
        }
        yield from self.run(model, {'getall': params}, backend=backend, ns=ns)

def changes(self, params: dict, *, backend='default', ns='default'):
    model = get_model_from_params(self.objects, ns, params['path'], params)
    with self.config.backends[backend].transaction() as transaction:
        params = {
            'transaction': transaction,
            'limit': params.get('limit'),
            'offset': params['changes'],
            'id': params.get('id', {}).get('value'),
        }
        yield from self.run(model, {'changes': params}, backend=backend, ns=ns)

def export(self, rows, fmt, params: dict = None, *, backend='default', ns='default'):
    command = f'export.{fmt}'
    if command not in self.available_commands:
        raise Exception(f"Unknonwn format {fmt}.")
    params = {
        'wrap': True,
        **(params or {}),
        'rows': rows,
    }

    yield from self.run(self.manifest, {command: params}, backend=None, ns=ns)

def wipe(self, model_name: str, backend='default', ns='default'):
    model = get_model_by_name(context, self, ns, model_name)
    with self.config.backends[backend].transaction() as transaction:
        self.run(model, {'wipe': {'transaction': transaction}}, backend=backend, ns=ns)


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


def get_model_by_name(context: Context, manifest: Manifest, name: str):
    # XXX: use of this function is deprecated, use get_model_from_params
    #      instead.
    params = parse_url_path(context, name)
    return get_model_from_params(manifest, params['path'], params)


def get_model_from_params(manifest, name, params):
    # Allow users to specify a different URL endpoint to make URL look
    # nicer, but that is optional, they can still use model.name.
    if name in manifest.endpoints:
        name = manifest.endpoints[name]

    if name not in manifest.tree:
        raise NotFound(f"Model or collection {name!r} not found.")

    if 'rs' in params:
        dataset = params['ds']
        if dataset not in manifest.objects['dataset']:
            raise NotFound(f"Dataset {dataset!r} not found.")
        resource = params['rs']
        if resource not in manifest.objects['dataset'][dataset].resources:
            raise NotFound(f"Resource ':ds/{dataset}/:rs/{resource}' not found.")
        if name not in manifest.objects['dataset'][dataset].resources[resource].objects:
            return None
        return manifest.objects['dataset'][dataset].resources[resource].objects[name]
    else:
        if name not in manifest.objects['model']:
            return None
        return manifest.objects['model'][name]
