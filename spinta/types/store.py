import inspect
import importlib
import pathlib
import types

import pkg_resources as pres

from spinta.commands import load, prepare, migrate, check, push
from spinta.components import Context, Store, Config, Manifest


@load.register()
def load(context: Context, store: Store, config: Config):
    """Load backends and manifests from configuration."""

    store.config = config

    # Load backends
    if 'default' not in config.backends:
        raise Exception("'default' backend must be set in the configuration.")
    for bconf in config.backends.values():
        backend = store.backends[bconf.name] = bconf.Backend()
        load(context, backend, bconf)

    # Load intrnal manifest.
    internal = store.internal = Manifest()
    load(context, internal, {
        'name': 'internal',
        'path': pathlib.Path(pres.resource_filename('spinta', 'manifest')),
        'backend': 'default',
    })

    # Load manifests
    if 'default' not in config.manifests:
        raise Exception("'default' manifest must be set in the configuration.")
    for name, manifest in config.manifests.items():
        store.manifests[name] = Manifest()
        load(context, store.manifests[name], {
            'name': name,
            'backend': 'default',
            **manifest,
        })


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
    backend = store.backends['default']
    manifest = store.manifests['default']
    client_supplied_ids = ClientSuppliedIDs()
    for data in stream:
        data = dict(data)
        model_name = data.pop('type', None)
        assert model_name is not None, data
        model = get_model_by_name(manifest, model_name)
        client_id = client_supplied_ids.replace(model_name, data)
        check(context, model, backend, data)
        inserted_id = push(context, model, backend, data)
        if inserted_id is not None:
            yield client_supplied_ids.update(client_id, {
                **data,
                'type': model_name,
                'id': inserted_id,
            })


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
    model = get_model_from_params(self, ns, model_name, params)
    with self.config.backends[backend].transaction() as transaction:
        params = {
            'transaction': transaction,
            'id': object_id,
            'show': params.get('show'),
        }
        return self.run(model, {'get': params}, backend=backend, ns=ns)

def getall(self, model_name: str, params: dict = None, *, backend='default', ns='default'):
    params = params or {}
    model = get_model_from_params(self, ns, model_name, params)
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
    model = get_model_from_params(self, ns, params['path'], params)
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
    model = get_model_by_name(self, ns, model_name)
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


class ClientSuppliedIDs:

    def __init__(self):
        self.ids = {}

    def replace(self, model_name, data):
        client_id = data.pop('<id>', None)
        for k, v in data.items():
            if not isinstance(v, dict):
                continue
            if set(v.keys()) == {'type', '<id>'}:
                if self.ids[(v['type'], v['<id>'])]:
                    data[k] = self.ids[(v['type'], v['<id>'])]
                else:
                    raise Exception(f"Can't find ID {v['<id>']!r} for {k} property of {model_name}.")
            elif '<id>' in v:
                raise Exception(f"ID replacement works with {{type=x, <id>=y}}, but instead got {data!r}")
        return client_id

    def update(self, client_id, data):
        if client_id is not None:
            self.ids[(data['type'], client_id)] = data['id']
            return {'<id>': client_id, **data}
        return data


def get_model_by_name(manifest, name):
    if '/:source/' in name:
        model, dataset = name.split('/:source/')
        return manifest.objects['dataset'][dataset].objects[model]
    else:
        return manifest.objects['model'][name]


def get_model_from_params(manifest, name, params):
    if 'source' in params:
        return manifest.objects['dataset'][params['source']].objects[name]
    else:
        return manifest.objects['model'][name]
