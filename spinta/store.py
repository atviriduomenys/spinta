import inspect
import importlib
import itertools
import pathlib

import pkg_resources as pres

from spinta.types import Type
from spinta.commands import Command


class Store:

    def __init__(self):
        self.modules = [
            'spinta.types',
            'spinta.backends',
            'spinta.commands',
        ]
        self.available_commands = {
            'backend.migrate': {},
            'backend.migrate.internal': {},
            'backend.prepare': {},
            'backend.prepare.internal': {},
            'manifest.check': {},
            'manifest.load': {},
            'manifest.load.backends': {},
            'manifest.load.manifests': {},
            'prepare.type': {},
            'serialize': {},
            'prepare': {},
            'check': {},
            'push': {},
            'pull': {},
            'get': {},
            'getall': {},
            'changes': {},
            'wipe': {},
            'csv': {
                'argument': 'source',
                'arguments': {
                    'source': {'type': 'string'},
                },
            },
            'html': {
                'argument': 'url',
                'arguments': {
                    'url': {'type': 'string'},
                },
            },
            'xml': {
                'argument': 'url',
                'arguments': {
                    'url': {'type': 'string', 'required': True},
                    'items': {'type': 'string', 'required': True},
                },
            },
            'replace': {},
            'hint': {
                'argument': 'source',
                'arguments': {
                    'source': {'type': 'string', 'required': True},
                },
            },
            'self': {},
            'xlsx': {
                'argument': 'url',
                'arguments': {
                    'url': {'type': 'string'},
                },
            },
            'chain': {},
            'json': {
                'argument': 'source',
                'arguments': {
                    'source': {'type': 'string', 'required': True},
                    'items': {'type': 'string', 'required': True},
                },
            },
            'all': {},
            'list': {
                'argument': 'commands',
                'arguments': {
                    'commands': {'type': 'array', 'items': 'command'},
                },
            },
            'denormalize': {},
            'unstack': {},
            'url': {
                'argument': 'url',
                'arguments': {
                    'url': {'type': 'string'},
                },
            },
            'getitem': {
                'argument': 'name',
                'arguments': {
                    'name': {'type': 'string'},
                },
            },
            'export.csv': {
                'arguments': {
                    'cols': {'type': 'array', 'items': {'type': 'string'}},
                    'rows': {'type': 'generator', 'items': {'type': 'dict'}},
                },
            },
            'export.json': {
                'arguments': {
                    'cols': {'type': 'array', 'items': {'type': 'string'}},
                    'rows': {'type': 'generator', 'items': {'type': 'dict'}},
                },
            },
        }
        self.types = None
        self.commands = None
        self.backends = {}
        self.objects = {}
        self.config = None
        self.manifest = None  # internal manifest

    def add_types(self):
        self.types = {}
        for Class in find_subclasses(Type, self.modules):
            if Class.metadata.name in self.types:
                name = self.types[Class.metadata.name].__name__
                raise Exception(f"Type {Class.__name__!r} named {Class.metadata.name!r} is already assigned to {name!r}.")
            self.types[Class.metadata.name] = Class

    def add_commands(self):
        assert self.types is not None, "Run add_types first."
        self.commands = {}
        Backend = self.types['backend']
        backends = {Type.metadata.name for Type in self.types.values() if issubclass(Type, Backend)}
        for Class in find_subclasses(Command, self.modules):
            if Class.metadata.name not in self.available_commands:
                raise Exception(f"Unknown command {Class.metadata.name!r} used by {Class.__module__}.{Class.__name__}.")
            if Class.metadata.backend and Class.metadata.backend not in backends:
                raise Exception(f"Unknown backend {Class.metadata.backend!r} used by {Class.__module__}.{Class.__name__}.")
            for type in Class.metadata.type:
                if type and type not in self.types:
                    raise Exception(f"Unknown type {type} used by {Class.__module__}.{Class.__name__}.")
                key = (Class.metadata.name, type, Class.metadata.backend)
                if key in self.commands:
                    old = self.commands[key].__module__ + '.' + self.commands[key].__name__
                    new = Class.__module__ + '.' + Class.__name__
                    raise Exception(f"Command {new} named {Class.metadata.name!r} with {type!r} type is already assigned to {old!r}.")
                self.commands[key] = Class

    def run(self, obj, call, *, value=None, base=0, backend=None, ns='default', optional=False, stack=()):
        assert self.commands is not None, "Run add_commands first."
        assert isinstance(obj, Type), obj
        assert isinstance(call, dict) and len(call) == 1, call

        command, args = next(iter(call.items()))
        backend = self.config.backends[backend] if backend else None
        backend_type = backend.type if backend else None
        bases = [cls.metadata.name for cls in obj.metadata.bases[base:] if issubclass(cls, Type)] + [None]

        max_call_depth = 10
        if len(stack) >= max_call_depth:
            cmd = Command(self, obj, command, args, value=value, base=bases[-1], backend=backend, ns=ns, stack=stack)
            cmd.error(f"Max depth {max_call_depth} of nested command calls has been reached, aborting.")

        for base, base_name in enumerate(bases, base):
            key = command, base_name, backend_type
            if key in self.commands:
                Cmd = self.commands[key]
                cmd = Cmd(self, obj, command, args, value=value, base=base, backend=backend, ns=ns, stack=stack)
                return cmd.execute()

        if optional:
            return

        keys = []
        for base, base_name in enumerate(bases, base):
            keys.append(f'{command}, type: {base_name}, backend: {backend_type}')
        keys = '\n  - ' + '\n  - '.join(keys)
        message = f"Can't find command {command!r} for {obj.type}. Tried these options:{keys}"
        cmd = Command(self, obj, command, args, value=value, base=base, backend=backend, ns=ns, stack=stack)
        cmd.error(message)

    def configure(self, config):
        assert self.config is None, "Store is already configured!"

        # Load configuration, manifests, backends and etc...
        self.config = self.load({'type': 'config', 'name': 'config', **config}, ns='internal')

        # Configure and check intrnal manifest.
        self.objects['internal'] = {}
        self.manifest = self.load({
            'type': 'manifest',
            'name': 'internal',
            'path': pres.resource_filename('spinta', 'manifest'),
        }, ns='internal')
        self.run(self.manifest, {'manifest.check': None}, ns='internal')

        # Load backends and manifests.
        self.run(self.config, {'manifest.load.backends': None}, ns='internal')
        self.run(self.config, {'manifest.load.manifests': None}, ns='internal')

        # Prepare all types.
        self.run(self.config, {'prepare.type': None}, ns='internal')

        # Check loaded manifests.
        for name, manifest in self.config.manifests.items():
            self.run(manifest, {'manifest.check': None}, ns=name)

    def load(self, data, ns: str = 'default', bare=False, stack=()):
        assert self.types is not None, "Run add_types first."

        if isinstance(data, str):
            data = {'type': data}

        assert isinstance(data, dict), data

        type_name = data.get('type')

        if 'const' in data and type_name is None:
            if isinstance(data['const'], str):
                type_name = 'string'
            else:
                self._load_error(data, ns, stack, f"Unknown data type of {data['const']!r} constant.")

        if type_name is None:
            self._load_error(data, ns, stack, f"Required parameter 'type' is not set.")

        if type_name not in self.types:
            self._load_error(data, ns, stack, f"Unknown type {type_name!r}.")

        Type = self.types[type_name]
        obj = Type()
        if bare:
            for name, params in obj.metadata.properties.items():
                setattr(obj, name, data.get(name) or params.get('default'))
        else:
            self.run(obj, {'manifest.load': {'data': data}}, ns=ns, stack=stack, optional=True)
        return obj

    def _load_error(self, data, ns, stack, message):
        obj = Type()
        for name in ('type', 'name', 'path'):
            setattr(obj, name, data.get(name) if isinstance(data, dict) else None)
        cmd = Command(self, obj, 'manifest.load', {'data': data}, ns=ns, stack=stack)
        cmd.error(message)

    def serialize(self, value=None, ns=None, level=0, limit=99):
        if level > limit:
            return

        if ns is None:
            return {
                k: self.serialize(v, k, level + 1, limit)
                for k, v in self.objects.items()
            }

        if isinstance(value, dict):
            return {
                k: self.serialize(v, ns, level + 1, limit)
                for k, v in value.items()
            }

        if isinstance(value, list):
            return [
                self.serialize(v, ns, level + 1, limit)
                for v in value
            ]

        if isinstance(value, Type):
            return self.serialize(self.run(value, {'serialize': None}, ns=ns), ns, level + 1, limit)

        if isinstance(value, pathlib.Path):
            return str(value)

        return value

    def prepare(self, internal=False):
        assert self.manifest is not None, "Run configure first."
        if internal:
            self.run(self.manifest, {'backend.prepare': None}, backend='default', ns='internal')
        else:
            for name, manifest in self.config.manifests.items():
                self.run(manifest, {'backend.prepare': None}, ns=name)

    def migrate(self, internal=False):
        assert self.manifest is not None, "Run configure first."
        if internal:
            self.run(self.manifest, {'backend.migrate': None}, backend='default', ns='internal')
        else:
            for name, manifest in self.config.manifests.items():
                self.run(manifest, {'backend.migrate': None}, ns=name)

    def push(self, stream, backend='default', ns='default'):
        result = []
        client_supplied_ids = ClientSuppliedIDs()
        with self.config.backends[backend].transaction(write=True) as transaction:
            for data in stream:
                data = dict(data)
                model_name = data.pop('type', None)
                assert model_name is not None, data
                model = get_model_by_name(self, ns, model_name)
                client_id = client_supplied_ids.replace(model_name, data)
                self.run(model, {'check': {'transaction': transaction, 'data': data}}, backend=backend, ns=ns)
                inserted_id = self.run(model, {'push': {'transaction': transaction, 'data': data}}, backend=backend, ns=ns)
                result.append(
                    client_supplied_ids.update(client_id, {
                        'type': model_name,
                        'id': inserted_id,
                    })
                )
        return result

    def pull(self, dataset_name, backend='default', ns='default'):
        dataset = self.objects[ns]['dataset'][dataset_name]
        data = self.run(dataset, {'pull': None}, backend=None, ns=ns)
        return self.push(data, backend=backend, ns=ns)

    def get(self, model_name: str, object_id, params: dict = None, backend='default', ns='default'):
        params = params or {}
        model = get_model_from_params(self, ns, model_name, params)
        with self.config.backends[backend].transaction() as transaction:
            return self.run(model, {'get': {'transaction': transaction, 'id': object_id}}, backend=backend, ns=ns)

    def getall(self, model_name: str, params: dict = None, *, backend='default', ns='default'):
        params = params or {}
        model = get_model_from_params(self, ns, model_name, params)
        with self.config.backends[backend].transaction() as transaction:
            params = {
                'transaction': transaction,
                'sort': params.get('sort', [{'name': 'id', 'ascending': True}]),
                'limit': params.get('limit'),
                'offset': params.get('offset'),
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

    def export(self, fmt, model_name: str, params: dict = None, *, backend='default', ns='default'):
        command = f'export.{fmt}'
        if command not in self.available_commands:
            raise Exception(f"Unknonwn format {fmt}.")
        model = get_model_from_params(self, ns, model_name, params)
        if 'id' in params:
            row = self.get(model_name, params['id']['value'], params, backend=backend, ns=ns)
            cols = list(row.keys())
            yield from self.run(model, {command: {'cols': cols, 'rows': [row], 'wrap': False}}, backend=None, ns=ns)
        else:
            rows = self.getall(model_name, params, backend=backend, ns=ns)
            peek = next(rows)
            cols = list(peek.keys())
            rows = itertools.chain([peek], rows)
            yield from self.run(model, {command: {'cols': cols, 'rows': rows, 'wrap': True}}, backend=None, ns=ns)

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


def get_model_by_name(store, ns, name):
    if '/:source/' in name:
        model, dataset = name.split('/:source/')
        return store.objects[ns]['dataset'][dataset].objects[model]
    else:
        return store.objects[ns]['model'][name]


def get_model_from_params(store, ns, name, params):
    if 'source' in params:
        return store.objects[ns]['dataset'][params['source']].objects[name]
    else:
        return store.objects[ns]['model'][name]
