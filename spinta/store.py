import inspect
import importlib
import pathlib

import pkg_resources as pres

from spinta.types import Type, NA
from spinta.commands import Command


class Store:

    def __init__(self):
        self.modules = [
            'spinta.types',
            'spinta.backends',
        ]
        self.available_commands = {
            'backend.migrate',
            'backend.migrate.internal',
            'backend.prepare',
            'backend.prepare.internal',
            'manifest.check',
            'manifest.load',
            'serialize',
            'check',
            'push',
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
                raise Exception(f"Unknown command {Class.metadata.name} used by {Class.__module__}.{Class.__name__}.")
            if Class.metadata.backend and Class.metadata.backend not in backends:
                raise Exception(f"Unknown backend {Class.metadata.backend} used by {Class.__module__}.{Class.__name__}.")
            for type in Class.metadata.type:
                if type and type not in self.types:
                    raise Exception(f"Unknown type {type} used by {Class.__module__}.{Class.__name__}.")
                key = (Class.metadata.name, type, Class.metadata.backend)
                if key in self.commands:
                    old = self.commands[key].__module__ + '.' + self.commands[key].__name__
                    new = Class.__module__ + '.' + Class.__name__
                    raise Exception(f"Command {new} named {Class.metadata.name!r} with {type!r} type is already assigned to {old!r}.")
                self.commands[key] = Class

    def run(self, obj, call, backend=None, ns='default', optional=False, stack=()):
        assert self.commands is not None, "Run add_commands first."
        assert len(stack) < 10

        Cmd, args = self.get_command(obj, call, backend)

        if Cmd is None:
            if optional:
                return
            if backend:
                message = f"Command {call!r} not found for {obj} and {backend}."
            else:
                message = f"Command {call!r} not found for {obj}."
            if stack:
                stack[-1].error(message)
            else:
                raise Exception(message)

        if backend:
            backend = self.config.backends[backend]
        else:
            backend = None

        cmd = Cmd(self, obj, backend, ns, stack)

        if args is None or args is NA:
            return cmd.execute()
        else:
            return cmd.execute(args)

    def get_command(self, obj, call, backend):
        assert self.commands is not None, "Run add_commands first."
        backend = self.config.backends[backend].type if backend else None
        bases = [cls.metadata.name for cls in obj.metadata.bases] + [None]
        for name, args in call.items():
            for base in bases:
                key = name, base, backend
                if key in self.commands:
                    return self.commands[key], args
        return None, None

    def get_object(self, type):
        assert self.types is not None, "Run add_types first."
        if type not in self.types:
            raise Exception(f"Unknown type {type!r}.")
        Type = self.types[type]
        return Type()

    def configure(self, config):
        # Configure and check intrnal manifest.
        assert self.manifest is None, "Store is already configured!"
        self.manifest = self.get_object('manifest')
        self.objects['internal'] = {}
        self.run(self.manifest, {
            'manifest.load': {
                'type': 'manifest',
                'name': 'internal',
                'path': pres.resource_filename('spinta', 'manifest'),
            },
        }, ns='internal')
        self.run(self.manifest, {'manifest.check': None}, ns='internal')

        # Load configuration, manifests, backends and etc...
        self.config = self.get_object('config')
        self.run(self.config, {'manifest.load': {'type': 'config', 'name': 'config', **config}}, ns='internal')

        # Check loaded manifests.
        for name, manifest in self.config.manifests.items():
            self.run(manifest, {'manifest.check': None}, ns=name)

    def prepare(self):
        assert self.manifest is not None, "Run configure first."
        self.run(self.manifest, {'backend.prepare.internal': None}, backend='default', ns='internal')
        for name, manifest in self.config.manifests.items():
            self.run(manifest, {'backend.prepare': None}, ns=name)

    def migrate(self):
        assert self.manifest is not None, "Run configure first."
        self.run(self.manifest, {'backend.migrate.internal': None}, backend='default', ns='internal')
        for name, manifest in self.config.manifests.items():
            self.run(manifest, {'backend.migrate': None}, ns=name)

    def push(self, stream, backend='default', ns='default'):
        result = []
        client_supplied_ids = ClientSuppliedIDs()
        with self.config.backends[backend].transaction() as connection:
            for data in stream:
                data = dict(data)
                model = self.objects[ns]['model'][data.pop('type')]
                client_id = client_supplied_ids.replace(model, data)
                self.run(model, {'check': {'connection': connection, 'data': data}}, backend=backend, ns=ns)
                inserted_id = self.run(model, {'push': {'connection': connection, 'data': data}}, backend=backend, ns=ns)
                result.append(
                    client_supplied_ids.update(client_id, {
                        'type': model.name,
                        'id': inserted_id,
                    })
                )
        return result


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

    def replace(self, model, data):
        client_id = data.pop('<id>', None)
        for k, v in data.items():
            if not isinstance(v, dict):
                continue
            if set(v.keys()) == {'type', '<id>'}:
                if self.ids[(v['type'], v['<id>'])]:
                    data[k] = self.ids[(v['type'], v['<id>'])]
                else:
                    raise Exception(f"Can't find ID {v['<id>']!r} for {k} property of {model.name}.")
            elif '<id>' in v:
                raise Exception(f"ID replacement works with {{type=x, <id>=y}}, but instead got {data!r}")
        return client_id

    def update(self, client_id, data):
        if client_id is not None:
            self.ids[(data['type'], client_id)] = data['id']
            return {'<id>': client_id, **data}
        return data
