import inspect
import importlib
from pathlib import Path

from ruamel.yaml import YAML
from ruamel.yaml.parser import ParserError

from spinta.types import Type, NA
from spinta.commands import Command
from spinta.backends import Backend

yaml = YAML(typ='safe')


def find_subclasses(Class, modules):
    for module_path in modules:
        module = importlib.import_module(module_path)
        path = Path(module.__file__).parent
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


class Manifest(Type):
    metadata = {
        'name': 'manifest',
    }

    def __init__(self):
        self.modules = [
            'spinta.types',
            'spinta.backends',
        ]
        self.namespaces = [
            'internal',
            'default',
        ]
        self.types = {}
        self.commands = {}
        self.backends = {}
        self.connections = {}
        self.objects = {ns: {} for ns in self.namespaces}
        self.config = {
            'backends': {
                'default': {
                    'type': 'postgresql',
                    'dsn': 'postgresql:///spinta',
                }
            }
        }

    def discover(self, modules=None):
        # Find all types.
        for Class in find_subclasses(Type, modules or self.modules):
            if Class.metadata.name in self.types:
                type = self.types[Class.metadata.name].__name__
                raise Exception(f"Type {Class.__name__!r} named {Class.metadata.name!r} is already assigned to {type!r}.")
            self.types[Class.metadata.name] = Class

        # Find all commands.
        for Class in find_subclasses(Command, modules or self.modules):
            for type in Class.metadata.type:
                key = (Class.metadata.name, type, Class.metadata.backend)
                if key in self.commands:
                    old = self.commands[key].__module__ + '.' + self.commands[key].__name__
                    new = Class.__module__ + '.' + Class.__name__
                    raise Exception(f"Command {new} named {Class.metadata.name!r} with {type!r} type is already assigned to {old!r}.")
                self.commands[key] = Class

        # Find all backends.
        for Class in find_subclasses(Backend, modules or self.modules):
            if Class.type in self.backends:
                backend = self.backends[Class.type].__name__
                raise Exception(f"Backend {Class.__name__!r} named {Class.type!r} is already assigned to {backend!r}.")
            self.backends[Class.type] = Class

        return self

    def run(self, obj, call, backend=None, ns='default', optional=False, stack=()):
        assert len(stack) < 10

        Cmd, args = self.find_command(obj, call, backend)

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
            backend = self.get_connection(backend)
        else:
            backend = None

        cmd = Cmd(self, obj, backend, ns, stack)

        if args is None or args is NA:
            return cmd.execute()
        else:
            return cmd.execute(args)

    def find_command(self, obj, call, backend):
        backend = self.config['backends'][backend]['type'] if backend else None
        bases = [cls.metadata.name for cls in obj.metadata.bases] + [None]
        for name, args in call.items():
            for base in bases:
                key = name, base, backend
                if key in self.commands:
                    return self.commands[key], args
        return None, None

    def get_obj(self, data):
        assert isinstance(data, dict)
        data = dict(data)
        if 'const' in data and 'type' not in data:
            if isinstance(data['const'], str):
                data['type'] = 'string'
            else:
                raise Exception(f"Unknown data type of {data['const']!r} constant.")
        assert 'type' in data
        if data['type'] not in self.types:
            raise Exception(f"Unknown type: {data['type']!r}.")
        Type = self.types[data['type']]
        return Type()

    def get_connection(self, name):
        if name not in self.connections:
            assert name in self.config['backends']
            assert self.config['backends'][name]['type'] in self.backends, self.config['backends'][name]['type']
            BackendClass = self.backends[self.config['backends'][name]['type']]
            self.connections[name] = BackendClass(name, self.config['backends'][name])
        return self.connections[name]


class LoadManifest(Command):
    metadata = {
        'name': 'manifest.load',
        'type': 'manifest',
    }

    def execute(self, path: str):
        for file in Path(path).glob('**/*.yml'):
            try:
                data = yaml.load(file.read_text())
            except ParserError as e:
                self.error(f"{file}: {e}.")
            if not isinstance(data, dict):
                self.error(f"{file}: expected dict got {data.__class__.__name__}.")
            data['path'] = file
            obj = self.manifest.get_obj(data)
            self.run(obj, {'manifest.load': data})


class CheckManifest(Command):
    metadata = {
        'name': 'manifest.check',
        'type': 'manifest',
    }

    def execute(self):
        for objects in self.manifest.objects[self.ns].values():
            for obj in objects.values():
                self.run(obj, {'manifest.check': None}, optional=True)


class Serialize(Command):
    metadata = {
        'name': 'serialize',
        'type': 'manifest',
    }

    def execute(self):
        output = {}
        for object_type, objects in self.manifest.objects[self.ns].items():
            output[object_type] = {}
            for name, obj in objects.items():
                output[object_type][name] = self.run(obj, {'serialize': NA})
        return output


class PrepareBackend(Command):
    metadata = {
        'name': 'backend.prepare',
        'type': 'manifest',
    }

    def execute(self):
        for backend in self.manifest.config['backends'].keys():
            self.run(self.obj, {self.name: None}, backend=backend)
