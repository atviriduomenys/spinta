import inspect
import importlib
import pathlib

from spinta.types import Type, NA
from spinta.commands import Command, AVAILABLE_COMMANDS
from spinta.backends import Backend


class Store:

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

        # Find all backends.
        for Class in find_subclasses(Backend, modules or self.modules):
            if Class.type in self.backends:
                backend = self.backends[Class.type].__name__
                raise Exception(f"Backend {Class.__name__!r} named {Class.type!r} is already assigned to {backend!r}.")
            self.backends[Class.type] = Class

        # Find all types.
        for Class in find_subclasses(Type, modules or self.modules):
            if Class.metadata.name in self.types:
                type = self.types[Class.metadata.name].__name__
                raise Exception(f"Type {Class.__name__!r} named {Class.metadata.name!r} is already assigned to {type!r}.")
            self.types[Class.metadata.name] = Class

        # Find all commands.
        for Class in find_subclasses(Command, modules or self.modules):
            if Class.metadata.name not in AVAILABLE_COMMANDS:
                raise Exception(f"Unknown command {Class.metadata.name} used by {Class.__module__}.{Class.__name__}.")
            if Class.metadata.backend and Class.metadata.backend not in self.backends:
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

        return self

    def run(self, obj, call, backend=None, ns='default', optional=False, stack=()):
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
            backend = self.get_connection(backend)
        else:
            backend = None

        cmd = Cmd(self, obj, backend, ns, stack)

        if args is None or args is NA:
            return cmd.execute()
        else:
            return cmd.execute(args)

    def get_command(self, obj, call, backend):
        backend = self.config['backends'][backend]['type'] if backend else None
        bases = [cls.metadata.name for cls in obj.metadata.bases] + [None]
        for name, args in call.items():
            for base in bases:
                key = name, base, backend
                if key in self.commands:
                    return self.commands[key], args
        return None, None

    def get_object(self, type):
        if type not in self.types:
            raise Exception(f"Unknown type {type!r}.")
        Type = self.types[type]
        return Type()

    def get_connection(self, name):
        if name not in self.connections:
            assert name in self.config['backends']
            assert self.config['backends'][name]['type'] in self.backends, self.config['backends'][name]['type']
            BackendClass = self.backends[self.config['backends'][name]['type']]
            self.connections[name] = BackendClass(name, self.config['backends'][name])
        return self.connections[name]


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
