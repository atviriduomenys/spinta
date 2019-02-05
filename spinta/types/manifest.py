import inspect
import importlib
from pathlib import Path

from ruamel.yaml import YAML
from ruamel.yaml.parser import ParserError

from spinta.types import Type, Function, NA
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
        self.types = {}
        self.functions = {}
        self.backends = {}
        self.connections = {}
        self.objects = {}
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

        # Find all functions.
        for Class in find_subclasses(Function, modules or self.modules):
            types = Class.types if Class.types else ((),)
            for type in types:
                key = (Class.name, type, Class.backend)
                if key in self.functions:
                    old = self.functions[key].__module__ + '.' + self.functions[key].__name__
                    new = Class.__module__ + '.' + Class.__name__
                    raise Exception(f"Function {new} named {Class.name!r} with {type!r} type is already assigned to {old!r}.")
                self.functions[key] = Class

        # Find all backends.
        for Class in find_subclasses(Backend, modules or self.modules):
            if Class.type in self.backends:
                backend = self.backends[Class.type].__name__
                raise Exception(f"Backend {Class.__name__!r} named {Class.type!r} is already assigned to {backend!r}.")
            self.backends[Class.type] = Class

        return self

    def run(self, type, call, backend=None, optional=False, stack=()):
        assert len(stack) < 10

        Func, args = self.find_function(type, call, backend)

        if Func is None:
            if optional:
                return
            if backend:
                message = f"Function {call!r} not found for {type} and {backend}."
            else:
                message = f"Function {call!r} not found for {type}."
            if stack:
                stack[-1].error(message)
            else:
                raise Exception(message)

        if backend:
            backend = self.get_connection(backend)
        else:
            backend = None

        func = Func(self, type, backend, stack)

        if args is NA:
            return func.execute()
        else:
            return func.execute(args)

    def find_function(self, type, call, backend):
        backend = self.config['backends'][backend]['type'] if backend else None
        bases = [cls.metadata.name for cls in type.metadata.bases] + [()]
        for name, args in call.items():
            for base in bases:
                key = name, base, backend
                if key in self.functions:
                    return self.functions[key], args
        return None, None

    def get_type(self, data):
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


class LoadManifest(Function):
    name = 'manifest.load'
    types = ['manifest']

    def execute(self, path: str):
        for file in Path(path).glob('**/*.yml'):
            try:
                data = yaml.load(file.read_text())
            except ParserError as e:
                self.error(f"{file}: {e}.")
            if not isinstance(data, dict):
                self.error(f"{file}: expected dict got {data.__class__.__name__}.")
            data['path'] = file
            type = self.manifest.get_type(data)
            self.run(type, {'manifest.load': data})


class LinkTypes(Function):
    name = 'manifest.link'
    types = ['manifest']

    def execute(self):
        for objects in self.manifest.objects.values():
            for obj in objects.values():
                self.run(obj, {'manifest.link': NA}, optional=True)


class Serialize(Function):
    name = 'serialize'
    types = ['manifest']

    def execute(self):
        output = {}
        for object_type, objects in self.manifest.objects.items():
            output[object_type] = {}
            for name, obj in objects.items():
                output[object_type][name] = self.run(obj, {'serialize': NA})
        return output


class PrepareBackend(Function):
    name = 'backend.prepare'
    types = ['manifest']

    def execute(self):
        for backend in self.manifest.config['backends'].keys():
            self.run(self.schema, {'backend.prepare': NA}, backend=backend)
