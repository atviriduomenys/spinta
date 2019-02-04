import inspect
import importlib
from pathlib import Path

import pp
import pkg_resources as pres
from ruamel.yaml import YAML
from ruamel.yaml.parser import ParserError

from spinta.types import Type, Function, NA

yaml = YAML(typ='safe')


def find_subclasses(Class, modules):
    for module_name in modules:
        module = importlib.import_module(module_name)
        for obj_name in dir(module):
            obj = getattr(module, obj_name)
            if inspect.isclass(obj) and issubclass(obj, Class) and obj is not Class and obj.__module__ == module_name:
                yield obj


class Manifest(Type):
    metadata = {
        'name': 'manifest',
    }

    def __init__(self):
        self.modules = [
            __name__,
            'spinta.types',
            'spinta.types.object',
            'spinta.types.model',
            'spinta.types.dataset',
            'spinta.types.property',
        ]
        self.types = {}
        self.functions = {}
        self.objects = {}

    def discover(self, modules=None):
        for Class in find_subclasses(Type, modules or self.modules):
            if Class.metadata.name in self.types:
                name = self.types[Class.metadata.name].__name__
                old = self.types[Class.metadata.name].__module__ + '.' + self.types[Class.metadata.name].__name__
                new = Class.__module__ + '.' + Class.__name__
                raise Exception(f"Type {Class.__name__!r} named {Class.metadata.name!r} is already assigned to {name!r}.")
            self.types[Class.metadata.name] = Class

        for Class in find_subclasses(Function, modules or self.modules):
            types = Class.types if Class.types else ((),)
            for type in types:
                key = (Class.name, type)
                if key in self.functions:
                    old = self.functions[key].__module__ + '.' + self.functions[key].__name__
                    new = Class.__module__ + '.' + Class.__name__
                    raise Exception(f"Function {new} named {Class.name!r} with {type!r} type is already assigned to {old!r}.")
                self.functions[key] = Class

        return self

    def run(self, type, call, backend=None, optional=False, stack=()):
        Func, args = self.get_function(type, call)

        if Func is None:
            if optional:
                return
            if stack:
                stack[-1].error(f"Function {call!r} not found for {type}.")
            else:
                raise Exception(f"Function {call!r} not found for {type}.")

        func = Func(self, type, stack)
        if args is NA:
            return func.execute()
        else:
            return func.execute(args)

    def get_function(self, type, call):
        bases = [cls.metadata.name for cls in type.metadata.bases] + [()]
        for name, args in call.items():
            for base in bases:
                if (name, base) in self.functions:
                    return self.functions[(name, base)], args
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


class LoadManifest(Function):
    name = 'load'
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
            self.run(type, {'load': data})


class LinkTypes(Function):
    name = 'link'
    types = ['manifest']

    def execute(self):
        for objects in self.manifest.objects.values():
            for obj in objects.values():
                self.run(obj, {'link': NA}, optional=True)


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


def test_schema_loader():
    manifest = Manifest().discover()
    manifest.run(manifest, {'load': pres.resource_filename('spinta', 'manifest')})
    manifest.run(manifest, {'link': NA})
    pp(manifest.run(manifest, {'serialize': NA}))
    assert False
