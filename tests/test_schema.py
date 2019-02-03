import inspect
import importlib
from pathlib import Path

import pkg_resources as pres
from ruamel.yaml import YAML

from spinta.types import Type, Function, NA

yaml = YAML(typ='safe')


def find_subclasses(Class, modules):
    for module_name in modules:
        module = importlib.import_module(module_name)
        for obj_name in dir(module):
            obj = getattr(module, obj_name)
            if inspect.isclass(obj) and issubclass(obj, Class) and obj is not Class and obj.__module__ == module_name:
                yield obj


class Manifest:

    def __init__(self):
        self.modules = [
            'spinta.types',
            'spinta.types.property',
            'spinta.types.string',
            'spinta.types.object',
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

    def load(self, data):
        assert isinstance(data, dict)
        data = dict(data)
        if 'const' in data and 'type' not in data:
            if isinstance(data['const'], str):
                data['type'] = 'string'
            else:
                raise Exception(f"Unknown data type of {data['const']!r} constant.")
        assert 'type' in data
        assert data['type'] in self.types
        Type = self.types[data['type']]
        obj = Type(data)
        return self.run(obj, {'load': NA})

    def run(self, obj, expr, backend=None):
        for name, args in expr.items():
            types = [cls.name for cls in obj.parents()[:-1]] + [()]
            for type in types:
                if (name, type) in self.functions:
                    Func = self.functions[(name, type)]
                    func = Func(self, obj)
                    if args is NA:
                        return func.execute()
                    else:
                        return func.execute(args)
        raise Exception(f"Function {expr!r} not found.")


def test_schema_loader():
    path = Path(pres.resource_filename('spinta', 'manifest'))
    data = yaml.load((path / 'models/model.yml').read_text())

    import pp
    pp(data)

    manifest = Manifest().discover()
    manifest.load(data)

    pp(manifest.objects)
    pp(data)

    assert False
