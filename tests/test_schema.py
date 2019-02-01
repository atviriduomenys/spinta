import inspect
import importlib
from pathlib import Path
from contextlib import contextmanager

import pkg_resources as pres
from ruamel.yaml import YAML

yaml = YAML(typ='safe')


class ValidationError(Exception):
    pass


class StackNode:

    def __init__(self, name, schema, source, output):
        self.name = name
        self.schema = schema
        self.source = source
        self.output = output


class Context:

    def __init__(self, schema=None):
        self.stack = [StackNode('', schema, None, None)]
        self.functions = FunctionManager().discover().functions
        self.types = TypeManager().discover().types
        self.errors = {}

    def run(self, call, fail=True):
        for name, args in call.items():
            if (name, ()) in self.functions:
                Func = self.functions[(name, ())]
                func = Func(self)
                # args = func.prepare(args)
                return func.execute(args)
        raise Exception(f"Function {call!r} not found.")

    @property
    def schema(self):
        return self.stack[-1].schema

    @property
    def source(self):
        return self.stack[-1].source

    @property
    def output(self):
        return self.stack[-1].output

    @contextmanager
    def push(self, name, schema, source, output):
        self.stack.append(StackNode(name, schema, source, output))
        try:
            yield self
        finally:
            self.stack.pop()

    def error(self, message: str, fail: str = True):
        key = '.'.join([x.name for x in self.stack])
        if key not in self.errors:
            self.errors[key] = []
        self.errors[key].append(message)
        if fail:
            raise ValidationError()
        return self


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
        from spinta.types import Type, Function

        for Class in find_subclasses(Type, modules or self.modules):
            if Class.name in self.types:
                name = self.types[Class.name].__name__
                old = self.types[Class.name].__module__ + '.' + self.types[Class.name].__name__
                new = Class.__module__ + '.' + Class.__name__
                raise Exception(f"Type {Class.__name__!r} named {Class.name!r} is already assigned to {name!r}.")
            self.types[Class.name] = Class

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

    def run(self, obj, expr, backend=None):
        if obj is None:
            from spinta.types import Type
            obj = Type({})

        for name, args in expr.items():
            types = [cls.name for cls in obj.parents()[:-1]] + [()]
            for type in types:
                if (name, type) in self.functions:
                    Func = self.functions[(name, type)]
                    func = Func(self, obj)
                    return func.execute(args)
        raise Exception(f"Function {expr!r} not found.")


def test_schema_loader():
    path = Path(pres.resource_filename('spinta', 'manifest'))
    data = yaml.load((path / 'models/model.yml').read_text())

    import pp
    pp(data)

    manifest = Manifest().discover()
    obj = manifest.run(None, {'load': data})
    manifest.run(obj, {'load': data})

    pp(manifest.objects)

    assert False


# def run(value, call):
#     return Context(call, value).run({'resolve': None})
#
#
# def test_function():
#     run(42, {'default': {}}) == 42
#     run(None, {'default': {}}) is None
#     run(NA, {'default': {}}) == {}
#
#
# def test_type():
#     run(42, {'type': 'object'}) is False
#     run({}, {'type': 'object'}) is True
