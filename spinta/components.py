import contextlib
from dataclasses import dataclass


class Context:
    # TODO: Use contextvars to prevent issues in concurent execution flow.
    #
    #       Since Context is a global variable, all changes to it must be async
    #       and thread safe.

    def __init__(self):
        self._factory = [{}]
        self._context = [{}]
        self._exitstack = []
        self._names = [set()]

    def bind(self, name, creator, *args, **kwargs):
        self._set_name(name)
        self._factory[-1][name] = (creator, args, kwargs)

    def set(self, name, value):
        self._set_name(name)
        return self._set_value(name, value)

    def get(self, name):
        if name in self._context[-1]:
            return self._context[-1][name]

        if name not in self._factory[-1]:
            raise Exception(f"Unknown context variable {name!r}.")

        factory, args, kwargs = self._factory[-1][name]
        return self._set_value(name, factory(*args, **kwargs))

    def has(self, name):
        return name in self._context[-1]

    def attach(self, value):
        return self._exitstack[-1].enter_context(value)

    @contextlib.contextmanager
    def enter(self):
        self._factory.append({**self._factory[-1]})
        self._context.append({**self._context[-1]})
        self._exitstack.append(contextlib.ExitStack())
        self._names.append(set())
        with self._exitstack[-1]:
            yield
        self._exitstack.pop()
        self._context.pop()
        self._factory.pop()
        self._names.pop()

    def error(self, message):
        # TODO: Use of this method is deprecated, raise exceptions directly.
        #
        #       New way of handling errors is through `spinta.commands.error`.
        raise Exception(message)

    def _set_name(self, name):
        if name in self._names[-1]:
            raise Exception(f"Context variable {name!r} has been already set.")
        self._names[-1].add(name)

    def _set_value(self, name, value):
        if isinstance(value, contextlib.AbstractContextManager):
            self._context[-1][name] = self._exitstack[-1].enter_context(value)
        else:
            self._context[-1][name] = value
        return self._context[-1][name]


class _CommandsConfig:

    def __init__(self):
        self.modules = []
        self.pull = {}


class Config:

    def __init__(self):
        self.commands = _CommandsConfig()
        self.components = {}
        self.exporters = {}
        self.backends = {}
        self.manifests = {}
        self.ignore = []
        self.debug = False


class Store:

    def __init__(self):
        self.config = None
        self.backends = {}
        self.manifests = {}


class Manifest:

    def __init__(self):
        self.objects = {}

        # {<endpoint>: <model.name>} mapping. There can be multiple model types, but
        # name and endpoint for all of them should match.
        self.endpoints = {}

    def add_model_endpoint(self, model):
        endpoint = model.endpoint
        if endpoint:
            if endpoint not in self.endpoints:
                self.endpoints[endpoint] = model.name
            elif self.endpoints[endpoint] != model.name:
                raise Exception(f"Same endpoint, but different model name, endpoint={endpoint!r}, model.name={model.name!r}.")

    def find_all_models(self):
        yield from self.objects['model'].values()
        for dataset in self.objects['dataset'].values():
            for resource in dataset.resources.values():
                yield from resource.objects.values()


class Node:
    schema = {
        'type': {},
        'path': {'required': True},
        'parent': {'required': True},
        'name': {'required': True},
        'title': {},
        'description': {},
        'backend': {'type': 'backend', 'inherit': True, 'required': True},
    }

    def __init__(self):
        self.manifest = None
        self.name = None
        self.parent = None
        self.path = None
        self.type = None


class Model(Node):
    schema = {
        'unique': {'default': []},
        'extends': {},
        'version': {},
        'date': {},
        'link': {},
        'properties': {'default': {}},
        'endpoint': {},
    }

    def __init__(self):
        super().__init__()
        self.unique = []
        self.extends = None
        self.backend = 'default'
        self.version = None
        self.date = None
        self.link = None
        self.properties = {}

    def get_type_value(self):
        return self.name

    def get_primary_key(self):
        return self.properties['id']


class Property(Node):
    schema = {
        'required': {'default': False},
        'unique': {'default': False},
        'const': {},
        'default': {},
        'nullable': {'default': False},
        'check': {},
        'link': {},
        'enum': {},
        'object': {},
        'items': {},
        'hidden': {'type': 'boolean', 'inherit': True, 'default': False},
    }

    def __init__(self):
        super().__init__()
        self.required = False
        self.unique = False
        self.const = None
        self.default = None
        self.nullable = False
        self.check = None
        self.link = None
        self.enum = None


class Command:

    def __init__(self):
        self.name = None
        self.command = None
        self.args = None

    def __call__(self, *args, **kwargs):
        return self.command(*args, **self.args, **kwargs)


class CommandList:

    def __init__(self):
        self.commands = None

    def __call__(self, *args, **kwargs):
        return [command(*args, **kwargs) for command in self.commands]


@dataclass
class Attachment:
    content_type: str
    filename: str
    data: bytes
