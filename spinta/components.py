import contextlib


class Context:

    def __init__(self):
        self._factory = [{}]
        self._context = [{}]
        self._exitstack = []

    def bind(self, name, creator, *args, **kwargs):
        self._factory[-1][name] = (creator, args, kwargs)

    def set(self, name, value):
        if isinstance(value, contextlib.AbstractContextManager):
            self._context[-1][name] = self._exitstack[-1].enter_context(value)
        else:
            self._context[-1][name] = value
        return self._context[-1][name]

    def get(self, name):
        if name in self._context[-1]:
            return self._context[-1][name]

        if name not in self._factory[-1]:
            raise Exception(f"Unknown context variable {name!r}.")

        factory, args, kwargs = self._factory[-1][name]
        self.set(name, factory(*args, **kwargs))
        return self._context[-1][name]

    def has(self, name):
        return name in self._context[-1]

    def attach(self, value):
        return self._exitstack[-1].enter_context(value)

    @contextlib.contextmanager
    def enter(self):
        self._factory.append({**self._factory[-1]})
        self._context.append({**self._context[-1]})
        self._exitstack.append(contextlib.ExitStack())
        with self._exitstack[-1]:
            yield
        self._exitstack.pop()
        self._context.pop()
        self._factory.pop()

    def error(self, message):
        raise Exception(message)


class _CommandsConfig:

    def __init__(self):
        self.modules = []
        self.pull = {}


class Config:

    def __init__(self):
        self.commands = _CommandsConfig()
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


class Node:

    def __init__(self):
        self.manifest = None
        self.parent = None
        self.name = None
        self.type = None


class Model(Node):
    schema = {
        'type': {},
        'path': {'required': True},
        'parent': {'required': True},
        'name': {'required': True},
        'title': {},
        'description': {},
        'unique': {'default': []},
        'extends': {},
        'backend': {'type': 'backend', 'inherit': True, 'required': True},
        'version': {},
        'date': {},
        'link': {},
        'properties': {'default': {}},
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
        'type': {},
        'path': {'required': True},
        'parent': {'required': True},
        'name': {'required': True},
        'title': {},
        'description': {},
        'required': {'default': False},
        'unique': {'default': False},
        'const': {},
        'default': {},
        'nullable': {'default': False},
        'check': {},
        'link': {},
        'enum': {},
        'object': {},
        'backend': {'type': 'backend', 'inherit': True, 'required': True},
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
