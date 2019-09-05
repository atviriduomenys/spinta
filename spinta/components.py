import enum
import contextlib
import dataclasses


class Context:

    def __init__(self, name, parent=None):
        self._name = name
        self._parent = parent

        # Bellow all attributes are lists, these lists are context state stacks.
        # When context is forked or new state is activated, then new item is
        # appended to each list, possibly taking shallow copy from previous
        # state. This way, each state can be changed independently.

        # Names defined in current context. Names from inherited context are not
        # listed here.
        self._local_names = [set()]
        self._exitstack = [contextlib.ExitStack()]
        # Context managers.
        self._cmgrs = [{}]

        if parent:
            self._factory = [parent._factory[-1].copy()]
            self._context = [parent._context[-1].copy()]
            self._names = [parent._names[-1].copy()]
        else:
            self._factory = [{}]
            self._context = [{}]
            self._names = [set()]

    def __repr__(self):
        name = []
        parent = self
        while parent is not None:
            name.append(f'{parent._name}:{len(parent._context) - 1}')
            parent = parent._parent
        name = ' < '.join(reversed(name))
        return f'<{self.__class__.__module__}.{self.__class__.__name__}({name}) at 0x{id(self):02x}>'

    def __enter__(self):
        self._context.append(self._context[-1].copy())
        self._exitstack.append(contextlib.ExitStack())
        self._names.append(self._names[-1].copy())
        self._local_names.append(set())
        self._factory.append({})
        self._cmgrs.append({})

    def __exit__(self, *exc):
        self._context.pop()
        self._exitstack.pop().close()
        self._names.pop()
        self._local_names.pop()
        self._factory.pop()
        self._cmgrs.pop()

    @contextlib.contextmanager
    def fork(self, name):
        """Fork this context manager by creating new state.

        This will create new Context class instance based on current state.

        `name` argument is used to identify forked state, this is mostly used
        for debugging, because usually you want to know which state you are
        currently in.

        Forking can be used, when you need to pass context to a function, that
        is executed concurrently.

        Keep in mind, that attached context managers are not copied to the
        forked context. This is because most context managers can be activated
        only once, that is why they must be activated in the same state they
        were attached. If attached context managers were activated before
        creating fork, then activated value will be available in forked state.

        For example if we have 'f' context manager attached like this:

            >>> from contextlib import contextmanager as cm
            >>> base = Context('base')
            >>> base.attach('f', cm(lambda: iter([42]))())

        We can't access it from a fork:

            >>> with base.fork('fork') as fork:  # doctest: +IGNORE_EXCEPTION_DETAIL
            ...     fork.get('f')
            Traceback (most recent call last):
            Exception: Unknown context variable 'f'.

        But if 'f' was already activated from base:

            >>> base.get('f')
            42

        Then it is also available in a fork:

            >>> with base.fork('fork') as fork:
            ...     fork.get('f')
            42

        """
        context = self.__class__(name, self)
        with context._exitstack[-1]:
            yield context

    def bind(self, name, factory, *args, **kwargs):
        """Bind a callable to the context.

        Value of returned by this callable will be cached and callable will be
        called only when first accessed. Subsequent access to this context name
        will return value from cache.
        """
        self._set_local_name(name)
        self._factory[-1][name] = (factory, args, kwargs)

    def attach(self, name, cmgr):
        """Attach new context manager to this context.

        Attached context managers are lazy, they will enter only when accessed
        for the first time. Active context managers will exit, when whole
        context exits.
        """
        self._set_local_name(name)
        self._cmgrs[-1][name] = cmgr

    def set(self, name, value):
        """Set `name` to `value` in current context."""
        self._set_local_name(name)
        self._context[-1][name] = value
        return value

    def get(self, name):
        """Get a value from context.

        If value was previously set, just return it.

        If value was bound as a callback, then call the callback once and set
        returned value to the context.

        If value was attached as context manager, then find where it was
        attached, enter attached context and return its value.

        Raises error, if given `name` was not set, bound or attached previously.
        """

        if name in self._context[-1]:
            return self._context[-1][name]

        # If value is not in current state, then we need to find a factory or
        # a context manager (cmgr) and get value from there. When `name` is
        # found as factory or cmgr, then first we set value in previous state
        # and then set same value in current state.
        stacks = [
            (self._factory, self._get_factory_value),
            (self._cmgrs, self._get_cmgr_value),
        ]
        for stack, value_getter in stacks:
            for state in range(len(stack) - 1, -1, -1):
                if name in stack[state]:
                    if name not in self._context[state]:
                        # Get value and set in on a previous state.
                        self._context[state][name] = value_getter(state, name)
                    if len(self._context) - 1 > state:
                        # If value was found not in current state, then update
                        # current state with value from previous state.
                        self._context[-1][name] = self._context[state][name]
                    return self._context[-1][name]

        raise Exception(f"Unknown context variable {name!r}.")

    def _get_factory_value(self, state, name):
        factory, args, kwargs = self._factory[state][name]
        return factory(*args, **kwargs)

    def _get_cmgr_value(self, state, name):
        cmgr = self._cmgrs[state][name]
        return self._exitstack[state].enter_context(cmgr)

    def has(self, name, local=False, value=False):
        """Check if given name exist in context.

        If `local` is `True`, check only names defined in this context, do not
        look in values defined in parent contexts.

        If `value` is `True`, check only evaluated names. Exclude all bound or
        attached values, that has not yet been accessed.
        """
        if local and value:
            return name in self._local_names[-1] and name in self._context[-1]
        if local:
            return name in self._local_names[-1]
        if value:
            return name in self._context[-1]
        return name in self._names[-1]

    def error(self, message):
        # TODO: Use of this method is deprecated, raise exceptions directly.
        #
        #       New way of handling errors is through `spinta.commands.error`.
        raise Exception(message)

    def _set_local_name(self, name):
        # Prevent redefining local names, but allow to redefine inherited names.
        if name in self._local_names[-1]:
            raise Exception(f"Context variable {name!r} has been already set.")
        self._local_names[-1].add(name)
        self._names[-1].add(name)


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
        'type': {'required': True},
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

    def __repr__(self):
        return f'<{self.__class__.__module__}.{self.__class__.__name__}(name={self.name!r})>'


class Model(Node):
    schema = {
        'unique': {'default': []},
        'extends': {},
        'version': {},
        'date': {},
        'link': {},
        'properties': {'default': {}},
        'flatprops': {},
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
        'model': {'required': True},
        'place': {'required': True},
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
        self.hidden = False


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


@dataclasses.dataclass
class Attachment:
    content_type: str
    filename: str
    data: bytes


class Action(enum.Enum):
    INSERT = 'insert'
    UPSERT = 'upsert'
    UPDATE = 'update'
    PATCH = 'patch'
    DELETE = 'delete'

    WIPE = 'wipe'

    GETONE = 'getone'
    GETALL = 'getall'
    SEARCH = 'search'

    CHANGES = 'changes'

    CONTENTS = 'contents'


class UrlParams:
    model: str
    id: str
    properties: list
    dataset: str
    changes: str
    sort: list
    limit: int
    offset: int
    format: str
    count: bool = False

    # Deprecated, added for backwards compatibility.
    params: dict


class Version:
    version: str
