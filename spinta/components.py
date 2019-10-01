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

        if parent:
            self._cmgrs = [parent._cmgrs[-1].copy()]
            self._factory = [parent._factory[-1].copy()]
            self._names = [parent._names[-1].copy()]

            # We only copy explicitly set keys, and exclude keys coming from
            # `bind` or `attach`.
            copy_keys = set(parent._context[-1]) - (
                set(parent._cmgrs[-1]) | set(parent._factory[-1])
            )
            parent_context = parent._context[-1]
            self._context = [{k: parent_context[k] for k in copy_keys}]
        else:
            self._cmgrs = [{}]
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

        Example:

            def hard_answer():
                return 42

            base = Context('base')
            base.set('easy_answer', 42)
            base.bind('hard_answer', hard_answer)

            with base.fork('fork') as fork:

                # Takes answer from cache, because it was set.
                fork.get('easy_answer')

                # Calls `hard_answer` function, even if it was already called
                # for `base`, because we can't modify `base` to ensure thread
                # safety.
                fork.get('hard_answer')

            # Calls `hard_answer` function, even it was already called in
            # `fork`, to ensure thread safety.
            base.get('hard_answer')

            # Takes hard answer from cache.
            base.get('hard_answer')

        """
        context = self.__class__(name, self)
        with context._exitstack[-1]:
            yield context

    def bind(self, name, factory, *args, **kwargs):
        """Bind a callable to the context.

        Value returned by this callable will be cached and callable will be
        called only when first accessed. Subsequent access to this context name
        will return value from cache.
        """
        self._set_local_name(name)
        self._factory[-1][name] = (factory, args, kwargs)

    def attach(self, name, factory, *args, **kwargs):
        """Attach new context manager factory to this state.

        Attached context manager factory is lazy, it will be created only when
        accessed for the first time. Active context managers will exit, when
        whole context exits.

        More information about context managers:

        https://www.python.org/dev/peps/pep-0343/
        https://docs.python.org/3/library/contextlib.html
        """
        self._set_local_name(name)
        self._cmgrs[-1][name] = (factory, args, kwargs)

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

        Example with bind:

            # State #1
            context = Context('base')

            # Bind a factory on state #1
            context.bind('foo', bar)

            with context:
                # State #2

                with context:
                    # State #3

                    # After calling `bar` function `foo` is cached in current
                    # state #3 and in previous state #1.
                    context.get('foo')

                # Back to state #2. State #3 is removed along with cached `foo`
                # value.

                # Takes 'foo' value from state #1 cache, `bar` function will not
                # be called.
                context.get('foo')

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
                        # Get value and set it on a previous state. This way, if
                        # we exit from current state, value stays in previous
                        # state an can be reused by others state between
                        # previous and current state.
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
        factory, args, kwargs = self._cmgrs[state][name]
        cmgr = factory(*args, **kwargs)
        return self._exitstack[state].enter_context(cmgr)

    def has(self, name, local=False, value=False):
        """Check if given name exists in context.

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
        self.name = None
        self.objects = {}

        # {<endpoint>: <model.name>} mapping. There can be multiple model types, but
        # name and endpoint for all of them should match.
        self.endpoints = {}

    def __repr__(self):
        return f'<{self.__class__.__module__}.{self.__class__.__name__}(name={self.name!r})>'

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
                yield from resource.models()


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

    def __contains__(self, node):
        while node:
            if node is self:
                return True
            node = node.parent
        return False

    def node_type(self):
        """Return node type.

        Usually node type is same as `Node.type` attribute, but in some cases,
        there can be same `Node.type`, but with a different behaviour. For
        example there are two types of `model` nodes, one is
        `spinta.types.dataset.Model` and other is `spinta.components.Model`.
        Both these nodes have `model` type, but `Node.node_type()` returns
        different types, `model:dataset` and `model`.

        In other words, there can be several types of model nodes, they mostly
        act like normal models, but they are implemented differently.
        """
        return self.type

    def model_type(self):
        """Return model name and specifier.

        This is a full and unique model type, used to identify a specific model.
        """
        specifier = self.model_specifier()
        if specifier:
            return f'{self.name}/{specifier}'
        else:
            return self.name

    def model_specifier(self):
        """Return model specifier.

        There can by sever different kinds of models. For example
        `model:dataset` always has a specifier, that looks like this
        `:dataset/dsname`, also, `ns` models have `:ns` specifier.
        """
        raise NotImplementedError


class Namespace(Node):
    schema = {
        'names': {'type': 'object'},
        'models': {'type': 'object'},
    }

    def model_specifier(self):
        return ':ns'


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

    def model_type(self):
        return self.name

    def model_specifier(self):
        return ''


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


class UrlParams:
    model: str
    id: str
    properties: list
    ns: str
    resource: str
    dataset: str
    origin: str
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
