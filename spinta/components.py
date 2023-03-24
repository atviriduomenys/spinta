from __future__ import annotations

from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterator
from typing import Set
from typing import TYPE_CHECKING, List, Optional, AsyncIterator, Union

import enum
import contextlib
import dataclasses
import pathlib
from typing import Type
from typing import TypedDict

from spinta import exceptions
from spinta.dimensions.lang.components import LangData
from spinta.units.components import Unit
from spinta.utils.schema import NA
from spinta.core.enums import Access
from spinta.datasets.enums import Level

if TYPE_CHECKING:
    from spinta.backends.components import Backend
    from spinta.types.datatype import DataType
    from spinta.manifests.components import Manifest
    from spinta.manifests.internal.components import InternalManifest
    from spinta.datasets.components import Attribute
    from spinta.datasets.components import Entity
    from spinta.datasets.keymaps.components import KeyMap
    from spinta.dimensions.enum.components import Enums
    from spinta.dimensions.enum.components import EnumValue
    from spinta.core.config import RawConfig
    from spinta.accesslog import AccessLog
    from spinta.formats.components import Format
    from spinta.dimensions.comments.components import Comment


class Context:
    _name: str
    _parent: Optional[Context]
    _exitstack: List[Optional[contextlib.ExitStack]]
    _local_names: List[Set[str]]

    def __init__(self, name: str, parent: Context = None):
        self._name = name
        self._parent = parent

        # Bellow all attributes are lists, these lists are context state stacks.
        # When context is forked or new state is activated, then new item is
        # appended to each list, possibly taking shallow copy from previous
        # state. This way, each state can be changed independently.

        # Names defined in current context. Names from inherited context are not
        # listed here.
        self._local_names = [set()]
        self._exitstack = [None]

        if parent:
            # cmgrs are copied in __enter__() method.
            self._cmgrs = [{}]
            self._factory = [parent._factory[-1].copy()]
            self._names = [parent._names[-1].copy()]

            # We only copy explicitly set keys, and exclude keys coming from
            # `bind` or `attach`.
            copy_keys = set(parent._context[-1]) - (
                set(parent._cmgrs[-1]) |
                set(parent._factory[-1])
            )
            self._context = [{k: parent._context[-1][k] for k in copy_keys}]
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
        return (
            f'<{self.__class__.__module__}.{self.__class__.__name__}({name}) '
            f'at 0x{id(self):02x}>'
        )

    def __enter__(self):
        self._context.append(self._context[-1].copy())
        self._exitstack.append(contextlib.ExitStack())
        self._names.append(self._names[-1].copy())
        self._local_names.append(set())
        self._factory.append({})
        if self._parent and len(self._cmgrs) == 1:
            # We delay copying cmgrs from parent, because ExitStack is only
            # available from inside with block.
            self._cmgrs.append(self._parent._cmgrs[-1].copy())
        else:
            self._cmgrs.append({})
        return self

    def __exit__(self, *exc):
        self._context.pop()
        self._exitstack.pop().close()
        self._names.pop()
        self._local_names.pop()
        self._factory.pop()
        self._cmgrs.pop()

    def fork(self, name) -> Context:
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
        return type(self)(name, self)

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
        assert self._exitstack[-1] is not None, (
            "You can attach only inside `with context:` block."
        )
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
                        # If value was not found in current state, then update
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


class Store:
    """Data store

    Contains all essential objects like manifest, backends, access log, etc.

    """
    manifest: Manifest = None
    internal: InternalManifest = None
    accesslog: AccessLog = None
    backends: Dict[str, Backend]

    def __init__(self):
        self.config = None
        self.keymaps = {}
        self.backends = {}


class Component:
    schema = {}


class Node(Component):
    schema = {}

    type: str = None
    name: str = None
    parent: Node = None
    manifest: Manifest = None
    path: pathlib.Path = None

    def __repr__(self) -> str:
        return f'<{self.__class__.__module__}.{self.__class__.__name__}(name={self.name!r})>'

    def __hash__(self) -> int:
        # This is a recursive hash, goes down to all parents. There can be nodes
        # with same type and name on different parts of nodes tree, that is why
        # we have to also include parents.
        return hash((self.type, self.name, self.parent))

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
        return ''

    @property
    def basename(self):
        return self.name.split('/')[-1]


# MetaData entry ID can be file path, uuid, table row id of a Model, Dataset,
# etc, depends on manifest type.
EntryId = Union[int, str, pathlib.Path]


class MetaData(Node):
    """Manifest metadata entry.

    This is a top level manifest node like Model, Dataset, Project, Owner.
    """

    eid: EntryId

    schema = {
        'type': {'type': 'string', 'required': True},
        'name': {'type': 'string', 'required': True},
        'id': {'type': 'string'},
        # FIXME: `eid` should be here, but currently it overrides eid, coming
        #        from somewhere else, this has to be fixed.
        # 'eid': {'type': 'string'},
        'version': {'type': 'string'},
        'title': {'type': 'string'},
        'description': {},
        'lang': {'type': 'object'},
    }

    def get_eid_for_error_context(self):
        if (
            isinstance(self.eid, pathlib.Path) and
            self.manifest and
            isinstance(getattr(self.manifest, 'path', None), pathlib.Path)
        ):
            # XXX: Didn't wanted to create command just for this so added na if.
            return str(self.eid.relative_to(self.manifest.path))
        else:
            return str(self.eid)


class NamespaceGiven:
    access: str = None


class Namespace(MetaData):
    access: Access
    keymap: KeyMap = None
    names: Dict[str, Namespace]
    models: Dict[str, Model]
    backend: Backend = None
    parent: Union[Namespace, Manifest]
    title: str
    description: str
    # Namespaces generated from model name.
    generated: bool = False
    given: NamespaceGiven
    lang: LangData = None
    enums: Enums = None

    def __init__(self):
        self.given = NamespaceGiven()

    def model_specifier(self):
        return ':ns'

    def parents(self) -> Iterator[Namespace]:
        ns = self.parent
        while isinstance(ns, Namespace):
            yield ns
            ns = ns.parent

    def is_root(self) -> bool:
        # TODO: Move Namespace component to spinta.namespaces
        from spinta.manifests.components import Manifest
        return isinstance(self.parent, Manifest)


class Base(Node):
    model: Model        # a.base.b - here `model` is `b`
    parent: Model       # a.base.b - here `parent` is `a`
    pk: List[Property]  # a.base.b - list of properties of `a` model
    lang: LangData = None

    schema = {
        'model': {'type': 'string'},
        'pk': {'type': 'string'},
        'lang': {'type': 'object'},
    }


class ModelGiven:
    access: str = None


class Model(MetaData):
    id: str
    level: Level
    access: Access
    title: str
    description: str
    ns: Namespace
    endpoint: str = None
    external: Entity = None
    properties: Dict[str, Property]
    mode: Mode = None
    given: ModelGiven
    lang: LangData = None
    comments: List[Comment] = None

    schema = {
        'keymap': {'type': 'string'},
        'backend': {'type': 'string'},
        'unique': {'default': []},
        'base': {},
        'link': {},
        'properties': {'default': {}},
        'endpoint': {},
        'external': {},
        'level': {
            'type': 'integer',
            'choices': Level,
            'inherit': 'external.resource.level',
        },
        'access': {
            'type': 'string',
            'choices': Access,
            'inherit': 'external.resource.access',
            'default': 'protected',
        },
        'lang': {'type': 'object'},
        'params': {'type': 'object'},
        'comments': {},
    }

    def __init__(self):
        super().__init__()
        self.unique = []
        self.extends = None
        self.keymap = None
        self.backend = None
        self.version = None
        self.date = None
        self.link = None
        self.properties = {}
        self.flatprops = {}
        self.leafprops = {}
        self.given = ModelGiven()
        self.params = {}

    def model_type(self):
        return self.name

    def get_name_without_ns(self):
        if '/' in self.name:
            return self.name.split('/')[-1]
        else:
            return self.name


class PropertyGiven:
    access: str = None
    enum: str = None
    unit: str = None


class Property(Node):
    place: str = None  # Dotted property path
    title: str = None
    description: str = None
    link: str = None
    hidden: bool = False
    access: Access
    level: Level
    dtype: DataType = None
    external: Attribute
    list: Property = None
    model: Model = None
    uri: str = None
    given: PropertyGiven
    enum: EnumValue = None        # Enum name from Enums dict.
    enums: Enums
    lang: LangData = None
    unit: Unit = None       # Given in ref column.
    comments: List[Comment] = None

    schema = {
        'title': {},
        'description': {},
        'link': {},
        'hidden': {'type': 'boolean', 'default': False},
        'level': {
            'type': 'integer',
            'choices': Level,
            'inherit': 'model.level',
        },
        'access': {
            'type': 'string',
            'choices': Access,
            'inherit': 'model.access',
            'default': 'protected',
        },
        'external': {},
        'uri': {'type': 'string'},
        'enum': {'type': 'string'},
        'enums': {},
        'units': {'type': 'string'},
        'lang': {'type': 'object'},
        'comments': {},
    }

    def __init__(self):
        self.given = PropertyGiven()

    def __repr__(self):
        pypath = [type(self).__module__, type(self).__name__]
        pypath = '.'.join(pypath)
        dtype = self.dtype.name if self.dtype else 'none'
        kwargs = [
            f'name={self.place!r}',
            f'type={dtype!r}',
            f'model={self.model.name!r}',
        ]
        kwargs = ', '.join(kwargs)
        return f'<{pypath}({kwargs})>'

    def model_type(self):
        return f'{self.model.name}.{self.place}'

    def is_reserved(self):
        return self.name.startswith('_')


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

    CHECK = 'check'

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_

    @classmethod
    def by_value(cls, value):
        return cls._value2member_map_[value]

    @classmethod
    def values(cls):
        return list(cls._value2member_map_.keys())


class UrlParseNode(TypedDict):
    name: str
    args: List[Any]


class UrlParams:
    parsetree: List[UrlParseNode]

    path_parts: List[str] = None
    path: Optional[str] = None
    model: Optional[Model] = None
    pk: Optional[str] = None
    prop: Optional[Property] = None
    # Tells if we accessing property content or reference. Applies only for some
    # property types, like references.
    propref: bool = False

    # List only models names
    ns: bool = False
    # Recursively select all models in a given namespace
    all: bool = False
    external: bool = False

    changes: bool = False
    changes_offset: Optional[str] = None

    fmt: Format
    format: Optional[str] = None
    formatparams: dict

    select: Optional[List[str]] = None
    sort: List[dict] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    # Limit can be enforced even if it is not explicitly given in URL.
    limit_enforced: bool = False
    limit_enforced_to: int = 100
    # XXX: Deprecated, count should be part of `select`.
    count: bool = False
    # In batch requests, return summary of what was done.
    summary: bool = False
    # In batch requests, continue execution even if some actions fail.
    fault_tolerant: bool = False

    action: Action = None

    # If True, then HTTP HEAD request was made, this means no data must be
    # returned only headers.
    head: bool = False

    query: List[Dict[str, Any]] = None

    def changed_parsetree(self, change):
        ptree = {x['name']: x['args'] for x in (self.parsetree or [])}
        ptree.update(change)
        return [
            {'name': k, 'args': v} for k, v in ptree.items()
        ]


class Version:
    version: str


class DataItem:
    model: Optional[Model] = None       # Data model.
    prop: Optional[Property] = None     # Action on a property, not a whole model.
    propref: bool = False               # Action on property reference or instance.
    backend: Optional[Backend] = None   # Model or property backend depending on prop and propref.
    action: Optional[Action] = None     # Action.
    payload: Optional[dict] = None      # Original data from request.
    given: Optional[dict] = None        # Request data converted to Python-native data types.
    saved: Optional[dict] = None        # Current data stored in database.
    patch: Optional[dict] = None        # Patch that is going to be stored to database.
    error: Optional[exceptions.UserError] = None  # Error while processing data.

    def __init__(
        self,
        model: Optional[Model] = None,
        prop: Optional[Property] = None,
        propref: bool = False,
        backend: Optional[Backend] = None,
        action: Optional[Action] = None,
        payload: Optional[dict] = None,
        error: Optional[exceptions.UserError] = None,
    ):
        self.model = model
        self.prop = prop
        self.propref = propref
        self.backend = model.backend if backend is None and model else backend
        self.action = action
        self.payload = payload
        self.error = error
        self.given = NA
        self.saved = NA
        self.patch = NA

    def __getitem__(self, key):
        return DataSubItem(self, *(
            d.get(key, NA) if d else NA
            for d in (self.given, self.saved, self.patch)
        ))

    def copy(self, **kwargs) -> 'DataItem':
        data = DataItem()
        attrs = [
            'model',
            'prop',
            'propref',
            'backend',
            'action',
            'payload',
            'given',
            'saved',
            'patch',
            'error',
        ]
        assert len(set(kwargs) - set(attrs)) == 0
        for name in attrs:
            if name in kwargs:
                setattr(data, name, kwargs[name])
            elif hasattr(self, name):
                value = getattr(self, name)
                if isinstance(value, dict):
                    setattr(data, name, value.copy())
                else:
                    setattr(data, name, value)
        return data


class DataSubItem:

    def __init__(self, parent, given, saved, patch):
        if isinstance(parent, DataSubItem):
            self.root = parent.root
        else:
            self.root = parent
        self.given = given
        self.saved = saved
        self.patch = patch

    def __getitem__(self, key):
        return DataSubItem(self, *(
            d.get(key, NA) if d else NA
            for d in (self.given, self.saved, self.patch)
        ))

    def __iter__(self):
        yield from self.iter(given=True, saved=True, patch=True)

    def iter(self, given=False, saved=False, patch=False):
        if saved and self.saved:
            given_ = NA
            patch_ = NA
            for saved_ in self.saved:
                yield DataSubItem(self, given_, saved_, patch_)

        if (patch or given) and self.given and self.patch:
            saved_ = NA
            for given_, patch_ in zip(self.given, self.patch):
                yield DataSubItem(self, given_, saved_, patch_)

        elif given and self.given:
            saved_ = NA
            patch_ = NA
            for given_ in self.given:
                yield DataSubItem(self, given_, saved_, patch_)


DataStream = AsyncIterator[DataItem]


class Mode(enum.Enum):
    # Internal mode always use internal backend set on manifest, namespace or
    # model.
    internal = 'internal'

    # External model always sue external backend set on dataset or model's
    # source entity.
    external = 'external'


ScopeFormatterFunc = Callable[[
    Context,
    Union[Namespace, Model, Property],
    Action,
], str]


class Config:
    """Spinta configuration

    This is the place, where all Spinta configuration options are stored and
    used at runtime.

    """
    # TODO: `rc` should not be here, because `Config` might be initialized from
    #       different configuration sources.
    rc: RawConfig
    debug: bool = False
    config_path: pathlib.Path
    server_url: str
    scope_prefix: str
    scope_formatter: ScopeFormatterFunc
    scope_max_length: int
    default_auth_client: str
    http_basic_auth: bool
    token_validation_key: dict = None
    datasets: dict
    env: str
    docs_path: pathlib.Path
    always_show_id: bool = False
    # Limit access to specified namespace root.
    root: str = None
    credentials_file: pathlib.Path
    data_path: pathlib.Path
    AccessLog: Type[AccessLog]
    exporters: Dict[str, Format]

    def __init__(self):
        self.commands = _CommandsConfig()
        self.components = {}
        self.exporters = {}
        self.backends = {}
        self.manifests = {}
        self.ignore = []
        self.debug = False
