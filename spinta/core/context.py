import contextlib
import importlib
import pathlib

from spinta.core.config import read_config
from spinta.utils.imports import importstr


def create_context(name='spinta', rc=None, context=None, args=None):
    if rc is None:
        rc = read_config(args)

    load_commands(rc.get('commands', 'modules', cast=list))

    if context is None:
        Context = rc.get('components', 'core', 'context', cast=importstr, required=True)
        context = Context(name)

    context.set('rc', rc)

    Config = rc.get('components', 'core', 'config', cast=importstr, required=True)
    context.set('config', Config())

    Store = rc.get('components', 'core', 'store', cast=importstr, required=True)
    context.set('store', Store())

    return context


def load_commands(modules):
    for module_path in modules:
        module = importlib.import_module(module_path)
        path = pathlib.Path(module.__file__).resolve()
        if path.name != '__init__.py':
            continue
        path = path.parent
        base = path.parents[module_path.count('.')]
        for path in path.glob('**/*.py'):
            if path.name == '__init__.py':
                module_path = path.parent.relative_to(base)
            else:
                module_path = path.relative_to(base).with_suffix('')
            module_path = '.'.join(module_path.parts)
            module = importlib.import_module(module_path)


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
        return f'<{self.__class__.__module__}.{self.__class__.__name__}({name}) at 0x{id(self):02x}>'

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
