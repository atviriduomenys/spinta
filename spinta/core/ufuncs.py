from typing import Any, Optional, List, Union

import importlib


from spinta.dispatcher import Command


class Expr:

    def __init__(self, name, *args, **kwargs):
        self.name = name
        self.args = args
        self.kwargs = kwargs

    def __call__(self, *args, **kwargs):
        return Expr(self.name, *args, **kwargs)


class Ufunc(Command):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.autoargs = True


class UFuncRegistry:

    def __init__(self, ufuncs=None):
        self._ufuncs = ufuncs or []

    def __call__(
        self,
        *types,
        name: Optional[str] = None,
        names: Optional[List[str]] = None,
    ):
        def decorator(func):
            names_ = names
            if names_ is None:
                if name is None:
                    names_ = [func.__name__]
                else:
                    names_ = [name]
            for name_ in names_:
                self._ufuncs.append((name_, func, types))
            return func
        return decorator

    def collect(self, modules: List[str]):
        # Collect all ufuncs from given list of modules.
        for module in modules:
            # It is enough to just import module, ufuncs will be collected
            # automatically by __call__.
            importlib.import_module(module)

    def ufuncs(self):
        # Create commands from collected ufuncs.
        ufuncs = {}
        for name, func, types in self._ufuncs:
            if name not in ufuncs:
                ufuncs[name] = Ufunc(name)
            dispatcher = ufuncs[name]
            dispatcher.add(types, func)
            if len(types) > 1 and issubclass(types[1], Expr):
                # If function is registered with @resolve(Env, Expr), then
                # disable autoargs. This means, that arguments must be resolved
                # manually by calling ufunc(env, expr).
                dispatcher.autoargs = False
        return ufuncs


class ufunc:
    resolver = UFuncRegistry()
    executor = UFuncRegistry()


class DefaultResolver:

    def __init__(self):
        self.autoargs = False

    def __call__(self, env, expr):
        args = [env.resolve(v) for v in expr.args]
        return expr(*args)


default_resolver = DefaultResolver()


class Env:

    def __init__(
        self,
        context,
        resolvers,
        executors,
        scope=None,
        default_resolver=default_resolver,
    ):
        self.context = context
        self._resolvers = resolvers
        self._executors = executors
        self._scope = scope or {}
        self._default_resolver = default_resolver

    def __call__(self, **scope):
        return Env(
            self.context,
            self._resolvers,
            self._executors,
            {**self._scope, **scope},
            self._default_resolver,
        )

    def __getattr__(self, name):
        return self._scope[name]

    def update(self, **scope):
        self._scope.update(scope)

    def resolve(
        self,
        expr: Any,
        args: Optional[Union[tuple, list]] = None,
        kwargs: Optional[Union[tuple, list, dict]] = None,
    ):
        if not isinstance(expr, Expr):
            # expr is either resolved or a literal value, no need to resolve.
            return expr

        if expr.name in self._resolvers:
            ufunc = self._resolvers[expr.name]
        else:
            ufunc = self._default_resolver

        if not ufunc.autoargs and args is None and kwargs is None:
            # Resolve arguments manually.
            return ufunc(self, expr)

        # Automatically resolve args
        if args is None:
            args = expr.args
        args = [self.resolve(v) for v in args]

        # Automatically resolve kwargs
        if kwargs is None:
            kwargs = expr.kwargs
        if isinstance(kwargs, dict):
            kwargs = {k: self.resolve(v) for k, v in kwargs.items()}
        else:
            pairs = (self.resolve(arg) for arg in kwargs)
            kwargs = {pair.name: pair.value for pair in pairs}

        # Call ufunc
        return ufunc(self, *args, **kwargs)

    def execute(self, expr: Any):
        if isinstance(expr, Expr):
            ufunc = self._executors[expr.name]
            args = [self.execute(v) for v in expr.args]
            kwargs = {k: self.execute(v) for k, v in expr.kwargs.items()}
            return ufunc(self, *args, **kwargs)
        else:
            return expr


def asttoexpr(ast):
    if isinstance(ast, dict):
        args = [asttoexpr(x) for x in ast['args']]
        return Expr(ast['name'], *args)
    else:
        return ast


class Bind:

    def __init__(self, name):
        self.name = name


class Pair:

    def __init__(self, name, value):
        self.name = name
        self.value = value
