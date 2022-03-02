from __future__ import annotations

from typing import Any, Optional, List

import importlib
import functools
from typing import Dict
from typing import TYPE_CHECKING
from typing import Tuple

from spinta.dispatcher import Command
from spinta import spyna
from spinta.exceptions import UnknownMethod
from spinta.utils.schema import NA

if TYPE_CHECKING:
    from spinta.components import Context


class Expr:
    name: str
    args: Tuple[Any]
    kwargs: Dict[str, Any]

    def __init__(self, *args, **kwargs):
        # Can't use name arguments, because it can be used in kwargs, so
        # expr.name is extracted from args.
        self.name, *args = args
        self.args = tuple(args)
        self.kwargs = kwargs

    def __repr__(self):
        return str(spyna.unparse(self.todict(), raw=True))

    def __str__(self):
        return str(spyna.unparse(self.todict()))

    def todict(self) -> dict:
        args = [
            v.todict() if isinstance(v, Expr) else v
            for v in self.args
        ]
        kwargs = [
            {
                'name': 'bind',
                'args': [k, v.todict() if isinstance(v, Expr) else v],
            } for k, v in self.kwargs.items()
        ]
        return {
            'name': self.name,
            'args': args + kwargs,
        }

    def __call__(self, *args, **kwargs):
        return type(self)(self.name, *args, **kwargs)

    def resolve(self, env: Env) -> Tuple[List[Any], Dict[str, Any]]:
        args = []
        kwargs = {}
        for arg in self.args:
            arg = env.resolve(arg)
            if isinstance(arg, Pair):
                kwargs[arg.name] = arg.value
            else:
                args.append(arg)
        for key, arg in self.kwargs.items():
            val = env.resolve(arg)
            kwargs[key] = val
        return args, kwargs


class ShortExpr(Expr):
    """Short form of an expression

    For example `a=2` is a short form of `eq(bind('a'),2)`.
    """

    def todict(self) -> dict:
        return {
            **super().todict(),
            'type': 'expression',
        }


class MethodExpr(Expr):
    """Method call expression

    For example `a.b()` instead of `b(a)`.
    """

    def todict(self) -> dict:
        return {
            **super().todict(),
            'type': 'method',
        }


def unparse(expr: Any):
    if expr is NA:
        return None
    if isinstance(expr, Expr):
        ast = expr.todict()
    else:
        ast = expr
    return spyna.unparse(ast)


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
            if names is None:
                self._ufuncs.append((name or func.__name__, func, types))
            else:
                for name_ in names:
                    func_ = _inject_name(func, name_)
                    self._ufuncs.append((name_, func_, types))
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
            if (
                len(types) > 1 and
                not isinstance(types[1], tuple) and
                issubclass(types[1], Expr)
            ):
                # If function is registered with @resolve(Env, Expr), then
                # disable autoargs. This means, that arguments must be resolved
                # manually by calling ufunc(env, expr).
                dispatcher.autoargs = False
        return ufuncs


def _inject_name(func, name):
    def wrapper(env, *args, **kwargs):
        return func(env, name, *args, **kwargs)
    return wrapper


class ufunc:
    resolver = UFuncRegistry()
    executor = UFuncRegistry()


class Env:
    this: Any
    context: Context

    def __init__(
        self,
        context: Context,
        resolvers: UFuncRegistry = None,
        executors: UFuncRegistry = None,
        scope=None,
    ):
        if resolvers is None or executors is None:
            config = context.get('config')
            resolvers = resolvers or config.resolvers
            executors = executors or config.executors
        self.context = context
        self._resolvers = resolvers
        self._executors = executors
        self._scope = scope or {}

    def __call__(self, **scope):
        return type(self)(
            self.context,
            self._resolvers,
            self._executors,
            {**self._scope, **scope},
        )

    def __getattr__(self, name):
        return self._scope[name]

    def update(self, **scope):
        self._scope.update(scope)

    def default_resolver(self, expr: Expr, *args, **kwargs):
        return expr(*args, **kwargs)

    def call(self, name, *args, **kwargs):
        if name not in self._resolvers:
            raise UnknownMethod(expr=str(Expr(name, *args, **kwargs)), name=name)
        ufunc = self._resolvers[name]
        try:
            return ufunc(self, *args, **kwargs)
        except NotImplementedError:
            raise UnknownMethod(expr=str(Expr(name, *args, **kwargs)), name=name)

    def resolve(self, expr: Any):
        if not isinstance(expr, Expr):
            # Expression is already resolved, return resolved value.
            return expr

        if expr.name in self._resolvers:
            ufunc = self._resolvers[expr.name]

        else:
            args, kwargs = expr.resolve(self)
            return self.default_resolver(expr, *args, **kwargs)

        if ufunc.autoargs:
            # Resolve arguments automatically.
            args, kwargs = expr.resolve(self)
            try:
                return ufunc(self, *args, **kwargs)
            except NotImplementedError:
                return self.default_resolver(expr, *args, **kwargs)

        else:
            # Resolve arguments manually.
            return ufunc(self, expr)

    def execute(self, expr: Any):
        if not isinstance(expr, Expr):
            return expr

        if expr.name not in self._executors:
            raise UnknownMethod(expr=str(expr), name=expr.name)

        ufunc = self._executors[expr.name]

        if ufunc.autoargs:
            args = [self.execute(v) for v in expr.args]
            kwargs = {k: self.execute(v) for k, v in expr.kwargs.items()}
            return ufunc(self, *args, **kwargs)

        else:
            return ufunc(self, expr)


def asttoexpr(ast) -> Expr:
    if isinstance(ast, dict):
        args = [asttoexpr(x) for x in ast['args']]
        typ = ast.get('type')
        if typ == 'expression':
            return ShortExpr(ast['name'], *args)
        elif typ == 'method':
            return MethodExpr(ast['name'], *args)
        else:
            return Expr(ast['name'], *args)
    else:
        return ast


class Unresolved:
    pass


class Bind(Unresolved):

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return self.name


class Pair(Unresolved):

    def __init__(self, name, value):
        self.name = name
        self.value = value

    def __repr__(self):
        return f'{self.name}: {self.value!r}'


class Negative(Bind):
    pass


class Positive(Bind):
    pass


bind = functools.partial(Expr, 'bind')
