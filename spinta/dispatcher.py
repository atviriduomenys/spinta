from __future__ import annotations

import collections
import inspect
import pathlib
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Tuple
from typing import TypeVar
from typing import cast

from multipledispatch.dispatcher import Dispatcher
from multipledispatch.dispatcher import str_signature

F = TypeVar('F', bound=Callable[[], Any])

_commands: Dict[str, Command] = {}


def command(
    name: str = None,
    schema: dict = None,
) -> Callable[[F], Command]:
    """Define a new command."""

    def _(func: F) -> Command:
        _name = func.__name__ if name is None else name
        if _name in _commands:
            raise Exception(f"{_name!r} is already defined.")
        _commands[_name] = Command(_name)
        _commands[_name].schema = schema or {}
        return _commands[_name]

    return cast(Command, _)


class Command(Dispatcher):
    schema: dict

    def register(
        self,
        *signature_types,
        schema=None,
    ) -> Callable[[F], Command]:
        def _(func: F) -> Command:
            types = signature_types or tuple(_find_func_types(func))
            self.add(types, func)
            return self
        return cast(Command, _)

    def __getitem__(self, types):
        types = types if isinstance(types, tuple) else (types,)
        func = self.dispatch(*types)
        if not func:
            raise NotImplementedError(
                f"Could not find signature for {self.name}: "
                f"<{str_signature(types)}>"
            )
        return func

    def print_methods(self, *args, **kwargs) -> None:
        """Print all commands method in resolution order."""
        if args:
            # Find method by given args.
            args = tuple([type(arg) for arg in args])
            func = self.dispatch(*args)
        else:
            func = None

        base = pathlib.Path().resolve()
        arg_names = _extend_duplicate_names(self.ordering)
        print('---')
        for args in self.ordering:
            func_ = self.funcs[args]
            mark = func_ is func
            _print_method(base, func_, self.name, arg_names, args, mark)


def _extend_duplicate_names(
    argslist: List[Tuple[type]]
) -> Dict[type, str]:

    argnames = collections.defaultdict(set)
    for args in argslist:
        for arg in args:
            name = arg.__name__
            argnames[name].add(arg)

    for i in range(10):
        found = False
        for name in list(argnames):
            if len(argnames[name]) == 1:
                continue
            found = True
            n = name.count('.') + 1
            args = argnames.pop(name)
            for arg in args:
                name = arg.__module__.split('.')[-n:] + [arg.__name__]
                name = '.'.join(name)
                argnames[name].add(arg)
        if not found:
            break

    return {next(iter(v)): k for k, v in argnames.items()}


def _print_method(base, func, name, argnames, args, mark=False):
    file = inspect.getsourcefile(func)
    line = inspect.getsourcelines(func)[1]
    try:
        file = pathlib.Path(file).relative_to(base)
    except ValueError:
        # If two paths do not have common base, then fallback to full
        # file path.
        pass
    argsn = ', '.join([argnames[arg] for arg in args])
    signature = f'{name}({argsn}):'
    marker = ' * ' if mark else '   '
    print(f'{marker}{signature:<60} {file}:{line}')


def _find_func_types(func):
    sig = inspect.signature(func)

    kinds = {
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    }
    for param in sig.parameters.values():
        if param.kind not in kinds or param.annotation is param.empty:
            break
        yield param.annotation
