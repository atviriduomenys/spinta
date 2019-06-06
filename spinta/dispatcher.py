import inspect
import pathlib

from multipledispatch.dispatcher import Dispatcher

_commands = {}


def command(name: str = None, schema: dict = None):
    """Define a new command."""

    def _(func):
        _name = func.__name__ if name is None else name
        if _name in _commands:
            raise Exception(f"{_name!r} is already defined.")
        _commands[_name] = Command(_name)
        _commands[_name].schema = schema or {}
        return _commands[_name]

    return _


class Command(Dispatcher):

    def register(self, schema=None):
        def _(func):
            types = tuple(_find_func_types(func))
            self.add(types, func)
            return self
        return _

    def __call__(self, *args, **kwargs):
        try:
            return super().__call__(*args, **kwargs)
        except Exception as exc:
            types = tuple([type(arg) for arg in (exc,) + args])
            if 'error' in _commands and self is not _commands['error'] and _commands['error'].dispatch(*types):
                _commands['error'](exc, *args, **kwargs)
            else:
                raise

    def print_methods(self):
        """Print all commands method in resolution order."""
        argnames = {}
        for args in self.ordering:
            for arg in args:
                name = arg.__name__
                if name in argnames:
                    name = arg.__module__.split('.')[-1] + '.' + name
                argnames[arg] = name
        for args in self.ordering:
            argsn = ', '.join([argnames[arg] for arg in args])
            func = self.funcs[args]
            file = inspect.getsourcefile(func)
            line = inspect.getsourcelines(func)[1]
            file = pathlib.Path(file).relative_to(pathlib.Path().resolve())
            signature = f'{self.name}({argsn}):'
            print(f'{signature:<60} {file}:{line}')


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
