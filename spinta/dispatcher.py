import inspect

from multipledispatch.dispatcher import Dispatcher

_commands = {}


def command(schema=None):
    """Define a new command."""

    def _(func):
        name = func.__name__
        if name in _commands:
            raise Exception(f"{name!r} is already defined.")
        _commands[name] = Command(name)
        _commands[name].schema = schema or {}
        return _commands[name]

    return _


class Command(Dispatcher):

    def register(self, schema=None):
        def _(func):
            types = tuple(_find_func_types(func))
            self.add(types, func)
            return self
        return _


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
