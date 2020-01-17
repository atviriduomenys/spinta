from typing import Union

import importlib

from spinta.dispatcher import Command
from spinta.components import Context, Model
from spinta.types.datatype import DataType


def execute(
    ufunc: dict,
    context: Context,
    dtype: Union[Model, DataType],
    this: tuple,
):
    if isinstance(ufunc, dict):
        config = context.get('config')
        func = config.ufuncs[ufunc['name']]
        args = [execute(arg, context, dtype, this) for arg in ufunc['args']]
        return func(context, dtype, this, *args)
    else:
        return ufunc


_found_ufuncs = []


def ufunc(*types):
    def _(func):
        # Just save given types and load it later with `init_ufuncs`.
        _found_ufuncs.append((func, types))
        return func
    return _


def init_ufuncs(ufuncs):
    # Collect all ufuncs to _found_ufuncs.
    for ns, modules in ufuncs.items():
        for module in modules:
            module = importlib.import_module(module)
    # Create commands from _found_ufuncs
    registry = {}
    for func, types in _found_ufuncs:
        name = func.__name__.rstrip('_')
        if ns != 'default':
            name = f'{ns}.{name}'
        if name not in registry:
            registry[name] = Command(name)
        dispatcher = registry[name]
        dispatcher.add(types, func)
    return registry
