from typing import Union

import importlib

from multipledispatch.dispatcher import Dispatcher

from spinta.components import Context, Model
from spinta.types.datatype import DataType


def execute(
    ufunc: dict,
    context: Context,
    dtype: Union[Model, DataType],
    this: tuple,
):
    config.context.get('config')
    return this[-1]


def ufunc(*types):
    def _(func):
        # Just save given types and load it later with `load_ufuncs`.
        func._ufunc = types
        return func
    return _


def load_ufuncs(ufuncs):
    registry = {}
    for ns, modules in ufuncs.items():
        for module in modules:
            module = importlib.import_module(module)
            for name in dir(module):
                func = getattr(module, name)
                if hasattr(func._ufunc):
                    name = func.__name__.rstrip('_')
                    if ns != 'default':
                        name = f'{ns}.{name}'
                    if name not in registry:
                        registry[name] = Dispatcher(name)
                    dispatcher = registry[name]
                    dispatcher.add(func._ufunc, func)
                    return dispatcher
    return registry


