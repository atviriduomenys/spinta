import importlib
import inspect
from typing import Any
from typing import Type


def importstr(path):
    if ':' not in path:
        raise Exception(
            f"Can't import python path: {path!r}. Python path must be in "
            f"'dotted.path:Name' form."
        )
    module, obj = path.split(':', 1)
    module = importlib.import_module(module)
    obj = getattr(module, obj)
    return obj


def full_class_name(obj: Any) -> str:
    klass: Type
    if not inspect.isclass(obj):
        klass = type(obj)
    else:
        klass = obj
    return f'{klass.__module__}.{klass.__name__}'
