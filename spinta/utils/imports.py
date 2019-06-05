import importlib


def importstr(path):
    if ':' not in path:
        raise Exception(f"Can't import python path: {path!r}. Python path must be in 'dotted.path:Name' form.")
    module, obj = path.split(':', 1)
    module = importlib.import_module(module)
    obj = getattr(module, obj)
    return obj
