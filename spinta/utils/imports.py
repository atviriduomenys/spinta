import importlib


def importstr(path):
    module, obj = path.split(':', 1)
    module = importlib.import_module(module)
    obj = getattr(module, obj)
    return obj
