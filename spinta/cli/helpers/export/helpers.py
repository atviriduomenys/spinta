from spinta.backends import Backend
from spinta.components import Context
from spinta.formats.components import Format


def validate_and_return_shallow_backend(context: Context, type_: str) -> Backend:
    """
    Validates and returns shallow backend.
    Shallow backend, is not loaded and is only used for type checking
    """
    config = context.get("config")
    backends = config.components['backends']
    if type_ not in backends:
        raise Exception(f"Unavailable backend, only available: {backends.keys()}")

    backend = config.components['backends'][type_]()
    backend.type = type_
    return backend


def validate_and_return_formatter(context: Context, type_: str) -> Format:
    config = context.get("config")
    exporters = config.exporters

    if type_ not in exporters:
        raise Exception(f"Unavailable formater, only available: {exporters.keys()}")

    fmt = config.exporters[type_]
    return fmt
