import contextlib

from spinta.types import Type


class Backend(Type):
    metadata = {
        'name': 'backend',
    }

    @contextlib.contextmanager
    def transaction(self):
        raise NotImplementedError
