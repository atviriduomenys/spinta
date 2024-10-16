import contextlib

from spinta.backends import Backend


class NoBackend(Backend):
    name = 'no backend'
    metadata = {
        'name': 'no backend',
    }

    def __init__(self):
        self.data = {}

    @contextlib.contextmanager
    def begin(self):
        yield self

    def bootstrapped(self):
        return True
