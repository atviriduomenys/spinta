import contextlib

from spinta.backends.components import Backend


class Memory(Backend):
    data: dict = None

    @contextlib.contextmanager
    def begin(self):
        yield self
