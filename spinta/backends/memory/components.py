import contextlib

from spinta.backends.components import Backend
from spinta.backends.components import BackendFeatures


class Memory(Backend):
    data: dict = None

    features = {
        BackendFeatures.WRITE,
    }

    @contextlib.contextmanager
    def begin(self):
        yield self
