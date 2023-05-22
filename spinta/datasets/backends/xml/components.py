import contextlib

from spinta.datasets.components import ExternalBackend


class Xml(ExternalBackend):
    type: str = 'xml'

    @contextlib.contextmanager
    def begin(self):
        yield
