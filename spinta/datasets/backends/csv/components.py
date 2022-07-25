import contextlib

from spinta.datasets.components import ExternalBackend


class Csv(ExternalBackend):
    type: str = 'csv'

    @contextlib.contextmanager
    def begin(self):
        yield
