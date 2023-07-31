import contextlib

from spinta.datasets.components import ExternalBackend


class Csv(ExternalBackend):
    type: str = 'csv'
    accept_types = {
        'text/csv',
    }

    @contextlib.contextmanager
    def begin(self):
        yield
