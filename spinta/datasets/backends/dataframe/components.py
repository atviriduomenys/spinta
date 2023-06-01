import contextlib

from spinta.datasets.components import ExternalBackend


class DaskBackend(ExternalBackend):
    type: str = 'dask'

    @contextlib.contextmanager
    def begin(self):
        yield


class Xml(DaskBackend):
    type: str = 'xml'

    @contextlib.contextmanager
    def begin(self):
        yield

        
class Csv(DaskBackend):
    type: str = 'csv'

    @contextlib.contextmanager
    def begin(self):
        yield


class Json(DaskBackend):
    type: str = 'json'

    @contextlib.contextmanager
    def begin(self):
        yield

