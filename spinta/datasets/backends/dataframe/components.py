import contextlib

from spinta.datasets.components import ExternalBackend


class DaskBackend(ExternalBackend):
    type: str = 'dask'

    @contextlib.contextmanager
    def begin(self):
        yield


class Xml(DaskBackend):
    type: str = 'xml'
    accept_types: set = {'text/xml'}

    @contextlib.contextmanager
    def begin(self):
        yield

        
class Csv(DaskBackend):
    type: str = 'csv'
    accept_types: set = {'text/csv'}

    @contextlib.contextmanager
    def begin(self):
        yield


class Json(DaskBackend):
    type: str = 'json'
    accept_types: set = {'application/json'}

    @contextlib.contextmanager
    def begin(self):
        yield

