import contextlib

from spinta.datasets.backends.dataframe.ufuncs.query.components import DaskDataFrameQueryBuilder
from spinta.datasets.components import ExternalBackend


class DaskBackend(ExternalBackend):
    type: str = 'dask'

    query_builder_class = DaskDataFrameQueryBuilder

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

