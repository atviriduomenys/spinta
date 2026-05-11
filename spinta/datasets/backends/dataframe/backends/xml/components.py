import contextlib

from spinta.datasets.backends.dataframe.components import DaskBackend


class Xml(DaskBackend):
    type: str = "dask/xml"
    query_builder_type = "dask/xml"

    @contextlib.contextmanager
    def begin(self):
        yield
