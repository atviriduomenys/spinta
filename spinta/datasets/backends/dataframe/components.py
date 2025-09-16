import contextlib

from spinta.datasets.components import ExternalBackend


class DaskBackend(ExternalBackend):
    type: str = "dask"

    query_builder_type = "dask"

    @contextlib.contextmanager
    def begin(self):
        yield
