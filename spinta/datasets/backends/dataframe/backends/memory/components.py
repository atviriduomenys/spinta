import contextlib

from spinta.datasets.backends.dataframe.components import DaskBackend


class MemoryDaskBackend(DaskBackend):
    type: str = "dask/memory"

    @contextlib.contextmanager
    def begin(self):
        yield
