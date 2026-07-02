import contextlib

from spinta.datasets.backends.dataframe.components import DaskBackend


class MemoryDaskBackend(DaskBackend):
    type: str = "dask/memory"

    # Memory backends can set model.source, but it can also leave it open and still work
    model_requires_source: bool = False

    @contextlib.contextmanager
    def begin(self):
        yield
