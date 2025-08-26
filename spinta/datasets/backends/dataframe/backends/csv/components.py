import contextlib

from spinta.datasets.backends.dataframe.components import DaskBackend


class Csv(DaskBackend):
    type: str = "dask/csv"

    @contextlib.contextmanager
    def begin(self):
        yield
