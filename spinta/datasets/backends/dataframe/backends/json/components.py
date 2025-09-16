import contextlib

from spinta.datasets.backends.dataframe.components import DaskBackend


class Json(DaskBackend):
    type: str = "dask/json"

    @contextlib.contextmanager
    def begin(self):
        yield
