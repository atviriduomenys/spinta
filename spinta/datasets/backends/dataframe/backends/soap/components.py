import contextlib

from spinta.datasets.backends.dataframe.components import DaskBackend


class Soap(DaskBackend):
    type: str = "soap"

    @contextlib.contextmanager
    def begin(self):
        yield
