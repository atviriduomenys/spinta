import contextlib

from spinta.datasets.backends.dataframe.components import DaskBackend


class Soap(DaskBackend):
    type: str = "soap"
    query_builder_type = "soap"

    @contextlib.contextmanager
    def begin(self):
        yield
