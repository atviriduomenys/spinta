import contextlib

from spinta.datasets.backends.dataframe.components import DaskBackend


class Csv(DaskBackend):
    type: str = "dask/csv"

    # Csv data is mainly one table, meaning model.source is usually not given
    model_requires_source: bool = False

    @contextlib.contextmanager
    def begin(self):
        yield
