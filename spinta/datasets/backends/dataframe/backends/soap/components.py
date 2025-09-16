from __future__ import annotations
import contextlib

from zeep.proxy import OperationProxy

from spinta.datasets.backends.dataframe.components import DaskBackend
from spinta.utils.schema import NA


class Soap(DaskBackend):
    type: str = "soap"
    query_builder_type = "soap"

    soap_operation: OperationProxy | NA = NA

    @contextlib.contextmanager
    def begin(self):
        yield

    # This backend is used as a parameter for _get_data_soap, which is then passed to Dask reader.
    # Dask requires all the given parameters to be hashable, it uses __dask_tokenize__ to achieve it.
    def __dask_tokenize__(self):
        return self.type, self.name
