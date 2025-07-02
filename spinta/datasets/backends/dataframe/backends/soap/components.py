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
