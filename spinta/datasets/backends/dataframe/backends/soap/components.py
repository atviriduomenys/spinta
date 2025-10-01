from __future__ import annotations
import contextlib

from zeep.proxy import OperationProxy

from spinta.datasets.backends.dataframe.components import DaskBackend
from spinta.datasets.backends.wsdl.components import WsdlBackend

from spinta.exceptions import SoapServiceError


class Soap(DaskBackend):
    type: str = "soap"
    query_builder_type = "soap"

    _soap_operation: OperationProxy | None = None

    wsdl_backend: WsdlBackend
    service: str
    port: str
    operation: str

    @property
    def soap_operation(self) -> OperationProxy:
        return self._soap_operation if self._soap_operation else self._get_soap_operation()

    def _get_soap_operation(self) -> OperationProxy:
        with self.wsdl_backend.begin():
            client = self.wsdl_backend.client

        try:
            soap_service = client.bind(self.service, self.port)
        except ValueError:
            raise SoapServiceError(f"SOAP service {self.service} with port {self.port} not found")

        try:
            soap_operation = soap_service[self.operation]
        except AttributeError:
            raise SoapServiceError(f"SOAP operation {self.operation} in service {self.service} does not exist")

        return soap_operation

    @contextlib.contextmanager
    def begin(self):
        yield

    # This backend is used as a parameter for _get_data_soap, which is then passed to Dask reader.
    # Dask requires all the given parameters to be hashable, it uses __dask_tokenize__ to achieve it.
    def __dask_tokenize__(self):
        return self.type, self.name
