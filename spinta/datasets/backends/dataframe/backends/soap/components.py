from __future__ import annotations

import contextlib

from zeep.proxy import OperationProxy

from spinta.datasets.backends.dataframe.components import DaskBackend
from spinta.datasets.backends.wsdl.components import WsdlBackend
from spinta.exceptions import SoapServiceError
from spinta.utils.url import append_query_params


class Soap(DaskBackend):
    type: str = "soap"
    query_builder_type = "soap"

    wsdl_backend: WsdlBackend
    service: str
    port: str
    operation: str

    def get_soap_operation(self, extra_headers: dict, query_params: dict | None = None) -> OperationProxy:
        with self.wsdl_backend.begin():
            client = self.wsdl_backend.client

        client.transport.session.headers.update(extra_headers)

        try:
            soap_service = client.bind(self.service, self.port)
        except ValueError:
            raise SoapServiceError(f"SOAP service {self.service} with port {self.port} not found")

        if query_params:
            # `client.bind()` returns a new service proxy, so its endpoint can be changed for this request only.
            # The endpoint is the one from WSDL, outgoing query params are appended to its URL.
            options = soap_service._binding_options
            if options.get("address"):
                options["address"] = append_query_params(options["address"], query_params)

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
