from __future__ import annotations

from spinta.components import Model
from spinta.datasets.backends.dataframe.backends.soap.components import Soap
from spinta.ufuncs.querybuilder.components import QueryBuilder, QueryParams


class SoapQueryBuilder(QueryBuilder):
    backend: Soap
    soap_request_body: dict
    property_values: dict
    params: dict

    def init(
        self,
        backend: Soap,
        model: Model,
        query_params: QueryParams | None,
    ) -> SoapQueryBuilder:
        builder = self(
            backend=backend,
            model=model,
        )
        builder.update(soap_request_body={}, property_values={})
        builder.update(params={param.name: param for param in model.external.resource.params})
        builder.init_query_params(query_params)
        # Request-time values of properties linked to resource params (`param()`), sent in the SOAP body.
        for param_prop in builder.query_params.param_props:
            builder.query_params.url_params[param_prop.prop.place] = str(param_prop.value)

        return builder

    def build(self) -> None:
        self.call("soap_request_body")
