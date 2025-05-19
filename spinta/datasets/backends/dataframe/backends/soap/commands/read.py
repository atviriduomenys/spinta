from typing import Iterator

import dask
import zeep
from zeep.helpers import serialize_object
from zeep.proxy import OperationProxy

from spinta import commands
from spinta.components import Context, Property, Model
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.dataframe.backends.soap.components import Soap
from spinta.datasets.backends.dataframe.commands.read import (
    parametrize_bases,
    get_dask_dataframe_meta,
    dask_get_all
)
from spinta.dimensions.param.components import ResolvedParams
from spinta.exceptions import SoapServiceError, InvalidSource
from spinta.typing import ObjectData


def _get_soap_operation(wsdl_url: str, model: Model) -> OperationProxy:
    model_source = model.external.name
    try:
        service_name, port_name, _, operation_name = model_source.split(".")
    except ValueError:
        error_msg = (
            f'Model source "{model_source}" format is invalid. '
            f'Source must be in following format: "service.port.port_type.operation"'
        )
        raise InvalidSource(model, error=error_msg)

    client = zeep.Client(wsdl=wsdl_url)

    try:
        soap_service = client.bind(service_name, port_name)
    except ValueError:
        raise SoapServiceError(f"SOAP service {service_name} with port {port_name} not found")

    try:
        soap_operation = soap_service[operation_name]
    except AttributeError:
        raise SoapServiceError(f"SOAP operation {operation_name} in service {service_name} does not exist")

    return soap_operation


def _get_data_soap(url: str, model: Model) -> list[dict]:
    response = _get_soap_operation(url, model)()

    return serialize_object(response, target_cls=dict)


@commands.getall.register(Context, Model, Soap)
def getall(
    context: Context,
    model: Model,
    backend: Soap,
    *,
    query: Expr = None,
    resolved_params: ResolvedParams = None,
    extra_properties: dict[str, Property] = None,
    **kwargs
) -> Iterator[ObjectData]:
    bases = parametrize_bases(
        context,
        model,
        model.external.resource,
        resolved_params
    )
    bases = list(bases)
    builder = backend.query_builder_class(context)
    builder.update(model=model)

    meta = get_dask_dataframe_meta(model)
    df = dask.bag.from_sequence(bases).map(_get_data_soap, model=model).flatten().to_dataframe(meta=meta)

    yield from dask_get_all(context, query, df, backend, model, builder, extra_properties)
