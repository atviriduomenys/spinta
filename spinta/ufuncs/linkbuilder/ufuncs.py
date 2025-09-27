import logging

from spinta.core.ufuncs import ufunc, Bind
from spinta.datasets.backends.wsdl.components import WsdlBackend
from spinta.exceptions import InvalidSource, SoapServiceError, InvalidValue
from spinta.ufuncs.linkbuilder.components import LinkBuilder

log = logging.getLogger(__name__)


@ufunc.resolver(LinkBuilder, Bind)
def wsdl(env: LinkBuilder, parent_resource_bind: Bind) -> None:
    resource_name = parent_resource_bind.name

    if resource_name == env.resource.name:
        raise InvalidValue(message=f"wsdl() argument {resource_name} must be name of another wsdl type resource.")

    wsdl_resources = (res_name for res_name, res in env.dataset.resources.items() if res.backend.type == "wsdl")
    if resource_name not in wsdl_resources:
        raise InvalidValue(message=f"wsdl() argument {resource_name} must be wsdl type resource.")

    return env.call("wsdl", env.dataset.resources[resource_name].backend)


@ufunc.resolver(LinkBuilder, WsdlBackend)
def wsdl(env: LinkBuilder, parent_resource_backend: WsdlBackend) -> None:
    soap_source = env.resource.backend.config.get("dsn")

    with parent_resource_backend.begin():
        client = parent_resource_backend.client

    try:
        service_name, port_name, _, operation_name = soap_source.split(".")
    except ValueError:
        error_msg = (
            f'Model source "{soap_source}" format is invalid. '
            f'Source must be provided in the following format: "service.port.port_type.operation"'
        )
        raise InvalidSource(env.resource, error=error_msg)

    try:
        soap_service = client.bind(service_name, port_name)
    except ValueError:
        raise SoapServiceError(f"SOAP service {service_name} with port {port_name} not found")

    try:
        soap_operation = soap_service[operation_name]
    except AttributeError:
        raise SoapServiceError(f"SOAP operation {operation_name} in service {service_name} does not exist")

    env.resource.backend.soap_operation = soap_operation
