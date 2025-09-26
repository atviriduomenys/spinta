import pytest
from zeep.proxy import OperationProxy

from spinta import commands
from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.exceptions import SoapServiceError
from spinta.testing.manifest import load_manifest_and_context


def test_soap_operation_if_source_is_from_dsa(rc: RawConfig) -> None:
    table = """
    d | r | b | m | property | type   | source                                          | prepare
    example                  |        |                                                 |
        | wsdl_resource        | wsdl   | tests/datasets/backends/wsdl/data/wsdl.xml      |
        | soap_resource        | soap   | CityService.CityPort.CityPortType.CityOperation | wsdl(wsdl_resource)
    """
    context, manifest = load_manifest_and_context(rc, table, mode=Mode.external)
    dataset = commands.get_dataset(context, manifest, "example")
    backend = dataset.resources["soap_resource"].backend

    assert isinstance(getattr(backend, "soap_operation", None), OperationProxy)


@pytest.mark.parametrize(
    "service, port, operation",
    [
        ("invalid_service", "invalid_port", "CityOperation"),
        ("invalid_service", "CityPort", "CityOperation"),
        ("CityService", "invalid_port", "CityOperation"),
    ],
)
def test_raise_error_if_service_not_found(
    rc: RawConfig,
    service: str,
    port: str,
    operation: str,
) -> None:
    table = f"""
    d | r | b | m | property | type   | source                                     | prepare
    example                  |        |                                            |
        | wsdl_resource        | wsdl   | tests/datasets/backends/wsdl/data/wsdl.xml |
        | soap_resource        | soap   | {service}.{port}.CityPortType.{operation}  | wsdl(wsdl_resource)
    """
    context, manifest = load_manifest_and_context(rc, table, mode=Mode.external)
    dataset = commands.get_dataset(context, manifest, "example")
    with pytest.raises(SoapServiceError) as e:
        dataset.resources["soap_resource"].backend.soap_operation

    assert str(e.value) == f"SOAP service {service} with port {port} not found"


def test_raise_error_if_operation_does_not_exist_in_service(rc: RawConfig) -> None:
    table = """
    d | r | b | m | property | type   | source                                               | prepare
    example                  |        |                                                      |
        | wsdl_resource        | wsdl   | tests/datasets/backends/wsdl/data/wsdl.xml           |
        | soap_resource        | soap   | CityService.CityPort.CityPortType.invalid_operation  | wsdl(wsdl_resource)
    """
    context, manifest = load_manifest_and_context(rc, table, mode=Mode.external)
    dataset = commands.get_dataset(context, manifest, "example")
    with pytest.raises(SoapServiceError) as e:
        dataset.resources["soap_resource"].backend.soap_operation

    assert str(e.value) == "SOAP operation invalid_operation in service CityService does not exist"
