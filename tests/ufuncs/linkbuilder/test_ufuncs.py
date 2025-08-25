import pytest
from zeep.proxy import OperationProxy

from spinta import commands
from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.exceptions import InvalidValue, SoapServiceError, InvalidSource
from spinta.testing.manifest import load_manifest_and_context


class TestPrepareWsdl:
    def test_adds_soap_operation_to_backend_if_source_is_from_dsa(self, rc: RawConfig) -> None:
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

    def test_raise_error_if_wsdl_argument_is_same_resource(self, rc: RawConfig) -> None:
        table = """
        d | r | b | m | property | type   | source                                          | prepare
        example                  |        |                                                 |
          | wsdl_resource        | wsdl   | tests/datasets/backends/wsdl/data/wsdl.xml      |
          | soap_resource        | soap   | CityService.CityPort.CityPortType.CityOperation | wsdl(soap_resource)
        """

        with pytest.raises(InvalidValue) as e:
            load_manifest_and_context(rc, table, mode=Mode.external)

        assert str(e.value.context["message"]) == (
            "wsdl() argument soap_resource must be name of another wsdl type resource."
        )

    def test_raise_error_if_wsdl_argument_is_not_resource_with_wsdl_type(self, rc: RawConfig) -> None:
        table = """
        d | r | b | m | property | type   | source                                          | prepare
        example                  |        |                                                 |
          | wsdl_resource        | wsdl   | tests/datasets/backends/wsdl/data/wsdl.xml      |
          | soap_resource        | soap   | CityService.CityPort.CityPortType.CityOperation | wsdl(incorrect_resource)
        """

        with pytest.raises(InvalidValue) as e:
            load_manifest_and_context(rc, table, mode=Mode.external)

        assert str(e.value.context["message"]) == ("wsdl() argument incorrect_resource must be wsdl type resource.")

    @pytest.mark.parametrize(
        "invalid_source",
        ["foo", "foo.bar", "foo.bar.baz"],
    )
    def test_raise_error_if_soap_resource_format_is_invalid(self, rc: RawConfig, invalid_source: str) -> None:
        table = f"""
        d | r | b | m | property | type   | source                                     | prepare
        example                  |        |                                            |
          | wsdl_resource        | wsdl   | tests/datasets/backends/wsdl/data/wsdl.xml |
          | soap_resource        | soap   | {invalid_source}                           | wsdl(wsdl_resource)
        """

        with pytest.raises(InvalidSource) as e:
            load_manifest_and_context(rc, table, mode=Mode.external)

        assert str(e.value.context["error"]) == (
            f'Model source "{invalid_source}" format is invalid. '
            f'Source must be provided in the following format: "service.port.port_type.operation"'
        )

    @pytest.mark.parametrize(
        "service, port, operation",
        [
            ("invalid_service", "invalid_port", "CityOperation"),
            ("invalid_service", "CityPort", "CityOperation"),
            ("CityService", "invalid_port", "CityOperation"),
        ],
    )
    def test_raise_error_if_service_not_found(
        self,
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
        with pytest.raises(SoapServiceError) as e:
            load_manifest_and_context(rc, table, mode=Mode.external)

        assert str(e.value) == f"SOAP service {service} with port {port} not found"

    def test_raise_error_if_operation_does_not_exist_in_service(self, rc: RawConfig) -> None:
        table = """
        d | r | b | m | property | type   | source                                               | prepare
        example                  |        |                                                      |
          | wsdl_resource        | wsdl   | tests/datasets/backends/wsdl/data/wsdl.xml           |
          | soap_resource        | soap   | CityService.CityPort.CityPortType.invalid_operation  | wsdl(wsdl_resource)
        """

        with pytest.raises(SoapServiceError) as e:
            load_manifest_and_context(rc, table, mode=Mode.external)

        assert str(e.value) == "SOAP operation invalid_operation in service CityService does not exist"
