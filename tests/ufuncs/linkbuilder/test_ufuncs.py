import pytest

from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.exceptions import InvalidValue, InvalidSource
from spinta.testing.manifest import load_manifest_and_context


class TestPrepareWsdl:
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
