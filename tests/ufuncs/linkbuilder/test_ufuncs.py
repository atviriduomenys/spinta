import pytest

from spinta.components import Context, Property
from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.core.ufuncs import Bind, asttoexpr
from spinta.datasets.components import Dataset, Param, Resource
from spinta.exceptions import InvalidSource, InvalidValue, ParamNotFound
from spinta.spyna import parse
from spinta.testing.manifest import load_manifest_and_context
from spinta.ufuncs.linkbuilder.components import LinkBuilder


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


class TestPrepareParam:
    def _create_env(self, context: Context, param_names: list[str]) -> tuple[LinkBuilder, list[Param]]:
        resource = Resource()
        params = []
        for name in param_names:
            p = Param()
            p.name = name
            params.append(p)
        resource.params = params
        resource.dataset = Dataset()
        return LinkBuilder(context, resource, resource.dataset, this=resource), params

    def test_param_links_property_by_name(self, context: Context) -> None:
        env, (param,) = self._create_env(context, ["status"])
        prop = Property()
        prop.name = "status"

        expr = asttoexpr(parse("param()"))
        env(this=prop).resolve(expr)

        assert prop.param == param

    def test_param_links_property_by_explicit_name(self, context: Context) -> None:
        env, (param,) = self._create_env(context, ["client_id"])
        prop = Property()
        prop.name = "client"

        expr = asttoexpr(parse('param("client_id")'))
        env(this=prop).resolve(expr)

        assert prop.param == param

    def test_param_links_property_by_bind_name(self, context: Context) -> None:
        env, (param,) = self._create_env(context, ["client_id"])
        prop = Property()
        prop.name = "client"

        expr = asttoexpr(parse("param(client_id)"))
        env(this=prop).resolve(expr)

        assert prop.param == param

    def test_param_direct_dispatch_on_property(self, context: Context) -> None:
        env, (param,) = self._create_env(context, ["status"])
        prop = Property()
        prop.name = "status"

        env.call("param", prop)
        assert prop.param == param

    def test_param_direct_dispatch_on_property_with_str(self, context: Context) -> None:
        env, (param,) = self._create_env(context, ["status"])
        prop = Property()
        prop.name = "other"

        env.call("param", prop, "status")
        assert prop.param == param

    def test_param_direct_dispatch_on_property_with_bind(self, context: Context) -> None:
        env, (param,) = self._create_env(context, ["status"])
        prop = Property()
        prop.name = "other"

        env.call("param", prop, Bind("status"))
        assert prop.param == param

    def test_param_raises_param_not_found(self, context: Context) -> None:
        env, _ = self._create_env(context, ["status"])
        prop = Property()
        prop.name = "nonexistent"

        expr = asttoexpr(parse("param()"))
        with pytest.raises(ParamNotFound) as exc_info:
            env(this=prop).resolve(expr)

        assert exc_info.value.context["param"] == "nonexistent"

    def test_param_on_resource_returns_param_object_by_bind(self, context: Context) -> None:
        env, (param,) = self._create_env(context, ["nested_param"])
        expr = asttoexpr(parse("param(nested_param)"))
        result = env.resolve(expr)
        assert result == param

    def test_param_on_resource_returns_param_object_by_str(self, context: Context) -> None:
        env, (param,) = self._create_env(context, ["nested_param"])
        expr = asttoexpr(parse('param("nested_param")'))
        result = env.resolve(expr)
        assert result == param


class TestLinkBuilderThis:
    def test_this_is_required_at_construction(self, context: Context) -> None:
        resource = Resource()
        resource.params = []
        resource.dataset = Dataset()
        with pytest.raises(TypeError):
            LinkBuilder(context, resource, resource.dataset)  # type: ignore[call-arg]

    def test_call_sets_this_without_mutating_original(self, context: Context) -> None:
        resource = Resource()
        resource.params = []
        resource.dataset = Dataset()
        env = LinkBuilder(context, resource, resource.dataset, this=resource)
        prop = Property()
        child = env(this=prop)
        assert child.this is prop
        assert env.this is resource
