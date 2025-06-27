from __future__ import annotations
import pytest

from spinta import auth
from spinta.auth import AdminToken, BearerTokenValidator
from spinta.backends.helpers import load_query_builder_class
from spinta.commands import get_model
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.core.ufuncs import asttoexpr
from spinta.datasets.backends.dataframe.backends.soap.ufuncs.components import SoapQueryBuilder
from spinta.exceptions import PropertyNotFound, UnknownMethod
from spinta.manifests.components import Manifest
from spinta.spyna import parse, unparse
from spinta.testing.manifest import prepare_manifest
from spinta.ufuncs.querybuilder.components import QueryParams


def _get_soap_query_builder(
    context: Context,
    manifest: Manifest,
    resource_params: dict | None = None,
    query_params: QueryParams | None = None,
) -> SoapQueryBuilder:
    resource_params = resource_params or {}
    query_params = query_params or QueryParams()

    model = get_model(context, manifest, 'example/City')
    load_query_builder_class(context.get("config"), model.backend)
    query_builder = model.backend.query_builder_class(context)

    return query_builder.init(model.backend, model, resource_params, query_params)


def _get_wsdl_soap_manifest(rc: RawConfig) -> tuple[Context, Manifest]:
    return prepare_manifest(rc, """
        d | r | b | m | property | type    | ref        | source                                          | access | prepare
        example                  | dataset |            |                                                 |        |
          | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
          | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
          |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | input('default_val')
          |   |   |   |          | param   | parameter2 | request_model/param2                            | open   | input('default_val')
          |   |   | City         |         | id         | /                                               | open   |
          |   |   |   | id       | integer |            | id                                              |        |
          |   |   |   | p1       | integer |            |                                                 |        | param(parameter1)
          |   |   |   | p2       | integer |            |                                                 |        | param(parameter2)
        """, mode=Mode.external)


class TestEq:
    def test_assigns_string_value_to_url_params_if_param_name_is_property(self, rc: RawConfig) -> None:
        context, manifest = _get_wsdl_soap_manifest(rc)
        soap_query_builder = _get_soap_query_builder(context, manifest)

        expr = asttoexpr(parse('p1="test"'))
        soap_query_builder.resolve(expr)
        assert soap_query_builder.query_params.url_params == {"p1": "test"}

    def test_raise_error_if_param_name_is_not_property(self, rc: RawConfig) -> None:
        context, manifest = _get_wsdl_soap_manifest(rc)
        soap_query_builder = _get_soap_query_builder(context, manifest)

        expr = asttoexpr(parse('any_prop="test"'))
        with pytest.raises(PropertyNotFound, match="Property 'any_prop' not found"):
            soap_query_builder.resolve(expr)

    def test_raise_error_if_param_value_is_not_string(self, rc: RawConfig) -> None:
        context, manifest = _get_wsdl_soap_manifest(rc)
        soap_query_builder = _get_soap_query_builder(context, manifest)

        expr = asttoexpr(parse('p1=test'))
        with pytest.raises(UnknownMethod, match="Unknown method 'eq' with args"):
            soap_query_builder.resolve(expr)


class TestAnd:
    def test_assign_multiple_string_values_to_url_params(self, rc: RawConfig) -> None:
        context, manifest = _get_wsdl_soap_manifest(rc)
        soap_query_builder = _get_soap_query_builder(context, manifest)

        expr = asttoexpr(parse('p1="test"&p2="test2"'))
        query = soap_query_builder.resolve(expr)

        assert soap_query_builder.query_params.url_params == {"p1": "test", "p2": "test2"}
        assert query is None

    def test_return_expr_that_is_not_resolved_by_soap_query_builder(self, rc: RawConfig) -> None:
        context, manifest = _get_wsdl_soap_manifest(rc)
        soap_query_builder = _get_soap_query_builder(context, manifest)

        expr = asttoexpr(parse('p1="test"&limit(10)'))
        query = soap_query_builder.resolve(expr)

        assert soap_query_builder.query_params.url_params == {"p1": "test"}
        assert unparse(query) == "limit(10)"

    def test_return_and_expr_if_multiple_expressions_are_not_resolved(self, rc: RawConfig) -> None:
        context, manifest = _get_wsdl_soap_manifest(rc)
        soap_query_builder = _get_soap_query_builder(context, manifest)

        expr = asttoexpr(parse('p1="test"&limit(10)&limit(12)'))
        query = soap_query_builder.resolve(expr)

        assert soap_query_builder.query_params.url_params == {"p1": "test"}
        assert unparse(query) == "and(limit(10), limit(12))"


class TestSoapRequestBody:
    def test_only_use_properties_if_node_is_authorized(self, rc: RawConfig) -> None:
        context, manifest = prepare_manifest(rc, """
            d | r | b | m | property | type    | ref        | source                                          | access | prepare
            example                  | dataset |            |                                                 |        |
              | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
              | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
              |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | input('default_val')
              |   |   | City         |         | id         | /                                               | open   |
              |   |   |   | id       | integer |            | id                                              |        |
              |   |   |   | p1       | integer |            |                                                 |        | param(parameter1)
            """, mode=Mode.external)

        # Create auth.token without scope
        pkey = auth.load_key(context, auth.KeyType.private)
        token = auth.create_access_token(context, pkey, "test-client", scopes=None)
        token = auth.Token(token, BearerTokenValidator(context))
        context.set('auth.token', token)

        model = get_model(context, manifest, 'example/City')
        resource_params = {
            param.name: param for param in model.external.resource.params
        }
        soap_query_builder = _get_soap_query_builder(context, manifest, resource_params=resource_params)
        soap_query_builder.build()

        assert soap_query_builder.soap_request_body == {}
        assert soap_query_builder.property_values == {}

    def test_skip_property_if_it_has_external_name(self, rc: RawConfig) -> None:
        context, manifest = prepare_manifest(rc, """
            d | r | b | m | property | type    | ref        | source                                          | access | prepare
            example                  | dataset |            |                                                 |        |
              | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
              | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
              |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | input('default_val')
              |   |   | City         |         | id         | /                                               | open   |
              |   |   |   | id       | integer |            | id                                              |        |
              |   |   |   | p1       | integer |            | any-name                                        |        | param(parameter1)
            """, mode=Mode.external)
        context.set('auth.token', AdminToken())

        model = get_model(context, manifest, 'example/City')
        resource_params = {
            param.name: param for param in model.external.resource.params
        }
        soap_query_builder = _get_soap_query_builder(context, manifest, resource_params=resource_params)
        soap_query_builder.build()

        assert soap_query_builder.soap_request_body == {}
        assert soap_query_builder.property_values == {}

    def test_skip_property_if_it_has_no_expression_in_prepare(self, rc: RawConfig) -> None:
        context, manifest = prepare_manifest(rc, """
            d | r | b | m | property | type    | ref        | source                                          | access | prepare
            example                  | dataset |            |                                                 |        |
              | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
              | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
              |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | input('default_val')
              |   |   | City         |         | id         | /                                               | open   |
              |   |   |   | id       | integer |            | id                                              |        |
              |   |   |   | p1       | integer |            |                                                 |        |

            """, mode=Mode.external)
        context.set('auth.token', AdminToken())

        model = get_model(context, manifest, 'example/City')
        resource_params = {
            param.name: param for param in model.external.resource.params
        }
        soap_query_builder = _get_soap_query_builder(context, manifest, resource_params=resource_params)
        soap_query_builder.build()

        assert soap_query_builder.soap_request_body == {}
        assert soap_query_builder.property_values == {}

    def test_build_body_from_request_param_source_and_default_value_if_url_param_not_given(self, rc: RawConfig) -> None:
        context, manifest = prepare_manifest(rc, """
            d | r | b | m | property | type    | ref        | source                                          | access | prepare
            example                  | dataset |            |                                                 |        |
              | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
              | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
              |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | input('default_val')
              |   |   | City         |         | id         | /                                               | open   |
              |   |   |   | id       | integer |            | id                                              |        |
              |   |   |   | p1       | integer |            |                                                 |        | param(parameter1)

            """, mode=Mode.external)
        context.set('auth.token', AdminToken())

        model = get_model(context, manifest, 'example/City')
        resource_params = {
            param.name: param for param in model.external.resource.params
        }
        soap_query_builder = _get_soap_query_builder(context, manifest, resource_params=resource_params)
        soap_query_builder.build()

        assert soap_query_builder.soap_request_body == {"request_model/param1": "default_val"}
        assert soap_query_builder.property_values == {"parameter1": "default_val"}

    def test_build_body_from_request_param_source_and_url_param_if_url_param_given(self, rc: RawConfig) -> None:
        context, manifest = prepare_manifest(rc, """
            d | r | b | m | property | type    | ref        | source                                          | access | prepare
            example                  | dataset |            |                                                 |        |
              | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
              | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
              |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | input('default_val')
              |   |   | City         |         | id         | /                                               | open   |
              |   |   |   | id       | integer |            | id                                              |        |
              |   |   |   | p1       | integer |            |                                                 |        | param(parameter1)

            """, mode=Mode.external)
        context.set('auth.token', AdminToken())

        query_params = QueryParams()
        query_params.url_params = {"p1": "url_value"}
        model = get_model(context, manifest, 'example/City')
        resource_params = {
            param.name: param for param in model.external.resource.params
        }
        soap_query_builder = _get_soap_query_builder(
            context,
            manifest,
            resource_params=resource_params,
            query_params=query_params,
        )
        soap_query_builder.build()

        assert soap_query_builder.soap_request_body == {"request_model/param1": "url_value"}
        assert soap_query_builder.property_values == {"parameter1": "url_value"}

    def test_build_body_from_request_if_param_source_without_default_and_no_url_param_given(
        self, rc: RawConfig
    ) -> None:
        context, manifest = prepare_manifest(rc, """
            d | r | b | m | property | type    | ref        | source                                          | access | prepare
            example                  | dataset |            |                                                 |        |
              | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
              | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
              |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | input()
              |   |   | City         |         | id         | /                                               | open   |
              |   |   |   | id       | integer |            | id                                              |        |
              |   |   |   | p1       | integer |            |                                                 |        | param(parameter1)

            """, mode=Mode.external)
        context.set('auth.token', AdminToken())

        model = get_model(context, manifest, 'example/City')
        resource_params = {
            param.name: param for param in model.external.resource.params
        }
        soap_query_builder = _get_soap_query_builder(context, manifest, resource_params=resource_params)
        soap_query_builder.build()

        assert soap_query_builder.soap_request_body == {"request_model/param1": None}
        assert soap_query_builder.property_values == {"parameter1": None}
