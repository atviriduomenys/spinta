from __future__ import annotations

import pathlib
import uuid

import pytest
from lxml import etree
from pytest_mock import MockerFixture

from spinta import auth
from spinta.auth import AdminToken, BearerTokenValidator, ensure_client_folders_exist
from spinta.backends.helpers import load_query_builder_class
from spinta.commands import get_model
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.core.ufuncs import asttoexpr, Expr
from spinta.datasets.backends.dataframe.backends.soap.ufuncs.components import SoapQueryBuilder
from spinta.exceptions import (
    UnknownMethod,
    InvalidClientBackend,
    InvalidClientBackendCredentials,
    MissingRequiredProperty,
)
from spinta.manifests.components import Manifest
from spinta.spyna import parse, unparse
from spinta.testing.manifest import prepare_manifest
from spinta.ufuncs.querybuilder.components import QueryParams
from spinta.utils.config import get_clients_path


def _get_soap_query_builder(
    context: Context,
    manifest: Manifest,
    query_params: QueryParams | None = None,
) -> SoapQueryBuilder:
    query_params = query_params or QueryParams()

    model = get_model(context, manifest, "example/City")
    load_query_builder_class(context.get("config"), model.backend)
    query_builder = model.backend.query_builder_class(context)

    return query_builder.init(model.backend, model, query_params)


def _set_auth_token_with_client_file(context: Context, clients_path: pathlib.Path, client_backends: dict) -> None:
    ensure_client_folders_exist(clients_path)
    client_id = uuid.uuid4()
    auth.create_client_file(
        clients_path,
        name=str(client_id),
        client_id=str(client_id),
        secret="secret",
        scopes=["spinta_getall"],
        backends=client_backends,
        add_secret=True,
    )

    pkey = auth.load_key(context, auth.KeyType.private)
    token = auth.create_access_token(context, pkey, str(client_id), scopes={"spinta_getall"})
    token = auth.Token(token, BearerTokenValidator(context))
    context.set("auth.token", token)


def _get_wsdl_soap_manifest(rc: RawConfig) -> tuple[Context, Manifest]:
    return prepare_manifest(
        rc,
        """
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
        """,
        mode=Mode.external,
    )


class TestEq:
    @pytest.mark.parametrize("value", ("test", 1))
    def test_assigns_string_value_to_url_params_if_param_name_is_property(
        self, rc: RawConfig, value: str | int
    ) -> None:
        context, manifest = _get_wsdl_soap_manifest(rc)
        soap_query_builder = _get_soap_query_builder(context, manifest)

        expr = asttoexpr(parse(f"p1={value}"))
        soap_query_builder.resolve(expr)
        assert soap_query_builder.query_params.url_params == {"p1": str(value)}

    def test_assigns_bind_name_to_url_params_if_param_name_is_property(self, rc: RawConfig) -> None:
        context, manifest = _get_wsdl_soap_manifest(rc)
        soap_query_builder = _get_soap_query_builder(context, manifest)

        expr = asttoexpr(parse("p1=name"))  # name is bind, because it has no quotes
        soap_query_builder.resolve(expr)
        assert soap_query_builder.query_params.url_params == {"p1": "name"}

    def test_do_not_resolve_expression_if_param_name_is_property_but_without_prepare(self, rc: RawConfig) -> None:
        context, manifest = prepare_manifest(
            rc,
            """
            d | r | b | m | property | type    | ref | source                                          | access | prepare
            example                  | dataset |     |                                                 |        |
              | wsdl_resource        | wsdl    |     | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
              | soap_resource        | soap    |     | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
              |   |   | City         |         | id  | /                                               | open   |
              |   |   |   | id       | integer |     | id                                              |        |
            """,
            mode=Mode.external,
        )
        soap_query_builder = _get_soap_query_builder(context, manifest)

        expr = asttoexpr(parse("id=1"))
        resolved = soap_query_builder.resolve(expr)

        assert type(resolved) is Expr

    def test_do_not_resolve_expression_if_param_name_is_not_property(self, rc: RawConfig) -> None:
        context, manifest = _get_wsdl_soap_manifest(rc)
        soap_query_builder = _get_soap_query_builder(context, manifest)

        expr = asttoexpr(parse('any_prop="test"'))
        resolved = soap_query_builder.resolve(expr)

        assert type(resolved) is Expr


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
    def test_only_read_url_values_for_authorized_properties(self, rc: RawConfig) -> None:
        context, manifest = prepare_manifest(
            rc,
            """
            d | r | b | m | property | type    | ref        | source                                          | access | prepare
            example                  | dataset |            |                                                 |        |
              | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
              | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
              |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | input('default_val')
              |   |   | City         |         | id         | /                                               | open   |
              |   |   |   | id       | integer |            | id                                              |        |
              |   |   |   | p1       | integer |            |                                                 |        | param(parameter1)
            """,
            mode=Mode.external,
        )

        # Create auth.token without scope
        pkey = auth.load_key(context, auth.KeyType.private)
        token = auth.create_access_token(context, pkey, "test-client", scopes=None)
        token = auth.Token(token, BearerTokenValidator(context))
        context.set("auth.token", token)

        query_params = QueryParams()
        query_params.url_params = {"p1": "url_value"}
        soap_query_builder = _get_soap_query_builder(context, manifest, query_params)
        soap_query_builder.build()

        assert soap_query_builder.soap_request_body == {"request_model/param1": "default_val"}
        assert soap_query_builder.property_values == {}

    def test_skip_url_param_reading_if_property_has_external_name(self, rc: RawConfig) -> None:
        context, manifest = prepare_manifest(
            rc,
            """
            d | r | b | m | property | type    | ref        | source                                          | access | prepare
            example                  | dataset |            |                                                 |        |
              | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
              | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
              |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | input('default_val')
              |   |   | City         |         | id         | /                                               | open   |
              |   |   |   | id       | integer |            | id                                              |        |
              |   |   |   | p1       | integer |            | any-name                                        |        | param(parameter1)
            """,
            mode=Mode.external,
        )
        context.set("auth.token", AdminToken())

        query_params = QueryParams()
        query_params.url_params = {"p1": "url_value"}
        soap_query_builder = _get_soap_query_builder(context, manifest, query_params)
        soap_query_builder.build()

        assert soap_query_builder.soap_request_body == {"request_model/param1": "default_val"}
        assert soap_query_builder.property_values == {}

    def test_skip_url_param_reading_if_property_has_no_expression_in_prepare(self, rc: RawConfig) -> None:
        context, manifest = prepare_manifest(
            rc,
            """
            d | r | b | m | property | type    | ref        | source                                          | access | prepare
            example                  | dataset |            |                                                 |        |
              | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
              | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
              |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | input('default_val')
              |   |   | City         |         | id         | /                                               | open   |
              |   |   |   | id       | integer |            | id                                              |        |
              |   |   |   | p1       | integer |            |                                                 |        |
            """,
            mode=Mode.external,
        )
        context.set("auth.token", AdminToken())

        query_params = QueryParams()
        query_params.url_params = {"p1": "url_value"}
        soap_query_builder = _get_soap_query_builder(context, manifest, query_params)
        soap_query_builder.build()

        assert soap_query_builder.soap_request_body == {"request_model/param1": "default_val"}
        assert soap_query_builder.property_values == {}

    def test_build_body_from_request_param_source_and_default_value_if_url_param_not_given(self, rc: RawConfig) -> None:
        context, manifest = prepare_manifest(
            rc,
            """
            d | r | b | m | property | type    | ref        | source                                          | access | prepare
            example                  | dataset |            |                                                 |        |
              | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
              | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
              |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | input('default_val')
              |   |   | City         |         | id         | /                                               | open   |
              |   |   |   | id       | integer |            | id                                              |        |
              |   |   |   | p1       | integer |            |                                                 |        | param(parameter1)
            """,
            mode=Mode.external,
        )
        context.set("auth.token", AdminToken())

        soap_query_builder = _get_soap_query_builder(context, manifest)
        soap_query_builder.build()

        assert soap_query_builder.soap_request_body == {"request_model/param1": "default_val"}
        assert soap_query_builder.property_values == {"parameter1": "default_val"}

    def test_build_body_from_request_param_source_and_url_param_if_url_param_given(self, rc: RawConfig) -> None:
        context, manifest = prepare_manifest(
            rc,
            """
            d | r | b | m | property | type    | ref        | source                                          | access | prepare
            example                  | dataset |            |                                                 |        |
              | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
              | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
              |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | input('default_val')
              |   |   | City         |         | id         | /                                               | open   |
              |   |   |   | id       | integer |            | id                                              |        |
              |   |   |   | p1       | integer |            |                                                 |        | param(parameter1)
            """,
            mode=Mode.external,
        )
        context.set("auth.token", AdminToken())

        query_params = QueryParams()
        query_params.url_params = {"p1": "url_value"}
        soap_query_builder = _get_soap_query_builder(context, manifest, query_params=query_params)
        soap_query_builder.build()

        assert soap_query_builder.soap_request_body == {"request_model/param1": "url_value"}
        assert soap_query_builder.property_values == {"parameter1": "url_value"}

    def test_build_body_from_request_if_param_source_without_default_and_no_url_param_given(
        self, rc: RawConfig
    ) -> None:
        context, manifest = prepare_manifest(
            rc,
            """
            d | r | b | m | property | type    | ref        | source                                          | access | prepare
            example                  | dataset |            |                                                 |        |
              | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
              | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
              |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | input()
              |   |   | City         |         | id         | /                                               | open   |
              |   |   |   | id       | integer |            | id                                              |        |
              |   |   |   | p1       | integer |            |                                                 |        | param(parameter1)
            """,
            mode=Mode.external,
        )
        context.set("auth.token", AdminToken())

        soap_query_builder = _get_soap_query_builder(context, manifest)
        soap_query_builder.build()

        assert soap_query_builder.soap_request_body == {}

    def test_build_body_from_request_with_required_prop_without_default_and_no_url_param_given(
        self, rc: RawConfig
    ) -> None:
        context, manifest = prepare_manifest(
            rc,
            """
            d | r | b | m | property | type             | ref        | source                                          | access | prepare
            example                  | dataset          |            |                                                 |        |
              | wsdl_resource        | wsdl             |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
              | soap_resource        | soap             |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
              |   |   |   |          | param            | parameter1 | request_model/param1                            | open   | input()
              |   |   | City         |                  | id         | /                                               | open   |
              |   |   |   | id       | integer          |            | id                                              |        |
              |   |   |   | p1       | integer required |            |                                                 |        | param(parameter1)
            """,
            mode=Mode.external,
        )
        context.set("auth.token", AdminToken())

        soap_query_builder = _get_soap_query_builder(context, manifest)
        with pytest.raises(MissingRequiredProperty, match="Property 'p1' is required."):
            soap_query_builder.build()

    def test_build_body_from_params_without_property_defined(
        self, rc: RawConfig, tmp_path: pathlib.Path, mocker: MockerFixture
    ) -> None:
        context, manifest = prepare_manifest(
            rc,
            """
            d | r | b | m | property | type    | ref        | source                                          | access | prepare
            example                  | dataset |            |                                                 |        |
              | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
              | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
              |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | creds("foo").input()
              |   |   | City         |         | id         | /                                               | open   |
              |   |   |   | id       | integer |            | id                                              |        |
            """,
            mode=Mode.external,
        )

        clients_path = get_clients_path(tmp_path)
        mocker.patch(
            "spinta.datasets.backends.dataframe.backends.soap.ufuncs.ufuncs.get_clients_path", return_value=clients_path
        )
        _set_auth_token_with_client_file(
            context, clients_path, client_backends={"soap_resource": {"foo": "foo_result"}}
        )

        soap_query_builder = _get_soap_query_builder(context, manifest)
        soap_query_builder.build()

        assert soap_query_builder.soap_request_body == {"request_model/param1": "foo_result"}
        assert soap_query_builder.property_values == {}

    def test_skip_build_body_if_soap_body_does_not_exist(
        self, rc: RawConfig, tmp_path: pathlib.Path, mocker: MockerFixture
    ) -> None:
        context, manifest = prepare_manifest(
            rc,
            """
            d | r | b | m | property | type    | ref        | source                                          | access | prepare
            example                  | dataset |            |                                                 |        |
              | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
              | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
              |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | creds("foo")
              |   |   | City         |         | id         | /                                               | open   |
              |   |   |   | id       | integer |            | id                                              |        |
            """,
            mode=Mode.external,
        )

        clients_path = get_clients_path(tmp_path)
        mocker.patch(
            "spinta.datasets.backends.dataframe.backends.soap.ufuncs.ufuncs.get_clients_path", return_value=clients_path
        )
        _set_auth_token_with_client_file(
            context, clients_path, client_backends={"soap_resource": {"foo": "foo_result"}}
        )

        soap_query_builder = _get_soap_query_builder(context, manifest)
        soap_query_builder.build()

        assert soap_query_builder.soap_request_body == {}
        assert soap_query_builder.property_values == {}

    def test_soap_request_body_values_does_not_persist_between_multiple_calls(
        self, rc: RawConfig, tmp_path: pathlib.Path, mocker: MockerFixture
    ) -> None:
        context, manifest = prepare_manifest(
            rc,
            """
        d | r | b | m | property | type    | ref        | source                                          | access | prepare
        example                  | dataset |            |                                                 |        |
          | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
          | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
          |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | creds("par1").input()
          |   |   | City         |         | id         | /                                               | open   |
          |   |   |   | id       | integer |            | id                                              |        |
          |   |   |   | p1       | integer |            |                                                 |        | param(parameter1)
        """,
            mode=Mode.external,
        )

        clients_path = get_clients_path(tmp_path)
        mocker.patch(
            "spinta.datasets.backends.dataframe.backends.soap.ufuncs.ufuncs.get_clients_path", return_value=clients_path
        )
        _set_auth_token_with_client_file(
            context, clients_path, client_backends={"soap_resource": {"par1": "cred_value"}}
        )

        query_params = QueryParams()
        query_params.url_params = {"p1": "url_value"}
        soap_query_builder = _get_soap_query_builder(context, manifest, query_params=query_params)
        soap_query_builder.build()
        assert soap_query_builder.soap_request_body == {"request_model/param1": "url_value"}
        assert soap_query_builder.property_values == {"parameter1": "url_value"}

        soap_query_builder = _get_soap_query_builder(context, manifest)
        soap_query_builder.build()
        assert soap_query_builder.soap_request_body == {"request_model/param1": "cred_value"}
        assert soap_query_builder.property_values == {"parameter1": "cred_value"}

    def test_soap_request_body_when_param_has_value_type_cdata_and_default_value_given(self, rc: RawConfig) -> None:
        context, manifest = prepare_manifest(
            rc,
            """
            d | r | b | m | property | type    | ref        | source                                          | access | prepare
            example                  | dataset |            |                                                 |        |
              | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
              | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
              |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | input('default_val').cdata()
              |   |   | City         |         | id         | /                                               | open   |
              |   |   |   | id       | integer |            | id                                              |        |
              |   |   |   | p1       | integer |            |                                                 |        | param(parameter1)
            """,
            mode=Mode.external,
        )
        context.set("auth.token", AdminToken())

        soap_query_builder = _get_soap_query_builder(context, manifest)
        soap_query_builder.build()

        dummy_element = etree.Element("foo")
        dummy_element.text = soap_query_builder.soap_request_body.get("request_model/param1")()
        assert etree.tostring(dummy_element) == b"<foo><![CDATA[default_val]]></foo>"
        assert soap_query_builder.property_values == {"parameter1": "default_val"}

    def test_soap_request_body_when_param_has_value_type_cdata_and_value_via_url_param(self, rc: RawConfig) -> None:
        context, manifest = prepare_manifest(
            rc,
            """
            d | r | b | m | property | type    | ref        | source                                          | access | prepare
            example                  | dataset |            |                                                 |        |
              | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
              | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
              |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | input('default_val').cdata()
              |   |   | City         |         | id         | /                                               | open   |
              |   |   |   | id       | integer |            | id                                              |        |
              |   |   |   | p1       | integer |            |                                                 |        | param(parameter1)
            """,
            mode=Mode.external,
        )
        context.set("auth.token", AdminToken())

        query_params = QueryParams()
        query_params.url_params = {"p1": "url_value"}
        soap_query_builder = _get_soap_query_builder(context, manifest, query_params=query_params)
        soap_query_builder.build()

        dummy_element = etree.Element("foo")
        dummy_element.text = soap_query_builder.soap_request_body.get("request_model/param1")()
        assert etree.tostring(dummy_element) == b"<foo><![CDATA[url_value]]></foo>"
        assert soap_query_builder.property_values == {"parameter1": "url_value"}

    def test_soap_request_body_when_param_has_value_type_cdata_and_value_not_given(self, rc: RawConfig) -> None:
        context, manifest = prepare_manifest(
            rc,
            """
            d | r | b | m | property | type    | ref        | source                                          | access | prepare
            example                  | dataset |            |                                                 |        |
              | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
              | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
              |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | input().cdata()
              |   |   | City         |         | id         | /                                               | open   |
              |   |   |   | id       | integer |            | id                                              |        |
              |   |   |   | p1       | integer |            |                                                 |        | param(parameter1)
            """,
            mode=Mode.external,
        )
        context.set("auth.token", AdminToken())

        soap_query_builder = _get_soap_query_builder(context, manifest)
        soap_query_builder.build()

        assert soap_query_builder.soap_request_body == {}


class TestCreds:
    def test_return_value_from_client_config_backend(
        self, rc: RawConfig, tmp_path: pathlib.Path, mocker: MockerFixture
    ) -> None:
        context, manifest = _get_wsdl_soap_manifest(rc)

        clients_path = get_clients_path(tmp_path)
        mocker.patch(
            "spinta.datasets.backends.dataframe.backends.soap.ufuncs.ufuncs.get_clients_path", return_value=clients_path
        )
        _set_auth_token_with_client_file(
            context, clients_path, client_backends={"soap_resource": {"foo": "foo_result"}}
        )

        soap_query_builder = _get_soap_query_builder(context, manifest)

        expr = asttoexpr(parse('creds("foo")'))
        assert soap_query_builder.resolve(expr) == "foo_result"

    def test_raise_error_if_resource_name_is_not_part_of_client_config_backends(
        self, rc: RawConfig, tmp_path: pathlib.Path, mocker: MockerFixture
    ) -> None:
        context, manifest = _get_wsdl_soap_manifest(rc)

        clients_path = get_clients_path(tmp_path)
        mocker.patch(
            "spinta.datasets.backends.dataframe.backends.soap.ufuncs.ufuncs.get_clients_path", return_value=clients_path
        )
        _set_auth_token_with_client_file(
            context, clients_path, client_backends={"test_resource": {"foo": "foo_result"}}
        )

        soap_query_builder = _get_soap_query_builder(context, manifest)

        expr = asttoexpr(parse('creds("foo")'))
        with pytest.raises(InvalidClientBackend):
            soap_query_builder.resolve(expr)

    def test_raise_error_if_credential_key_is_not_defined_in_client_config_backend(
        self, rc: RawConfig, tmp_path: pathlib.Path, mocker: MockerFixture
    ) -> None:
        context, manifest = _get_wsdl_soap_manifest(rc)

        clients_path = get_clients_path(tmp_path)
        mocker.patch(
            "spinta.datasets.backends.dataframe.backends.soap.ufuncs.ufuncs.get_clients_path", return_value=clients_path
        )
        _set_auth_token_with_client_file(
            context, clients_path, client_backends={"soap_resource": {"bar": "bar_result"}}
        )

        soap_query_builder = _get_soap_query_builder(context, manifest)

        expr = asttoexpr(parse('creds("foo")'))
        with pytest.raises(InvalidClientBackendCredentials):
            soap_query_builder.resolve(expr)

    def test_raise_error_if_creds_given_without_key(
        self, rc: RawConfig, tmp_path: pathlib.Path, mocker: MockerFixture
    ) -> None:
        context, manifest = _get_wsdl_soap_manifest(rc)

        clients_path = get_clients_path(tmp_path)
        mocker.patch(
            "spinta.datasets.backends.dataframe.backends.soap.ufuncs.ufuncs.get_clients_path", return_value=clients_path
        )
        _set_auth_token_with_client_file(
            context, clients_path, client_backends={"soap_resource": {"bar": "bar_result"}}
        )

        soap_query_builder = _get_soap_query_builder(context, manifest)

        expr = asttoexpr(parse("creds()"))
        with pytest.raises(UnknownMethod, match="Unknown method 'creds' with args"):
            soap_query_builder.resolve(expr)
