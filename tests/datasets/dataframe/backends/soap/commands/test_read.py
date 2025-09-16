from unittest.mock import ANY

import pytest
from pytest_mock import MockerFixture
from responses import RequestsMock, POST

from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.datasets.backends.dataframe.backends.soap.commands.read import _expand_dict_keys
from spinta.exceptions import SoapRequestBodyParseError
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import prepare_manifest
from spinta.testing.utils import get_error_codes

WSDL_SOAP_PARAM_MANIFEST = """
    d | r | b | m | property | type    | ref        | source                                          | access | prepare
    example                  | dataset |            |                                                 |        |
      | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
      | soap_resource        | soap    |            | {source}                                        |        | wsdl(wsdl_resource)
      |   |   |   |          | param   | parameter1 | request_model/param1                            | open   | input('default_val')
      |   |   |   |          | param   | parameter2 | request_model/param2                            | open   | input('default_val')
      |   |   | City         |         | id         | /                                               | open   |
      |   |   |   | id       | integer |            | id                                              |        |
      |   |   |   | p1       | integer |            |                                                 |        | param(parameter1)
      |   |   |   | p2       | integer |            |                                                 |        | param(parameter2)
"""


class TestExpandDictKeys:
    @pytest.mark.parametrize(
        "target, result",
        [
            ({}, {}),
            ({"a": "result"}, {"a": "result"}),
            ({"a/b/c": "result"}, {"a": {"b": {"c": "result"}}}),
            (
                {
                    "a/b/c": "result",
                    "a/b/d": "result2",
                    "a/e": "result3",
                    "f": "result4",
                },
                {
                    "a": {
                        "b": {
                            "c": "result",
                            "d": "result2",
                        },
                        "e": "result3",
                    },
                    "f": "result4",
                },
            ),
        ],
    )
    def test_expand_keys(self, target: dict, result: dict) -> None:
        assert _expand_dict_keys(target) == result

    @pytest.mark.parametrize(
        "target",
        [
            {"a/b": "result", "a": "result2"},
            {"a": "result2", "a/b": "result"},
            {"a/b/c": "result", "a/b": "result2"},
            {"a/b": "result2", "a/b/c": "result"},
            ({"a/": "result2"}),
        ],
    )
    def test_raise_error_when_impossible_to_expand_keys(self, target: dict) -> None:
        with pytest.raises(ValueError):
            _expand_dict_keys(target)


def test_soap_read_calls_soap_operation_with_empty_request_body(rc: RawConfig, mocker: MockerFixture) -> None:
    soap_data_mock = mocker.patch(
        "spinta.datasets.backends.dataframe.backends.soap.commands.read._get_data_soap",
        return_value=[],
    )
    soap_data_mock.configure_mock(
        __dask_tokenize__=lambda: ("_get_data_soap", "empty"),
    )

    source = "CityService.CityPort.CityPortType.CityOperation"
    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property | type    | ref | source                                     | access | prepare
        example                  | dataset |     |                                            |        |
          | wsdl_resource        | wsdl    |     | tests/datasets/backends/wsdl/data/wsdl.xml |        |
          | soap_resource        | soap    |     | {source}                                   |        | wsdl(wsdl_resource)
          |   |   | City         |         | id  | /                                          | open   |
          |   |   |   | id       | integer |     | id                                         |        |
        """,
        mode=Mode.external,
    )

    context.loaded = True
    app = create_test_client(context)
    app.authmodel("/example/City/", ["getall"])
    app.get("/example/City/")

    soap_data_mock.assert_called_with(source, backend=ANY, soap_request={})


def test_soap_read_calls_soap_operation_with_default_request_body_values(rc: RawConfig, mocker: MockerFixture) -> None:
    soap_data_mock = mocker.patch(
        "spinta.datasets.backends.dataframe.backends.soap.commands.read._get_data_soap",
        return_value=[],
    )
    soap_data_mock.configure_mock(
        __dask_tokenize__=lambda: ("_get_data_soap", "empty"),
    )

    source = "CityService.CityPort.CityPortType.CityOperation"
    context, manifest = prepare_manifest(rc, WSDL_SOAP_PARAM_MANIFEST.format(source=source), mode=Mode.external)

    context.loaded = True
    app = create_test_client(context)
    app.authmodel("/example/City/", ["getall"])
    app.get("/example/City/")

    expected_soap_request = {
        "request_model": {
            "param1": "default_val",
            "param2": "default_val",
        },
    }
    soap_data_mock.assert_called_with(source, backend=ANY, soap_request=expected_soap_request)


def test_soap_read_calls_soap_operation_with_request_body_values_from_url(rc: RawConfig, mocker: MockerFixture) -> None:
    soap_data_mock = mocker.patch(
        "spinta.datasets.backends.dataframe.backends.soap.commands.read._get_data_soap",
        return_value=[],
    )
    soap_data_mock.configure_mock(
        __dask_tokenize__=lambda: ("_get_data_soap", "empty"),
    )

    source = "CityService.CityPort.CityPortType.CityOperation"
    context, manifest = prepare_manifest(rc, WSDL_SOAP_PARAM_MANIFEST.format(source=source), mode=Mode.external)

    context.loaded = True
    app = create_test_client(context)
    app.authmodel("/example/City/", ["getall", "search"])
    app.get("/example/City/?p1='foo'&p2='bar'")

    expected_soap_request = {
        "request_model": {
            "param1": "foo",
            "param2": "bar",
        },
    }
    soap_data_mock.assert_called_with(source, backend=ANY, soap_request=expected_soap_request)


def test_soap_read_raise_error_if_manifest_resource_param_source_cannot_be_parsed(rc: RawConfig) -> None:
    context, manifest = prepare_manifest(
        rc,
        """
        d | r | b | m | property | type    | ref        | source                                          | access | prepare
        example                  | dataset |            |                                                 |        |
          | wsdl_resource        | wsdl    |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
          | soap_resource        | soap    |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
          |   |   |   |          | param   | parameter1 | request_model                                   | open   | input('default_val')
          |   |   |   |          | param   | parameter2 | request_model/param2                            | open   | input('default_val')
          |   |   | City         |         | id         | /                                               | open   |
          |   |   |   | id       | integer |            | id                                              |        |
          |   |   |   | p1       | integer |            |                                                 |        | param(parameter1)
          |   |   |   | p2       | integer |            |                                                 |        | param(parameter2)
        """,
        mode=Mode.external,
    )

    context.loaded = True
    app = create_test_client(context)
    app.authmodel("/example/City/", ["getall", "search"])

    with pytest.raises(SoapRequestBodyParseError):
        app.get("/example/City/?p1='foo'&p2='bar'")


def test_soap_read_http_404_if_url_param_does_not_exist_as_property(rc: RawConfig) -> None:
    source = "CityService.CityPort.CityPortType.CityOperation"
    context, manifest = prepare_manifest(rc, WSDL_SOAP_PARAM_MANIFEST.format(source=source), mode=Mode.external)

    context.loaded = True
    app = create_test_client(context)
    app.authmodel("/example/City/", ["getall", "search"])

    response = app.get("/example/City/?p3='foo'")
    assert response.status_code == 404
    assert get_error_codes(response.json()) == ["PropertyNotFound"]


def test_soap_read(rc: RawConfig, responses: RequestsMock) -> None:
    endpoint_url = "http://example.com/city"
    soap_response = """
        <ns0:Envelope xmlns:ns0="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="city_app">
            <ns0:Body>
                <ns1:CityOutputResponse>
                    <ns1:CityOutput>
                        <ns1:id>100</ns1:id>
                        <ns1:name>Name One</ns1:name>
                    </ns1:CityOutput>
                    <ns1:CityOutput>
                        <ns1:id>101</ns1:id>
                        <ns1:name>Name Two</ns1:name>
                    </ns1:CityOutput>
                </ns1:CityOutputResponse>
            </ns0:Body>
        </ns0:Envelope>
    """
    responses.add(POST, endpoint_url, status=200, content_type="text/plain; charset=utf-8", body=soap_response)

    context, manifest = prepare_manifest(
        rc,
        """
        d | r | b | m | property | type    | ref | source                                          | access | prepare
        example                  | dataset |     |                                                 |        |
          | wsdl_resource        | wsdl    |     | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
          | soap_resource        | soap    |     | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
          |   |   | City         |         | id  | /                                               | open   |
          |   |   |   | id       | integer |     | id                                              |        |
          |   |   |   | name     | string  |     | name                                            |        |
        """,
        mode=Mode.external,
    )

    context.loaded = True
    app = create_test_client(context)
    app.authmodel("/example/City/", ["getall"])

    response = app.get("/example/City/")
    assert listdata(response, sort=False, full=True) == [
        {
            "id": 100,
            "name": "Name One",
        },
        {
            "id": 101,
            "name": "Name Two",
        },
    ]


def test_soap_read_with_default_soap_request_params(rc: RawConfig, responses: RequestsMock) -> None:
    endpoint_url = "http://example.com/city"
    soap_response = """
        <ns0:Envelope xmlns:ns0="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="city_app">
            <ns0:Body>
                <ns1:CityOutputResponse>
                    <ns1:CityOutput>
                        <ns1:id>100</ns1:id>
                    </ns1:CityOutput>
                    <ns1:CityOutput>
                        <ns1:id>101</ns1:id>
                    </ns1:CityOutput>
                </ns1:CityOutputResponse>
            </ns0:Body>
        </ns0:Envelope>
    """
    responses.add(POST, endpoint_url, status=200, content_type="text/plain; charset=utf-8", body=soap_response)

    source = "CityService.CityPort.CityPortType.CityOperation"
    context, manifest = prepare_manifest(rc, WSDL_SOAP_PARAM_MANIFEST.format(source=source), mode=Mode.external)

    context.loaded = True
    app = create_test_client(context)
    app.authmodel("/example/City/", ["getall"])

    response = app.get("/example/City/")

    assert listdata(response, sort=False, full=True) == [
        {
            "id": 100,
            "p1": "default_val",
            "p2": "default_val",
        },
        {
            "id": 101,
            "p1": "default_val",
            "p2": "default_val",
        },
    ]


def test_soap_read_with_soap_request_params_from_url(rc: RawConfig, responses: RequestsMock) -> None:
    endpoint_url = "http://example.com/city"
    soap_response = """
        <ns0:Envelope xmlns:ns0="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="city_app">
            <ns0:Body>
                <ns1:CityOutputResponse>
                    <ns1:CityOutput>
                        <ns1:id>100</ns1:id>
                    </ns1:CityOutput>
                    <ns1:CityOutput>
                        <ns1:id>101</ns1:id>
                    </ns1:CityOutput>
                </ns1:CityOutputResponse>
            </ns0:Body>
        </ns0:Envelope>
    """
    responses.add(POST, endpoint_url, status=200, content_type="text/plain; charset=utf-8", body=soap_response)

    source = "CityService.CityPort.CityPortType.CityOperation"
    context, manifest = prepare_manifest(rc, WSDL_SOAP_PARAM_MANIFEST.format(source=source), mode=Mode.external)

    context.loaded = True
    app = create_test_client(context)
    app.authmodel("/example/City/", ["getall", "search"])

    response = app.get("/example/City/?p1='foo'&p2='bar'")

    assert listdata(response, sort=False, full=True) == [
        {
            "id": 100,
            "p1": "foo",
            "p2": "bar",
        },
        {
            "id": 101,
            "p1": "foo",
            "p2": "bar",
        },
    ]
