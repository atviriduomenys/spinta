import pytest
from responses import RequestsMock, POST

from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.exceptions import (
    ReservedPropertyTypeShouldMatchPrimaryKey,
    ReservedPropertySourceOrModelRefShouldBeSet,
    Base32TypeOnlyAllowedOnId,
)
from spinta.testing.client import create_test_client
from spinta.testing.manifest import prepare_manifest


WSDL_SOURCE = "tests/datasets/backends/wsdl/data/order_wsdl.xml"
SOAP_SOURCE = "OrderService.OrderPort.OrderPortType.OrderOperation"
ENDPOINT_URL = "http://example.com/order"

SOAP_RESPONSE = """
    <ns0:Envelope xmlns:ns0="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="order_app">
        <ns0:Body>
            <ns1:OrderOutputResponse>
                <ns1:OrderOutput>
                    <ns1:code>ORD001</ns1:code>
                    <ns1:id>123</ns1:id>
                    <ns1:uuid_id>d6420786-082f-4ee4-9624-7a559f31d032</ns1:uuid_id>
                    <ns1:comma_code>OR,D001</ns1:comma_code>
                </ns1:OrderOutput>
                <ns1:OrderOutput>
                    <ns1:code>ORD002</ns1:code>
                    <ns1:id>1234</ns1:id>
                    <ns1:uuid_id>8f5773d6-a5eb-4409-8f88-aca874e27200</ns1:uuid_id>
                    <ns1:comma_code>OR,D002</ns1:comma_code>
                </ns1:OrderOutput>
            </ns1:OrderOutputResponse>
        </ns0:Body>
    </ns0:Envelope>
"""

REGION_SOAP_SOURCE = "OrderService.OrderPort.OrderPortType.OrderOperation"
CITY_SOAP_SOURCE = "CityService.CityPort.CityPortType.CityOperation"
REGION_ENDPOINT_URL = "http://example.com/order"
CITY_ENDPOINT_URL = "http://example.com/city"

CITY_SOAP_RESPONSE = """
    <ns0:Envelope xmlns:ns0="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="order_app">
        <ns0:Body>
            <ns1:CityOutputResponse>
                <ns1:CityOutput>
                    <ns1:id>1</ns1:id>
                    <ns1:region_code>ORD001</ns1:region_code>
                    <ns1:region_id>123</ns1:region_id>
                    <ns1:region_uuid>d6420786-082f-4ee4-9624-7a559f31d032</ns1:region_uuid>
                </ns1:CityOutput>
                <ns1:CityOutput>
                    <ns1:id>2</ns1:id>
                    <ns1:region_code>ORD002</ns1:region_code>
                    <ns1:region_id>1234</ns1:region_id>
                    <ns1:region_uuid>8f5773d6-a5eb-4409-8f88-aca874e27200</ns1:region_uuid>
                </ns1:CityOutput>
            </ns1:CityOutputResponse>
        </ns0:Body>
    </ns0:Envelope>
"""


class TestIdStr:
    def test_for_id_string_with_string(self, rc: RawConfig, responses: RequestsMock):
        responses.add(POST, ENDPOINT_URL, status=200, content_type="text/plain; charset=utf-8", body=SOAP_RESPONSE)

        context, manifest = prepare_manifest(
            rc,
            f"""
            d | r | b | m | property | type    | ref  | source        | access | prepare
            example                  |         |      |               |        |
              | wsdl_resource        | wsdl    |      | {WSDL_SOURCE} |        |
              | soap_resource        | soap    |      | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
              |   |   | Region       |         | code | /             |        |
              |   |   |   | code     | string  |      | code          | open   |
              |   |   |   | _id      | string  |      |               | open   |
            """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]
        assert first_row_data["code"]["value"] == "ORD001"
        assert second_row_data["code"]["value"] == "ORD002"
        assert first_row_data["_id"]["value"] == "ORD001"
        assert second_row_data["_id"]["value"] == "ORD002"
        assert first_row_data["_id"]["link"] == "/example/Region/=ORD001"
        assert second_row_data["_id"]["link"] == "/example/Region/=ORD002"

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])
            # Expected, SOAP does not support getone operations

    def test_for_id_string_with_integer(self, rc: RawConfig, responses: RequestsMock):
        responses.add(POST, ENDPOINT_URL, status=200, content_type="text/plain; charset=utf-8", body=SOAP_RESPONSE)

        context, manifest = prepare_manifest(
            rc,
            f"""
            d | r | b | m | property | type    | ref | source        | access | prepare
            example                  |         |     |               |        |
              | wsdl_resource        | wsdl    |     | {WSDL_SOURCE} |        |
              | soap_resource        | soap    |     | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
              |   |   | Region       |         | id  | /             |        |
              |   |   |   | id       | integer |     | id            | open   |
              |   |   |   | _id      | string  |     |               | open   |
            """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]
        assert first_row_data["id"]["value"] == 123
        assert second_row_data["id"]["value"] == 1234
        assert first_row_data["_id"]["value"] == "123"
        assert second_row_data["_id"]["value"] == "1234"
        assert first_row_data["_id"]["link"] == "/example/Region/=123"
        assert second_row_data["_id"]["link"] == "/example/Region/=1234"

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])

    def test_for_id_string_with_uuid(self, rc: RawConfig, responses: RequestsMock):
        responses.add(POST, ENDPOINT_URL, status=200, content_type="text/plain; charset=utf-8", body=SOAP_RESPONSE)

        context, manifest = prepare_manifest(
            rc,
            f"""
            d | r | b | m | property | type    | ref     | source        | access | prepare
            example                  |         |         |               |        |
              | wsdl_resource        | wsdl    |         | {WSDL_SOURCE} |        |
              | soap_resource        | soap    |         | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
              |   |   | Region       |         | uuid_id | /             |        |
              |   |   |   | uuid_id  | uuid    |         | uuid_id       | open   |
              |   |   |   | _id      | string  |         |               | open   |
            """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]
        assert first_row_data["uuid_id"]["value"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert second_row_data["uuid_id"]["value"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert first_row_data["_id"]["value"] == "d6420786"
        assert second_row_data["_id"]["value"] == "8f5773d6"
        assert first_row_data["_id"]["link"] == "/example/Region/=d6420786-082f-4ee4-9624-7a559f31d032"
        assert second_row_data["_id"]["link"] == "/example/Region/=8f5773d6-a5eb-4409-8f88-aca874e27200"

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])

    def test_for_id_string_with_string_correct_equal_sign(self, rc: RawConfig, responses: RequestsMock):
        responses.add(POST, ENDPOINT_URL, status=200, content_type="text/plain; charset=utf-8", body=SOAP_RESPONSE)

        context, manifest = prepare_manifest(
            rc,
            f"""
            d | r | b | m | property | type    | ref  | source        | access | prepare
            example                  |         |      |               |        |
              | wsdl_resource        | wsdl    |      | {WSDL_SOURCE} |        |
              | soap_resource        | soap    |      | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
              |   |   | Region       |         | code | /             |        |
              |   |   |   | code     | string  |      | comma_code    | open   |
              |   |   |   | _id      | string  |      |               | open   |
            """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]
        assert first_row_data["code"]["value"] == "OR,D001"
        assert second_row_data["code"]["value"] == "OR,D002"
        assert first_row_data["_id"]["value"] == "OR,D001"
        assert second_row_data["_id"]["value"] == "OR,D002"
        assert first_row_data["_id"]["link"] == "/example/Region/=OR,D001"
        assert second_row_data["_id"]["link"] == "/example/Region/=OR,D002"

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])


class TestIdInt:
    def test_for_id_integer(self, rc: RawConfig, responses: RequestsMock):
        responses.add(POST, ENDPOINT_URL, status=200, content_type="text/plain; charset=utf-8", body=SOAP_RESPONSE)

        context, manifest = prepare_manifest(
            rc,
            f"""
            d | r | b | m | property | type    | ref | source        | access | prepare
            example                  |         |     |               |        |
              | wsdl_resource        | wsdl    |     | {WSDL_SOURCE} |        |
              | soap_resource        | soap    |     | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
              |   |   | Region       |         | id  | /             |        |
              |   |   |   | id       | integer |     | id            | open   |
              |   |   |   | _id      | integer |     |               | open   |
            """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]
        assert first_row_data["id"]["value"] == 123
        assert second_row_data["id"]["value"] == 1234
        assert first_row_data["_id"]["value"] == 123
        assert second_row_data["_id"]["value"] == 1234
        assert first_row_data["_id"]["link"] == "/example/Region/123"
        assert second_row_data["_id"]["link"] == "/example/Region/1234"

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])

    def test_for_id_integer_errors_with_string(self, rc: RawConfig):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                f"""
                d | r | b | m | property | type    | ref | source        | access | prepare
                example                  |         |     |               |        |
                  | wsdl_resource        | wsdl    |     | {WSDL_SOURCE} |        |
                  | soap_resource        | soap    |     | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
                  |   |   | Region       |         | id  | /             |        |
                  |   |   |   | id       | string  |     | id            | open   |
                  |   |   |   | _id      | integer |     |               | open   |
                """,
                mode=Mode.external,
            )

    def test_for_id_integer_errors_with_uuid(self, rc: RawConfig):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                f"""
                d | r | b | m | property | type    | ref | source        | access | prepare
                example                  |         |     |               |        |
                  | wsdl_resource        | wsdl    |     | {WSDL_SOURCE} |        |
                  | soap_resource        | soap    |     | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
                  |   |   | Region       |         | id  | /             |        |
                  |   |   |   | id       | uuid    |     | id            | open   |
                  |   |   |   | _id      | integer |     |               | open   |
                """,
                mode=Mode.external,
            )


class TestIdUuid:
    def test_for_id_uuid(self, rc: RawConfig, responses: RequestsMock):
        responses.add(POST, ENDPOINT_URL, status=200, content_type="text/plain; charset=utf-8", body=SOAP_RESPONSE)

        context, manifest = prepare_manifest(
            rc,
            f"""
            d | r | b | m | property | type    | ref     | source        | access | prepare
            example                  |         |         |               |        |
              | wsdl_resource        | wsdl    |         | {WSDL_SOURCE} |        |
              | soap_resource        | soap    |         | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
              |   |   | Region       |         | uuid_id | /             |        |
              |   |   |   | uuid_id  | uuid    |         | uuid_id       | open   |
              |   |   |   | _id      | uuid    |         |               | open   |
            """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]
        assert first_row_data["uuid_id"]["value"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert second_row_data["uuid_id"]["value"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert first_row_data["_id"]["value"] == "d6420786"
        assert second_row_data["_id"]["value"] == "8f5773d6"
        assert first_row_data["_id"]["link"] == "/example/Region/d6420786-082f-4ee4-9624-7a559f31d032"
        assert second_row_data["_id"]["link"] == "/example/Region/8f5773d6-a5eb-4409-8f88-aca874e27200"

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])

    def test_for_id_uuid_errors_with_string(self, rc: RawConfig):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                f"""
                d | r | b | m | property | type    | ref | source        | access | prepare
                example                  |         |     |               |        |
                  | wsdl_resource        | wsdl    |     | {WSDL_SOURCE} |        |
                  | soap_resource        | soap    |     | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
                  |   |   | Region       |         | id  | /             |        |
                  |   |   |   | id       | string  |     | id            | open   |
                  |   |   |   | _id      | uuid    |     |               | open   |
                """,
                mode=Mode.external,
            )

    def test_for_id_uuid_errors_with_integer(self, rc: RawConfig):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                f"""
                d | r | b | m | property | type    | ref | source        | access | prepare
                example                  |         |     |               |        |
                  | wsdl_resource        | wsdl    |     | {WSDL_SOURCE} |        |
                  | soap_resource        | soap    |     | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
                  |   |   | Region       |         | id  | /             |        |
                  |   |   |   | id       | integer |     | id            | open   |
                  |   |   |   | _id      | uuid    |     |               | open   |
                """,
                mode=Mode.external,
            )


class TestIdComp:
    def test_for_id_comp(self, rc: RawConfig, responses: RequestsMock):
        responses.add(POST, ENDPOINT_URL, status=200, content_type="text/plain; charset=utf-8", body=SOAP_RESPONSE)

        context, manifest = prepare_manifest(
            rc,
            f"""
            d | r | b | m | property | type    | ref      | source        | access | prepare
            example                  |         |          |               |        |
              | wsdl_resource        | wsdl    |          | {WSDL_SOURCE} |        |
              | soap_resource        | soap    |          | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
              |   |   | Region       |         | id, code | /             |        |
              |   |   |   | code     | string  |          | code          | open   |
              |   |   |   | _id      | string  |          |               | open   |
              |   |   |   | id       | integer |          | id            | open   |
            """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]
        assert first_row_data["code"]["value"] == "ORD001"
        assert second_row_data["code"]["value"] == "ORD002"
        assert first_row_data["id"]["value"] == 123
        assert second_row_data["id"]["value"] == 1234
        assert first_row_data["_id"]["value"] == "123,ORD0"
        assert second_row_data["_id"]["value"] == "1234,ORD"
        assert first_row_data["_id"]["link"] == "/example/Region/123,ORD001"
        assert second_row_data["_id"]["link"] == "/example/Region/1234,ORD002"

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])

    def test_for_id_comp_error_with_integer(self, rc: RawConfig):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                f"""
                d | r | b | m | property | type    | ref      | source        | access | prepare
                example                  |         |          |               |        |
                  | wsdl_resource        | wsdl    |          | {WSDL_SOURCE} |        |
                  | soap_resource        | soap    |          | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
                  |   |   | Region       |         | id, code | /             |        |
                  |   |   |   | code     | string  |          | code          | open   |
                  |   |   |   | _id      | integer |          |               | open   |
                  |   |   |   | id       | integer |          | id            | open   |
                """,
                mode=Mode.external,
            )

    def test_for_id_comp_error_with_uuid(self, rc: RawConfig):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                f"""
                d | r | b | m | property | type    | ref      | source        | access | prepare
                example                  |         |          |               |        |
                  | wsdl_resource        | wsdl    |          | {WSDL_SOURCE} |        |
                  | soap_resource        | soap    |          | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
                  |   |   | Region       |         | id, code | /             |        |
                  |   |   |   | code     | string  |          | code          | open   |
                  |   |   |   | _id      | uuid    |          |               | open   |
                  |   |   |   | id       | integer |          | id            | open   |
                """,
                mode=Mode.external,
            )


class TestIdBase:
    def test_for_id_base32_with_string(self, rc: RawConfig, responses: RequestsMock):
        responses.add(POST, ENDPOINT_URL, status=200, content_type="text/plain; charset=utf-8", body=SOAP_RESPONSE)

        context, manifest = prepare_manifest(
            rc,
            f"""
            d | r | b | m | property | type    | ref  | source        | access | prepare
            example                  |         |      |               |        |
              | wsdl_resource        | wsdl    |      | {WSDL_SOURCE} |        |
              | soap_resource        | soap    |      | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
              |   |   | Region       |         | code | /             |        |
              |   |   |   | code     | string  |      | code          | open   |
              |   |   |   | _id      | base32  |      |               | open   |
            """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]
        assert first_row_data["code"]["value"] == "ORD001"
        assert second_row_data["code"]["value"] == "ORD002"
        assert first_row_data["_id"]["value"] == "J5JEIMBQ"
        assert second_row_data["_id"]["value"] == "J5JEIMBQ"
        assert first_row_data["_id"]["link"] == "/example/Region/=J5JEIMBQGE"
        assert second_row_data["_id"]["link"] == "/example/Region/=J5JEIMBQGI"

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])

    def test_for_id_base32_with_integer(self, rc: RawConfig, responses: RequestsMock):
        responses.add(POST, ENDPOINT_URL, status=200, content_type="text/plain; charset=utf-8", body=SOAP_RESPONSE)

        context, manifest = prepare_manifest(
            rc,
            f"""
            d | r | b | m | property | type    | ref | source        | access | prepare
            example                  |         |     |               |        |
              | wsdl_resource        | wsdl    |     | {WSDL_SOURCE} |        |
              | soap_resource        | soap    |     | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
              |   |   | Region       |         | id  | /             |        |
              |   |   |   | _id      | base32  |     |               | open   |
              |   |   |   | id       | integer |     | id            | open   |
            """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]
        assert first_row_data["id"]["value"] == 123
        assert second_row_data["id"]["value"] == 1234
        assert first_row_data["_id"]["value"] == "GEZDG"
        assert second_row_data["_id"]["value"] == "GEZDGNA"
        assert first_row_data["_id"]["link"] == "/example/Region/=GEZDG"
        assert second_row_data["_id"]["link"] == "/example/Region/=GEZDGNA"

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])

    def test_for_id_base32_with_uuid(self, rc: RawConfig, responses: RequestsMock):
        responses.add(POST, ENDPOINT_URL, status=200, content_type="text/plain; charset=utf-8", body=SOAP_RESPONSE)

        context, manifest = prepare_manifest(
            rc,
            f"""
            d | r | b | m | property | type    | ref     | source        | access | prepare
            example                  |         |         |               |        |
              | wsdl_resource        | wsdl    |         | {WSDL_SOURCE} |        |
              | soap_resource        | soap    |         | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
              |   |   | Region       |         | uuid_id | /             |        |
              |   |   |   | _id      | base32  |         |               | open   |
              |   |   |   | uuid_id  | uuid    |         | uuid_id       | open   |
            """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]
        assert first_row_data["uuid_id"]["value"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert second_row_data["uuid_id"]["value"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert first_row_data["_id"]["value"] == "MQ3DIMRQ"
        assert second_row_data["_id"]["value"] == "HBTDKNZX"
        assert (
            first_row_data["_id"]["link"]
            == "/example/Region/=MQ3DIMRQG44DMLJQHAZGMLJUMVSTILJZGYZDILJXME2TKOLGGMYWIMBTGI"
        )
        assert (
            second_row_data["_id"]["link"]
            == "/example/Region/=HBTDKNZXGNSDMLLBGVSWELJUGQYDSLJYMY4DQLLBMNQTQNZUMUZDOMRQGA"
        )

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])

    def test_for_id_base32_with_composite(self, rc: RawConfig, responses: RequestsMock):
        responses.add(POST, ENDPOINT_URL, status=200, content_type="text/plain; charset=utf-8", body=SOAP_RESPONSE)

        context, manifest = prepare_manifest(
            rc,
            f"""
            d | r | b | m | property | type    | ref          | source        | access | prepare
            example                  |         |              |               |        |
              | wsdl_resource        | wsdl    |              | {WSDL_SOURCE} |        |
              | soap_resource        | soap    |              | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
              |   |   | Region       |         | uuid_id, code | /            |        |
              |   |   |   | code     | string  |              | code          | open   |
              |   |   |   | _id      | base32  |              |               | open   |
              |   |   |   | uuid_id  | uuid    |              | uuid_id       | open   |
            """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]
        assert first_row_data["uuid_id"]["value"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert second_row_data["uuid_id"]["value"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert first_row_data["code"]["value"] == "ORD001"
        assert second_row_data["code"]["value"] == "ORD002"
        assert first_row_data["_id"]["value"] == "QJ4CIZBW"
        assert second_row_data["_id"]["value"] == "QJ4CIODG"
        assert first_row_data["_id"]["link"] == (
            "/example/Region/=QJ4CIZBWGQZDANZYGYWTAOBSMYWTIZLFGQWTSNRSGQWTOYJVGU4WMMZRMQYDGMTGJ5JEIMBQGE"
        )
        assert second_row_data["_id"]["link"] == (
            "/example/Region/=QJ4CIODGGU3TOM3EGYWWCNLFMIWTINBQHEWTQZRYHAWWCY3BHA3TIZJSG4ZDAMDGJ5JEIMBQGI"
        )

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])


class TestManifestLoading:
    def test_error_if_model_ref_and_source_empty(self, rc: RawConfig):
        with pytest.raises(ReservedPropertySourceOrModelRefShouldBeSet):
            prepare_manifest(
                rc,
                f"""
                d | r | b | m | property | type    | ref | source        | access | prepare
                example                  |         |     |               |        |
                  | wsdl_resource        | wsdl    |     | {WSDL_SOURCE} |        |
                  | soap_resource        | soap    |     | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
                  |   |   | Region       |         |     | /             |        |
                  |   |   |   | _id      | base32  |     |               | open   |
                """,
                mode=Mode.external,
            )

    def test_error_if_model_ref_and_source_populated(self, rc: RawConfig):
        with pytest.raises(ReservedPropertySourceOrModelRefShouldBeSet):
            prepare_manifest(
                rc,
                f"""
                d | r | b | m | property | type    | ref | source        | access | prepare
                example                  |         |     |               |        |
                  | wsdl_resource        | wsdl    |     | {WSDL_SOURCE} |        |
                  | soap_resource        | soap    |     | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
                  |   |   | Region       |         | id  | /             |        |
                  |   |   |   | _id      | base32  |     | id            | open   |
                  |   |   |   | id       | integer |     |               | open   |
                """,
                mode=Mode.external,
            )

    def test_for_id_uuid_works_with_string_if_source_set(self, rc: RawConfig, responses: RequestsMock):
        responses.add(POST, ENDPOINT_URL, status=200, content_type="text/plain; charset=utf-8", body=SOAP_RESPONSE)

        context, manifest = prepare_manifest(
            rc,
            f"""
            d | r | b | m | property | type    | ref | source        | access | prepare
            example                  |         |     |               |        |
              | wsdl_resource        | wsdl    |     | {WSDL_SOURCE} |        |
              | soap_resource        | soap    |     | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
              |   |   | Region       |         |     | /             |        |
              |   |   |   | uuid_id  | uuid    |     | uuid_id       | open   |
              |   |   |   | _id      | uuid    |     | uuid_id       | open   |
            """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]
        assert first_row_data["uuid_id"]["value"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert second_row_data["uuid_id"]["value"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert first_row_data["_id"]["value"] == "d6420786"
        assert second_row_data["_id"]["value"] == "8f5773d6"
        assert first_row_data["_id"]["link"] == "/example/Region/d6420786-082f-4ee4-9624-7a559f31d032"
        assert second_row_data["_id"]["link"] == "/example/Region/8f5773d6-a5eb-4409-8f88-aca874e27200"

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])

    def test_only_id_can_have_base32_type(self, rc: RawConfig):
        with pytest.raises(Base32TypeOnlyAllowedOnId):
            prepare_manifest(
                rc,
                f"""
                d | r | b | m | property | type    | ref | source        | access | prepare
                example                  |         |     |               |        |
                  | wsdl_resource        | wsdl    |     | {WSDL_SOURCE} |        |
                  | soap_resource        | soap    |     | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
                  |   |   | Region       |         | id  | /             |        |
                  |   |   |   | _id      | base32  |     |               | open   |
                  |   |   |   | id       | base32  |     |               | open   |
                """,
                mode=Mode.external,
            )

    def test_composite_values_cant_have_commas(self, rc: RawConfig, responses: RequestsMock):
        responses.add(POST, ENDPOINT_URL, status=200, content_type="text/plain; charset=utf-8", body=SOAP_RESPONSE)

        context, manifest = prepare_manifest(
            rc,
            f"""
            d | r | b | m | property | type    | ref          | source        | access | prepare
            example                  |         |              |               |        |
              | wsdl_resource        | wsdl    |              | {WSDL_SOURCE} |        |
              | soap_resource        | soap    |              | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
              |   |   | Region       |         | uuid_id, code | /            |        |
              |   |   |   | code     | string  |              | comma_code    | open   |
              |   |   |   | _id      | string  |              |               | open   |
              |   |   |   | uuid_id  | uuid    |              | uuid_id       | open   |
            """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 500
        assert resp.json()["errors"][0]["code"] == "ValuesForIdCantHaveSpecialSymbols"

    def test_composite_values_with_base32_can_have_commas(self, rc: RawConfig, responses: RequestsMock):
        responses.add(POST, ENDPOINT_URL, status=200, content_type="text/plain; charset=utf-8", body=SOAP_RESPONSE)

        context, manifest = prepare_manifest(
            rc,
            f"""
            d | r | b | m | property | type    | ref          | source        | access | prepare
            example                  |         |              |               |        |
              | wsdl_resource        | wsdl    |              | {WSDL_SOURCE} |        |
              | soap_resource        | soap    |              | {SOAP_SOURCE} |        | wsdl(wsdl_resource)
              |   |   | Region       |         | uuid_id, code | /            |        |
              |   |   |   | code     | string  |              | comma_code    | open   |
              |   |   |   | _id      | base32  |              |               | open   |
              |   |   |   | uuid_id  | uuid    |              | uuid_id       | open   |
            """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]
        assert first_row_data["_id"]["value"] == "QJ4CIZBW"
        assert second_row_data["_id"]["value"] == "QJ4CIODG"
        assert first_row_data["code"]["value"] == "OR,D001"
        assert second_row_data["code"]["value"] == "OR,D002"
        assert (
            first_row_data["_id"]["link"]
            == "/example/Region/=QJ4CIZBWGQZDANZYGYWTAOBSMYWTIZLFGQWTSNRSGQWTOYJVGU4WMMZRMQYDGMTHJ5JCYRBQGAYQ"
        )
        assert (
            second_row_data["_id"]["link"]
            == "/example/Region/=QJ4CIODGGU3TOM3EGYWWCNLFMIWTINBQHEWTQZRYHAWWCY3BHA3TIZJSG4ZDAMDHJ5JCYRBQGAZA"
        )

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])


class TestIdRef:
    def test_ref_to_string_id_model(self, rc: RawConfig, responses: RequestsMock):
        responses.add(
            POST, REGION_ENDPOINT_URL, status=200, content_type="text/plain; charset=utf-8", body=SOAP_RESPONSE
        )
        responses.add(
            POST, CITY_ENDPOINT_URL, status=200, content_type="text/plain; charset=utf-8", body=CITY_SOAP_RESPONSE
        )

        context, manifest = prepare_manifest(
            rc,
            f"""
            d | r | b | m | property | type    | ref    | source                | access | prepare
            example                  |         |        |                       |        |
              | wsdl_resource        | wsdl    |        | {WSDL_SOURCE}         |        |
              | region_resource      | soap    |        | {REGION_SOAP_SOURCE}  |        | wsdl(wsdl_resource)
              |   |   | Region       |         | code   | /                     |        |
              |   |   |   | code     | string  |        | code                  | open   |
              |   |   |   | _id      | string  |        |                       | open   |
              | city_resource        | soap    |        | {CITY_SOAP_SOURCE}    |        | wsdl(wsdl_resource)
              |   |   | City         |         | id     | /                     |        |
              |   |   |   | id       | integer |        | id                    | open   |
              |   |   |   | region   | ref     | Region | region_code           | open   |
            """,
            mode=Mode.external,
        )

        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall"])
        app.authmodel("example/City", ["getall"])

        region_rows = {r["code"]: r["_id"] for r in app.get("/example/Region").json()["_data"]}
        city_rows = {r["id"]: r for r in app.get("/example/City").json()["_data"]}
        assert city_rows[1]["region"]["_id"] == region_rows["ORD001"]
        assert city_rows[2]["region"]["_id"] == region_rows["ORD002"]

    def test_ref_to_uuid_id_model(self, rc: RawConfig, responses: RequestsMock):
        responses.add(
            POST, REGION_ENDPOINT_URL, status=200, content_type="text/plain; charset=utf-8", body=SOAP_RESPONSE
        )
        responses.add(
            POST, CITY_ENDPOINT_URL, status=200, content_type="text/plain; charset=utf-8", body=CITY_SOAP_RESPONSE
        )

        context, manifest = prepare_manifest(
            rc,
            f"""
            d | r | b | m | property | type    | ref     | source                | access | prepare
            example                  |         |         |                       |        |
              | wsdl_resource        | wsdl    |         | {WSDL_SOURCE}         |        |
              | region_resource      | soap    |         | {REGION_SOAP_SOURCE}  |        | wsdl(wsdl_resource)
              |   |   | Region       |         | uuid_id | /                     |        |
              |   |   |   | uuid_id  | uuid    |         | uuid_id               | open   |
              |   |   |   | _id      | uuid    |         |                       | open   |
              | city_resource        | soap    |         | {CITY_SOAP_SOURCE}    |        | wsdl(wsdl_resource)
              |   |   | City         |         | id      | /                     |        |
              |   |   |   | id       | integer |         | id                    | open   |
              |   |   |   | region   | ref     | Region  | region_uuid           | open   |
            """,
            mode=Mode.external,
        )

        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall"])
        app.authmodel("example/City", ["getall"])

        region_rows = {r["uuid_id"]: r["_id"] for r in app.get("/example/Region").json()["_data"]}
        city_rows = {r["id"]: r for r in app.get("/example/City").json()["_data"]}
        assert city_rows[1]["region"]["_id"] == region_rows["d6420786-082f-4ee4-9624-7a559f31d032"]
        assert city_rows[2]["region"]["_id"] == region_rows["8f5773d6-a5eb-4409-8f88-aca874e27200"]

    def test_ref_to_base32_model_produces_correct_id(self, rc: RawConfig, responses: RequestsMock):
        responses.add(
            POST, REGION_ENDPOINT_URL, status=200, content_type="text/plain; charset=utf-8", body=SOAP_RESPONSE
        )
        responses.add(
            POST, CITY_ENDPOINT_URL, status=200, content_type="text/plain; charset=utf-8", body=CITY_SOAP_RESPONSE
        )

        context, manifest = prepare_manifest(
            rc,
            f"""
            d | r | b | m | property | type    | ref    | source                | access | prepare
            example                  |         |        |                       |        |
              | wsdl_resource        | wsdl    |        | {WSDL_SOURCE}         |        |
              | region_resource      | soap    |        | {REGION_SOAP_SOURCE}  |        | wsdl(wsdl_resource)
              |   |   | Region       |         | code   | /                     |        |
              |   |   |   | code     | string  |        | code                  | open   |
              |   |   |   | _id      | base32  |        |                       | open   |
              | city_resource        | soap    |        | {CITY_SOAP_SOURCE}    |        | wsdl(wsdl_resource)
              |   |   | City         |         | id     | /                     |        |
              |   |   |   | id       | integer |        | id                    | open   |
              |   |   |   | region   | ref     | Region | region_code           | open   |
            """,
            mode=Mode.external,
        )

        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall"])
        app.authmodel("example/City", ["getall"])

        region_rows = {r["code"]: r["_id"] for r in app.get("/example/Region").json()["_data"]}
        city_rows = {r["id"]: r for r in app.get("/example/City").json()["_data"]}
        assert city_rows[1]["region"]["_id"] == region_rows["ORD001"]
        assert city_rows[2]["region"]["_id"] == region_rows["ORD002"]
