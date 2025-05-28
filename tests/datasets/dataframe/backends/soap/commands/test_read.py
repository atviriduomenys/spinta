from responses import RequestsMock, POST

from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import prepare_manifest


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
    responses.add(POST, endpoint_url, status=200, content_type='text/plain; charset=utf-8', body=soap_response)

    context, manifest = prepare_manifest(rc, """
        d | r | b | m | property      | type    | ref | source                                          | access | prepare
        example                       | dataset |     |                                                 |        |
          | wsdl_resource             | wsdl    |     | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
          | soap_resource             | soap    |     | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
          |   |   | City              |         | id  | /                                               | open   |
          |   |   |   | id            | integer |     | id                                              |        |
          |   |   |   | name          | string  |     | name                                            |        |
        """, mode=Mode.external)

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
        }
    ]
