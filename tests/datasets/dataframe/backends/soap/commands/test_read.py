from unittest.mock import ANY

import pytest
from responses import RequestsMock, POST, GET

from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.exceptions import SoapServiceError
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import prepare_manifest
from spinta.testing.utils import get_error_codes


def _get_wsdl_response(
    endpoint_url: str,
    service_name: str = "Service",
    port_name: str = "Port",
    operation_name: str = "endpoint",
) -> str:
    return f"""
        <wsdl:definitions xmlns:xs="http://www.w3.org/2001/XMLSchema" 
                          xmlns:wsdlsoap11="http://schemas.xmlsoap.org/wsdl/soap/" 
                          xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/" 
                          xmlns:tns="test_soap_application_tns" 
                          xmlns:s0="apps.address_registry.services" 
                          targetNamespace="test_soap_application_tns" 
                          name="PortType">
            <wsdl:types>
                <xs:schema targetNamespace="test_soap_application_tns" elementFormDefault="qualified">
                    <xs:import namespace="apps.address_registry.services"/>
                    <xs:complexType name="pavadinimas"/>
                    <xs:complexType name="pavadinimasResponse">
                        <xs:sequence>
                            <xs:element name="pavadinimasResult" type="s0:PavadinimasComplexModelArray" minOccurs="0" nillable="true"/>
                        </xs:sequence>
                    </xs:complexType>
                    <xs:element name="pavadinimas" type="tns:pavadinimas"/>
                    <xs:element name="pavadinimasResponse" type="tns:pavadinimasResponse"/>
                </xs:schema>
                <xs:schema targetNamespace="apps.address_registry.services" elementFormDefault="qualified">
                    <xs:complexType name="GyvenvieteComplexModel">
                        <xs:sequence>
                            <xs:element name="id" type="xs:integer" minOccurs="0" nillable="true"/>
                            <xs:element name="pavadinimas" type="xs:string" minOccurs="0" nillable="true"/>
                            <xs:element name="kurortas" type="xs:boolean" minOccurs="0" nillable="true"/>
                        </xs:sequence>
                    </xs:complexType>
                    <xs:complexType name="PavadinimasComplexModel">
                        <xs:sequence>
                            <xs:element name="id" type="xs:integer" minOccurs="0" nillable="true"/>
                            <xs:element name="pavadinimas" type="xs:string" minOccurs="0" nillable="true"/>
                            <xs:element name="gyvenviete_id" type="xs:integer" minOccurs="0" nillable="true"/>
                            <xs:element name="gyvenviete" type="s0:GyvenvieteComplexModel" minOccurs="0" nillable="true"/>
                        </xs:sequence>
                    </xs:complexType>
                    <xs:complexType name="PavadinimasComplexModelArray">
                        <xs:sequence>
                            <xs:element name="PavadinimasComplexModel" type="s0:PavadinimasComplexModel" minOccurs="0" maxOccurs="unbounded" nillable="true"/>
                        </xs:sequence>
                    </xs:complexType>
                    <xs:element name="GyvenvieteComplexModel" type="s0:GyvenvieteComplexModel"/>
                    <xs:element name="PavadinimasComplexModel" type="s0:PavadinimasComplexModel"/>
                    <xs:element name="PavadinimasComplexModelArray" type="s0:PavadinimasComplexModelArray"/>
                </xs:schema>
            </wsdl:types>
            <wsdl:message name="pavadinimas">
                <wsdl:part name="pavadinimas" element="tns:pavadinimas"/>
            </wsdl:message>
            <wsdl:message name="pavadinimasResponse">
                <wsdl:part name="pavadinimasResponse" element="tns:pavadinimasResponse"/>
            </wsdl:message>
            <wsdl:service name="{service_name}">
                <wsdl:port name="{port_name}" binding="tns:PortType">
                    <wsdlsoap11:address location="{endpoint_url}"/>
                </wsdl:port>
            </wsdl:service>
            <wsdl:portType name="PortType">
                <wsdl:operation name="{operation_name}" parameterOrder="endpoint">
                    <wsdl:input name="pavadinimas" message="tns:pavadinimas"/>
                    <wsdl:output name="pavadinimasResponse" message="tns:pavadinimasResponse"/>
                </wsdl:operation>
            </wsdl:portType>
            <wsdl:binding name="PortType" type="tns:PortType">
                <wsdlsoap11:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
                <wsdl:operation name="{operation_name}">
                    <wsdlsoap11:operation soapAction="{operation_name}" style="document"/>
                    <wsdl:input name="pavadinimas">
                        <wsdlsoap11:body use="literal"/>
                    </wsdl:input>
                    <wsdl:output name="pavadinimasResponse">
                        <wsdlsoap11:body use="literal"/>
                    </wsdl:output>
                </wsdl:operation>
            </wsdl:binding>
        </wsdl:definitions>
        """


@pytest.mark.parametrize(
    "invalid_source",
    [
        "foo",
        "foo.bar",
        "foo.bar.baz"
    ],
)
def test_raise_error_when_wsdl_source_in_wrong_format(
    rc: RawConfig,
    responses: RequestsMock,
    invalid_source: str,
) -> None:
    context, manifest = prepare_manifest(rc, f"""
        d | r | b | m | property               | type    | ref        | source                         | access
        example                                | dataset |            |                                |       
          | soap_resource                      | soap    |            | not_important_wsdl_url         |       
          |   |   | Pavadinimas                |         | id         | {invalid_source}               | open  
          |   |   |   | id                     | integer |            | id                             |       
        """, mode=Mode.external)

    context.loaded = True
    app = create_test_client(context)
    app.authmodel("/example/Pavadinimas/", ["getall"])

    response = app.get("/example/Pavadinimas/")

    assert response.status_code == 500
    assert get_error_codes(response.json()) == ["InvalidSource"]


@pytest.mark.parametrize(
    "service, port, operation",
    [
        ("invalid_service", "invalid_port", "endpoint"),
        ("invalid_service", "Port", "endpoint"),
        ("Service", "invalid_port", "endpoint"),
        ("Service", "Port", "invalid_endpoint")
    ]
)
def test_raise_error_when_service_does_not_exist_in_wsdl(
    rc: RawConfig,
    responses: RequestsMock,
    service: str,
    port: str,
    operation: str,
) -> None:
    wsdl_url = "https://www.test.com?wsdl"
    endpoint_url = "https://www.test.com/soap/"
    wsdl_response = _get_wsdl_response(
        endpoint_url,
        service_name=service,
        port_name=port,
        operation_name=operation,
    )

    responses.add(GET, wsdl_url, status=200, content_type='text/plain; charset=utf-8', body=wsdl_response)

    context, manifest = prepare_manifest(rc, f"""
        d | r | b | m | property               | type    | ref        | source                             | access
        example                                | dataset |            |                                    |       
          | soap_resource                      | soap    |            | {wsdl_url}                         |       
          |   |   | Pavadinimas                |         | id         | Service.Port.PortType.endpoint     | open  
          |   |   |   | id                     | integer |            | id                                 |       
        """, mode=Mode.external)

    context.loaded = True
    app = create_test_client(context)
    app.authmodel("/example/Pavadinimas/", ["getall"])

    with pytest.raises(SoapServiceError):
        app.get("/example/Pavadinimas/")


def test_soap_read(rc: RawConfig, responses: RequestsMock) -> None:
    wsdl_url = "https://www.test.com?wsdl"
    endpoint_url = "https://www.test.com/soap/"
    wsdl_response = _get_wsdl_response(endpoint_url)
    soap_response = """
        <ns0:Envelope xmlns:ns0="http://schemas.xmlsoap.org/soap/envelope/" 
                      xmlns:ns1="test_soap_application_tns"
                      xmlns:ns2="apps.address_registry.services">
            <ns0:Body>
                <ns1:pavadinimasResponse>
                    <ns1:pavadinimasResult>
                        <ns2:PavadinimasComplexModel>
                            <ns2:id>12660</ns2:id>
                            <ns2:pavadinimas>Pavadinimas - 1</ns2:pavadinimas>
                            <ns2:gyvenviete_id>6479</ns2:gyvenviete_id>
                            <ns2:gyvenviete>
                                <ns2:id>6479</ns2:id>
                                <ns2:pavadinimas>Gyvenvietes pavadinimas - 1</ns2:pavadinimas>
                                <ns2:kurortas>true</ns2:kurortas>
                            </ns2:gyvenviete>
                        </ns2:PavadinimasComplexModel>
                        <ns2:PavadinimasComplexModel>
                            <ns2:id>12661</ns2:id>
                            <ns2:pavadinimas>Pavadinimas - 2</ns2:pavadinimas>
                            <ns2:gyvenviete_id>6479</ns2:gyvenviete_id>
                            <ns2:gyvenviete>
                                <ns2:id>6479</ns2:id>
                                <ns2:pavadinimas>Gyvenvietes pavadinimas - 1</ns2:pavadinimas>
                                <ns2:kurortas>true</ns2:kurortas>
                            </ns2:gyvenviete>
                        </ns2:PavadinimasComplexModel>
                    </ns1:pavadinimasResult>
                </ns1:pavadinimasResponse>
            </ns0:Body>
        </ns0:Envelope>
    """

    responses.add(GET, wsdl_url, status=200, content_type='text/plain; charset=utf-8', body=wsdl_response)
    responses.add(POST, endpoint_url, status=200, content_type='text/plain; charset=utf-8', body=soap_response)

    context, manifest = prepare_manifest(rc, f"""
        d | r | b | m | property               | type    | ref        | source                         | access
        example                                | dataset |            |                                |       
          | soap_resource                      | soap    |            | {wsdl_url}                     |       
          |   |   | Pavadinimas                |         | id         | Service.Port.PortType.endpoint | open  
          |   |   |   | id                     | integer |            | id                             |       
          |   |   |   | pavadinimas            | string  |            | pavadinimas                    |       
          |   |   |   | gyvenviete_id          | integer |            | gyvenviete_id                  |       
          |   |   |   | gyvenviete             | ref     | Gyvenviete | gyvenviete                     |       
          |   |   |   | gyvenviete.id          |         |            |                                |       
          |   |   |   | gyvenviete.pavadinimas |         |            |                                |       
          |   |   |   | gyvenviete.kurortas    |         |            |                                |       
          |   |   | Gyvenviete                 |         | id         |                                | open  
          |   |   |   | id                     | integer |            | id                             |       
          |   |   |   | pavadinimas            | string  |            | pavadinimas                    |       
          |   |   |   | kurortas               | boolean |            | kurortas                       |       
        """, mode=Mode.external)

    context.loaded = True
    app = create_test_client(context)
    app.authmodel("/example/Pavadinimas/", ["getall"])

    response = app.get("/example/Pavadinimas/")
    assert listdata(response, sort=False, full=True) == [
        {
            "gyvenviete._id": ANY,
            "id": 12660,
            "pavadinimas": "Pavadinimas - 1",
            "gyvenviete_id": 6479,
        },
        {
            "gyvenviete._id": ANY,
            "id": 12661,
            "pavadinimas": "Pavadinimas - 2",
            "gyvenviete_id": 6479,
        }
    ]
