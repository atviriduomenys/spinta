"""Legacy transitional WSDL manifest scaffolding.

The functional contract baseline now lives in test_wsdl_functional.py.
Keep helper-seam, extraction-context, and other implementation-oriented
coverage here until a later retirement decision is made.
"""

from pathlib import Path
from io import BytesIO
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest

from spinta import commands
from spinta.core.config import RawConfig
from spinta.datasets.inspect.helpers import create_manifest_from_inspect
from spinta.exceptions import ReferencedSchemaFileDoesNotExist
from spinta.manifests.components import Manifest
from spinta.manifests.wsdl.commands.load import load as load_wsdl_manifest
from spinta.manifests.wsdl.components import WsdlManifest
from spinta.manifests.wsdl.helpers import _extract_wsdl_read_context
from spinta.manifests.wsdl.helpers import _read_xml
from spinta.manifests.wsdl.helpers import normalize_wsdl_path
from spinta.manifests.wsdl.helpers import read_schema
from spinta.testing.context import create_test_context
from spinta.testing.manifest import load_manifest_and_context


COUNTRY_WSDL = """
<wsdl:definitions
    xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
    xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:tns="urn:country"
    targetNamespace="urn:country"
    name="CountryService"
>
    <wsdl:types>
        <xs:schema targetNamespace="urn:country">
            <xs:element name="GetCountryRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="code" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetCountryResponse">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" type="xs:string" />
                        <xs:element name="population" type="xs:int" minOccurs="0" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
    </wsdl:types>
    <wsdl:message name="GetCountryInput">
        <wsdl:part name="parameters" element="tns:GetCountryRequest" />
    </wsdl:message>
    <wsdl:message name="GetCountryOutput">
        <wsdl:part name="parameters" element="tns:GetCountryResponse" />
    </wsdl:message>
    <wsdl:portType name="CountryPortType">
        <wsdl:operation name="GetCountry">
            <wsdl:input message="tns:GetCountryInput" />
            <wsdl:output message="tns:GetCountryOutput" />
        </wsdl:operation>
    </wsdl:portType>
    <wsdl:binding name="CountryBinding" type="tns:CountryPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http" />
        <wsdl:operation name="GetCountry">
            <soap:operation soapAction="urn:country#GetCountry" />
        </wsdl:operation>
    </wsdl:binding>
    <wsdl:service name="CountryService">
        <wsdl:port name="CountryPort" binding="tns:CountryBinding">
            <soap:address location="https://example.com/country" />
        </wsdl:port>
    </wsdl:service>
</wsdl:definitions>
"""


MULTIPART_ELEMENT_WSDL = """
<wsdl:definitions
    xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
    xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:tns="urn:multipart"
    targetNamespace="urn:multipart"
    name="MultipartService"
>
    <wsdl:types>
        <xs:schema targetNamespace="urn:multipart">
            <xs:element name="GetCountryRequestHeader">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="request_id" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetCountryRequestBody">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="code" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetCountryResponseMeta">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="status" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetCountryResponseBody">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
    </wsdl:types>
    <wsdl:message name="GetCountryInput">
        <wsdl:part name="header" element="tns:GetCountryRequestHeader" />
        <wsdl:part name="body" element="tns:GetCountryRequestBody" />
    </wsdl:message>
    <wsdl:message name="GetCountryOutput">
        <wsdl:part name="meta" element="tns:GetCountryResponseMeta" />
        <wsdl:part name="payload" element="tns:GetCountryResponseBody" />
    </wsdl:message>
    <wsdl:portType name="MultipartPortType">
        <wsdl:operation name="GetCountry">
            <wsdl:input message="tns:GetCountryInput" />
            <wsdl:output message="tns:GetCountryOutput" />
        </wsdl:operation>
    </wsdl:portType>
    <wsdl:binding name="MultipartBinding" type="tns:MultipartPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http" />
        <wsdl:operation name="GetCountry">
            <soap:operation soapAction="urn:multipart#GetCountry" />
        </wsdl:operation>
    </wsdl:binding>
    <wsdl:service name="MultipartService">
        <wsdl:port name="MultipartPort" binding="tns:MultipartBinding">
            <soap:address location="https://example.com/multipart" />
        </wsdl:port>
    </wsdl:service>
</wsdl:definitions>
"""


COUNTRY_WSDL_WITHOUT_SOAP_ACTION = COUNTRY_WSDL.replace(
    '<soap:operation soapAction="urn:country#GetCountry" />',
    '<soap:operation />',
)


COUNTRY_WSDL_DUPLICATE_MESSAGE = COUNTRY_WSDL.replace(
    '    <wsdl:message name="GetCountryOutput">\n',
    '    <wsdl:message name="GetCountryInput">\n'
    '        <wsdl:part name="parameters" element="tns:GetCountryResponse" />\n'
    '    </wsdl:message>\n'
    '    <wsdl:message name="GetCountryOutput">\n',
)


COUNTRY_WSDL_DUPLICATE_EMBEDDED_ELEMENT = """
<wsdl:definitions
    xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
    xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:tns="urn:country"
    targetNamespace="urn:country"
    name="CountryService"
>
    <wsdl:types>
        <xs:schema targetNamespace="urn:country">
            <xs:element name="GetCountryRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="code" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetCountryResponse">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" type="xs:string" />
                        <xs:element name="population" type="xs:int" minOccurs="0" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetCountryResponse">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="foreign_name" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
    </wsdl:types>
    <wsdl:message name="GetCountryInput">
        <wsdl:part name="parameters" element="tns:GetCountryRequest" />
    </wsdl:message>
    <wsdl:message name="GetCountryOutput">
        <wsdl:part name="parameters" element="tns:GetCountryResponse" />
    </wsdl:message>
    <wsdl:portType name="CountryPortType">
        <wsdl:operation name="GetCountry">
            <wsdl:input message="tns:GetCountryInput" />
            <wsdl:output message="tns:GetCountryOutput" />
        </wsdl:operation>
    </wsdl:portType>
    <wsdl:binding name="CountryBinding" type="tns:CountryPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http" />
        <wsdl:operation name="GetCountry">
            <soap:operation soapAction="urn:country#GetCountry" />
        </wsdl:operation>
    </wsdl:binding>
    <wsdl:service name="CountryService">
        <wsdl:port name="CountryPort" binding="tns:CountryBinding">
            <soap:address location="https://example.com/country" />
        </wsdl:port>
    </wsdl:service>
</wsdl:definitions>
"""


COUNTRY_WSDL_DUPLICATE_EMBEDDED_TYPE = """
<wsdl:definitions
    xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
    xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:tns="urn:country"
    targetNamespace="urn:country"
    name="CountryService"
>
    <wsdl:types>
        <xs:schema targetNamespace="urn:country">
            <xs:element name="GetCountryRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="code" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:complexType name="GetCountryResponseType">
                <xs:sequence>
                    <xs:element name="name" type="xs:string" />
                    <xs:element name="population" type="xs:int" minOccurs="0" />
                </xs:sequence>
            </xs:complexType>
            <xs:complexType name="GetCountryResponseType">
                <xs:sequence>
                    <xs:element name="foreign_name" type="xs:string" />
                </xs:sequence>
            </xs:complexType>
            <xs:element name="GetCountryResponse" type="tns:GetCountryResponseType" />
        </xs:schema>
    </wsdl:types>
    <wsdl:message name="GetCountryInput">
        <wsdl:part name="parameters" element="tns:GetCountryRequest" />
    </wsdl:message>
    <wsdl:message name="GetCountryOutput">
        <wsdl:part name="parameters" element="tns:GetCountryResponse" />
    </wsdl:message>
    <wsdl:portType name="CountryPortType">
        <wsdl:operation name="GetCountry">
            <wsdl:input message="tns:GetCountryInput" />
            <wsdl:output message="tns:GetCountryOutput" />
        </wsdl:operation>
    </wsdl:portType>
    <wsdl:binding name="CountryBinding" type="tns:CountryPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http" />
        <wsdl:operation name="GetCountry">
            <soap:operation soapAction="urn:country#GetCountry" />
        </wsdl:operation>
    </wsdl:binding>
    <wsdl:service name="CountryService">
        <wsdl:port name="CountryPort" binding="tns:CountryBinding">
            <soap:address location="https://example.com/country" />
        </wsdl:port>
    </wsdl:service>
</wsdl:definitions>
"""


COUNTRY_WSDL_NAMESPACE_VARIANT = (
    COUNTRY_WSDL
    .replace('xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"', 'xmlns:soap11="http://schemas.xmlsoap.org/wsdl/soap/"')
    .replace('<soap:binding', '<soap11:binding')
    .replace('<soap:operation', '<soap11:operation')
    .replace('<soap:address', '<soap11:address')
)


COUNTRY_WSDL_QNAME_PREFIX_VARIANT = (
    COUNTRY_WSDL
    .replace(
        'xmlns:tns="urn:country"',
        'xmlns:tns="urn:country"\n    xmlns:messages="urn:country"\n    xmlns:bindings="urn:country"\n    xmlns:interfaces="urn:country"\n    xmlns:elements="urn:country"',
    )
    .replace('element="tns:GetCountryRequest"', 'element="elements:GetCountryRequest"')
    .replace('element="tns:GetCountryResponse"', 'element="elements:GetCountryResponse"')
    .replace('message="tns:GetCountryInput"', 'message="messages:GetCountryInput"')
    .replace('message="tns:GetCountryOutput"', 'message="messages:GetCountryOutput"')
    .replace('type="tns:CountryPortType"', 'type="interfaces:CountryPortType"')
    .replace('binding="tns:CountryBinding"', 'binding="bindings:CountryBinding"')
)


COUNTRY_WSDL_DEFAULT_NAMESPACE_REFERENCES = """
<definitions
    xmlns="http://schemas.xmlsoap.org/wsdl/"
    xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    targetNamespace="urn:country"
    name="CountryService"
>
    <types>
        <xs:schema targetNamespace="urn:country">
            <xs:element name="GetCountryRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="code" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetCountryResponse">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" type="xs:string" />
                        <xs:element name="population" type="xs:int" minOccurs="0" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
    </types>
    <message name="GetCountryInput">
        <part name="parameters" element="GetCountryRequest" />
    </message>
    <message name="GetCountryOutput">
        <part name="parameters" element="GetCountryResponse" />
    </message>
    <portType name="CountryPortType">
        <operation name="GetCountry">
            <input message="GetCountryInput" />
            <output message="GetCountryOutput" />
        </operation>
    </portType>
    <binding name="CountryBinding" type="CountryPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http" />
        <operation name="GetCountry">
            <soap:operation soapAction="urn:country#GetCountry" />
        </operation>
    </binding>
    <service name="CountryService">
        <port name="CountryPort" binding="CountryBinding">
            <soap:address location="https://example.com/country" />
        </port>
    </service>
</definitions>
"""


COUNTRY_WSDL_DEFAULT_NAMESPACE_SCHEMA_TYPES = """
<wsdl:definitions
    xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
    xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    targetNamespace="urn:country"
    name="CountryService"
>
    <wsdl:types>
        <xs:schema xmlns="urn:country" targetNamespace="urn:country" elementFormDefault="qualified">
            <xs:complexType name="GetCountryResponseType">
                <xs:sequence>
                    <xs:element name="name" type="xs:string" />
                    <xs:element name="population" type="xs:int" minOccurs="0" />
                </xs:sequence>
            </xs:complexType>
            <xs:element name="GetCountryRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="code" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetCountryResponse" type="GetCountryResponseType" />
        </xs:schema>
    </wsdl:types>
    <wsdl:message name="GetCountryInput">
        <wsdl:part name="parameters" element="GetCountryRequest" />
    </wsdl:message>
    <wsdl:message name="GetCountryOutput">
        <wsdl:part name="parameters" element="GetCountryResponse" />
    </wsdl:message>
    <wsdl:portType name="CountryPortType">
        <wsdl:operation name="GetCountry">
            <wsdl:input message="GetCountryInput" />
            <wsdl:output message="GetCountryOutput" />
        </wsdl:operation>
    </wsdl:portType>
    <wsdl:binding name="CountryBinding" type="CountryPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http" />
        <wsdl:operation name="GetCountry">
            <soap:operation soapAction="urn:country#GetCountry" />
        </wsdl:operation>
    </wsdl:binding>
    <wsdl:service name="CountryService">
        <wsdl:port name="CountryPort" binding="CountryBinding">
            <soap:address location="https://example.com/country" />
        </wsdl:port>
    </wsdl:service>
</wsdl:definitions>
"""


SCALAR_TYPES_WSDL = """
<wsdl:definitions
    xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
    xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:tns="urn:scalars"
    targetNamespace="urn:scalars"
    name="ScalarService"
>
    <wsdl:types>
        <xs:schema targetNamespace="urn:scalars">
            <xs:element name="GetScalarRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="id" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetScalarResponse">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" type="xs:string" />
                        <xs:element name="enabled" type="xs:boolean" />
                        <xs:element name="created_on" type="xs:date" />
                        <xs:element name="updated_at" type="xs:dateTime" />
                        <xs:element name="opens_at" type="xs:time" />
                        <xs:element name="score" type="xs:decimal" />
                        <xs:element name="count" type="xs:int" />
                        <xs:element name="website" type="xs:anyURI" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
    </wsdl:types>
    <wsdl:message name="GetScalarInput">
        <wsdl:part name="parameters" element="tns:GetScalarRequest" />
    </wsdl:message>
    <wsdl:message name="GetScalarOutput">
        <wsdl:part name="parameters" element="tns:GetScalarResponse" />
    </wsdl:message>
    <wsdl:portType name="ScalarPortType">
        <wsdl:operation name="GetScalar">
            <wsdl:input message="tns:GetScalarInput" />
            <wsdl:output message="tns:GetScalarOutput" />
        </wsdl:operation>
    </wsdl:portType>
    <wsdl:binding name="ScalarBinding" type="tns:ScalarPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http" />
        <wsdl:operation name="GetScalar">
            <soap:operation soapAction="urn:scalars#GetScalar" />
        </wsdl:operation>
    </wsdl:binding>
    <wsdl:service name="ScalarService">
        <wsdl:port name="ScalarPort" binding="tns:ScalarBinding">
            <soap:address location="https://example.com/scalars" />
        </wsdl:port>
    </wsdl:service>
</wsdl:definitions>
"""


NESTED_TYPES_WSDL = """
<wsdl:definitions
    xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
    xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:tns="urn:nested"
    targetNamespace="urn:nested"
    name="NestedService"
>
    <wsdl:types>
        <xs:schema targetNamespace="urn:nested">
            <xs:element name="GetNestedRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="code" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetNestedResponse">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="location" minOccurs="0">
                            <xs:complexType>
                                <xs:sequence>
                                    <xs:element name="city" type="xs:string" />
                                    <xs:element name="zip" type="xs:int" />
                                </xs:sequence>
                            </xs:complexType>
                        </xs:element>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
    </wsdl:types>
    <wsdl:message name="GetNestedInput">
        <wsdl:part name="parameters" element="tns:GetNestedRequest" />
    </wsdl:message>
    <wsdl:message name="GetNestedOutput">
        <wsdl:part name="parameters" element="tns:GetNestedResponse" />
    </wsdl:message>
    <wsdl:portType name="NestedPortType">
        <wsdl:operation name="GetNested">
            <wsdl:input message="tns:GetNestedInput" />
            <wsdl:output message="tns:GetNestedOutput" />
        </wsdl:operation>
    </wsdl:portType>
    <wsdl:binding name="NestedBinding" type="tns:NestedPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http" />
        <wsdl:operation name="GetNested">
            <soap:operation soapAction="urn:nested#GetNested" />
        </wsdl:operation>
    </wsdl:binding>
    <wsdl:service name="NestedService">
        <wsdl:port name="NestedPort" binding="tns:NestedBinding">
            <soap:address location="https://example.com/nested" />
        </wsdl:port>
    </wsdl:service>
</wsdl:definitions>
"""


LOCAL_REFERENCED_RESPONSE_TYPES_XSD = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:country" xmlns:tns="urn:country">
    <xs:complexType name="GetCountryResponseType">
        <xs:sequence>
            <xs:element name="name" type="xs:string" />
            <xs:element name="population" type="xs:int" minOccurs="0" />
        </xs:sequence>
    </xs:complexType>
</xs:schema>
"""


REMOTE_REFERENCED_RESPONSE_TYPES_XSD = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:country-types" xmlns:types="urn:country-types">
    <xs:complexType name="GetCountryResponseType">
        <xs:sequence>
            <xs:element name="name" type="xs:string" />
            <xs:element name="population" type="xs:int" minOccurs="0" />
        </xs:sequence>
    </xs:complexType>
</xs:schema>
"""


CROSS_NAMESPACE_REFERENCED_TYPES_XSD = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:country-types" xmlns:types="urn:country-types" elementFormDefault="qualified">
    <xs:element name="name" type="xs:string" />
    <xs:simpleType name="PopulationType">
        <xs:restriction base="xs:int" />
    </xs:simpleType>
</xs:schema>
"""


CROSS_NAMESPACE_COLLIDING_RESPONSE_TYPES_XSD = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:country-types" xmlns:types="urn:country-types" elementFormDefault="qualified">
    <xs:element name="GetCountryResponse">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="foreign_name" type="xs:string" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    <xs:element name="name" type="xs:string" />
    <xs:simpleType name="PopulationType">
        <xs:restriction base="xs:int" />
    </xs:simpleType>
</xs:schema>
"""


CYCLIC_REFERENCED_PRIMARY_TYPES_XSD = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:country" xmlns:tns="urn:country">
    <xs:include schemaLocation="country-types-secondary.xsd" />
</xs:schema>
"""


CYCLIC_REFERENCED_SECONDARY_TYPES_XSD = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:country" xmlns:tns="urn:country">
    <xs:include schemaLocation="country-types-primary.xsd" />
    <xs:complexType name="GetCountryResponseType">
        <xs:sequence>
            <xs:element name="name" type="xs:string" />
            <xs:element name="population" type="xs:int" minOccurs="0" />
        </xs:sequence>
    </xs:complexType>
</xs:schema>
"""


def _build_local_reference_wsdl(schema_location: str) -> str:
    return f"""
<wsdl:definitions
    xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
    xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:tns="urn:country"
    targetNamespace="urn:country"
    name="CountryService"
>
    <wsdl:types>
        <xs:schema targetNamespace="urn:country">
            <xs:include schemaLocation="{schema_location}" />
            <xs:element name="GetCountryRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="code" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetCountryResponse" type="tns:GetCountryResponseType" />
        </xs:schema>
    </wsdl:types>
    <wsdl:message name="GetCountryInput">
        <wsdl:part name="parameters" element="tns:GetCountryRequest" />
    </wsdl:message>
    <wsdl:message name="GetCountryOutput">
        <wsdl:part name="parameters" element="tns:GetCountryResponse" />
    </wsdl:message>
    <wsdl:portType name="CountryPortType">
        <wsdl:operation name="GetCountry">
            <wsdl:input message="tns:GetCountryInput" />
            <wsdl:output message="tns:GetCountryOutput" />
        </wsdl:operation>
    </wsdl:portType>
    <wsdl:binding name="CountryBinding" type="tns:CountryPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http" />
        <wsdl:operation name="GetCountry">
            <soap:operation soapAction="urn:country#GetCountry" />
        </wsdl:operation>
    </wsdl:binding>
    <wsdl:service name="CountryService">
        <wsdl:port name="CountryPort" binding="tns:CountryBinding">
            <soap:address location="https://example.com/country" />
        </wsdl:port>
    </wsdl:service>
</wsdl:definitions>
"""


def _build_remote_reference_wsdl(schema_location: str) -> str:
    return f"""
<wsdl:definitions
    xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
    xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:tns="urn:country"
    xmlns:types="urn:country-types"
    targetNamespace="urn:country"
    name="CountryService"
>
    <wsdl:types>
        <xs:schema targetNamespace="urn:country">
            <xs:import namespace="urn:country-types" schemaLocation="{schema_location}" />
            <xs:element name="GetCountryRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="code" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetCountryResponse" type="types:GetCountryResponseType" />
        </xs:schema>
    </wsdl:types>
    <wsdl:message name="GetCountryInput">
        <wsdl:part name="parameters" element="tns:GetCountryRequest" />
    </wsdl:message>
    <wsdl:message name="GetCountryOutput">
        <wsdl:part name="parameters" element="tns:GetCountryResponse" />
    </wsdl:message>
    <wsdl:portType name="CountryPortType">
        <wsdl:operation name="GetCountry">
            <wsdl:input message="tns:GetCountryInput" />
            <wsdl:output message="tns:GetCountryOutput" />
        </wsdl:operation>
    </wsdl:portType>
    <wsdl:binding name="CountryBinding" type="tns:CountryPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http" />
        <wsdl:operation name="GetCountry">
            <soap:operation soapAction="urn:country#GetCountry" />
        </wsdl:operation>
    </wsdl:binding>
    <wsdl:service name="CountryService">
        <wsdl:port name="CountryPort" binding="tns:CountryBinding">
            <soap:address location="https://example.com/country" />
        </wsdl:port>
    </wsdl:service>
</wsdl:definitions>
"""


def _build_partial_local_reference_wsdl(schema_location: str) -> str:
    return f"""
<wsdl:definitions
    xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
    xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:tns="urn:country"
    targetNamespace="urn:country"
    name="CountryService"
>
    <wsdl:types>
        <xs:schema targetNamespace="urn:country">
            <xs:element name="GetCountryRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="code" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
        <xs:schema targetNamespace="urn:country">
            <xs:include schemaLocation="{schema_location}" />
            <xs:element name="GetCountryResponse" type="tns:GetCountryResponseType" />
        </xs:schema>
    </wsdl:types>
    <wsdl:message name="GetCountryInput">
        <wsdl:part name="parameters" element="tns:GetCountryRequest" />
    </wsdl:message>
    <wsdl:message name="GetCountryOutput">
        <wsdl:part name="parameters" element="tns:GetCountryResponse" />
    </wsdl:message>
    <wsdl:portType name="CountryPortType">
        <wsdl:operation name="GetCountry">
            <wsdl:input message="tns:GetCountryInput" />
            <wsdl:output message="tns:GetCountryOutput" />
        </wsdl:operation>
    </wsdl:portType>
    <wsdl:binding name="CountryBinding" type="tns:CountryPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http" />
        <wsdl:operation name="GetCountry">
            <soap:operation soapAction="urn:country#GetCountry" />
        </wsdl:operation>
    </wsdl:binding>
    <wsdl:service name="CountryService">
        <wsdl:port name="CountryPort" binding="tns:CountryBinding">
            <soap:address location="https://example.com/country" />
        </wsdl:port>
    </wsdl:service>
</wsdl:definitions>
"""


def _build_cross_namespace_embedded_reference_wsdl(schema_location: str) -> str:
    return f"""
<wsdl:definitions
    xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
    xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:tns="urn:country"
    xmlns:types="urn:country-types"
    targetNamespace="urn:country"
    name="CountryService"
>
    <wsdl:types>
        <xs:schema targetNamespace="urn:country" elementFormDefault="qualified">
            <xs:import namespace="urn:country-types" schemaLocation="{schema_location}" />
            <xs:element name="GetCountryRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="code" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:complexType name="GetCountryResponseType">
                <xs:sequence>
                    <xs:element ref="types:name" />
                    <xs:element name="population" type="types:PopulationType" minOccurs="0" />
                </xs:sequence>
            </xs:complexType>
            <xs:element name="GetCountryResponse" type="tns:GetCountryResponseType" />
        </xs:schema>
    </wsdl:types>
    <wsdl:message name="GetCountryInput">
        <wsdl:part name="parameters" element="tns:GetCountryRequest" />
    </wsdl:message>
    <wsdl:message name="GetCountryOutput">
        <wsdl:part name="parameters" element="tns:GetCountryResponse" />
    </wsdl:message>
    <wsdl:portType name="CountryPortType">
        <wsdl:operation name="GetCountry">
            <wsdl:input message="tns:GetCountryInput" />
            <wsdl:output message="tns:GetCountryOutput" />
        </wsdl:operation>
    </wsdl:portType>
    <wsdl:binding name="CountryBinding" type="tns:CountryPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http" />
        <wsdl:operation name="GetCountry">
            <soap:operation soapAction="urn:country#GetCountry" />
        </wsdl:operation>
    </wsdl:binding>
    <wsdl:service name="CountryService">
        <wsdl:port name="CountryPort" binding="tns:CountryBinding">
            <soap:address location="https://example.com/country" />
        </wsdl:port>
    </wsdl:service>
</wsdl:definitions>
"""


def _build_cross_namespace_same_local_name_wsdl(schema_location: str) -> str:
    return f"""
<wsdl:definitions
    xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
    xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:tns="urn:country"
    xmlns:types="urn:country-types"
    targetNamespace="urn:country"
    name="CountryService"
>
    <wsdl:types>
        <xs:schema targetNamespace="urn:country" elementFormDefault="qualified">
            <xs:import namespace="urn:country-types" schemaLocation="{schema_location}" />
            <xs:element name="GetCountryRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="code" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:complexType name="GetCountryResponseType">
                <xs:sequence>
                    <xs:element name="name" type="xs:string" />
                    <xs:element name="population" type="xs:int" minOccurs="0" />
                </xs:sequence>
            </xs:complexType>
            <xs:element name="GetCountryResponse" type="tns:GetCountryResponseType" />
        </xs:schema>
    </wsdl:types>
    <wsdl:message name="GetCountryInput">
        <wsdl:part name="parameters" element="tns:GetCountryRequest" />
    </wsdl:message>
    <wsdl:message name="GetCountryOutput">
        <wsdl:part name="parameters" element="tns:GetCountryResponse" />
    </wsdl:message>
    <wsdl:portType name="CountryPortType">
        <wsdl:operation name="GetCountry">
            <wsdl:input message="tns:GetCountryInput" />
            <wsdl:output message="tns:GetCountryOutput" />
        </wsdl:operation>
    </wsdl:portType>
    <wsdl:binding name="CountryBinding" type="tns:CountryPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http" />
        <wsdl:operation name="GetCountry">
            <soap:operation soapAction="urn:country#GetCountry" />
        </wsdl:operation>
    </wsdl:binding>
    <wsdl:service name="CountryService">
        <wsdl:port name="CountryPort" binding="tns:CountryBinding">
            <soap:address location="https://example.com/country" />
        </wsdl:port>
    </wsdl:service>
</wsdl:definitions>
"""


WSDL_2_0_DETECTION_WSDL = """
<wsdl:description
    xmlns:wsdl="http://www.w3.org/ns/wsdl"
    xmlns:tns="urn:country"
    targetNamespace="urn:country"
    name="CountryService"
/>
"""


COUNTRY_WSDL_2_0 = """
<wsdl:description
    xmlns:wsdl="http://www.w3.org/ns/wsdl"
    xmlns:soap="http://www.w3.org/ns/wsdl/soap"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:tns="urn:country"
    targetNamespace="urn:country"
    name="CountryService"
>
    <wsdl:types>
        <xs:schema targetNamespace="urn:country">
            <xs:element name="GetCountryRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="code" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetCountryResponse">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" type="xs:string" />
                        <xs:element name="population" type="xs:int" minOccurs="0" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
    </wsdl:types>
    <wsdl:interface name="CountryInterface">
        <wsdl:operation name="GetCountry" pattern="http://www.w3.org/ns/wsdl/in-out">
            <wsdl:input element="tns:GetCountryRequest" />
            <wsdl:output element="tns:GetCountryResponse" />
        </wsdl:operation>
    </wsdl:interface>
    <wsdl:binding name="CountryBinding" interface="tns:CountryInterface" type="http://www.w3.org/ns/wsdl/soap">
        <soap:binding wsoap:protocol="http://www.w3.org/2003/05/soap/bindings/HTTP/" xmlns:wsoap="http://www.w3.org/ns/wsdl/soap" />
    </wsdl:binding>
    <wsdl:service name="CountryService" interface="tns:CountryInterface">
        <wsdl:endpoint name="CountryEndpoint" binding="tns:CountryBinding" address="https://example.com/country" />
    </wsdl:service>
</wsdl:description>
"""


COUNTRY_WSDL_2_0_QNAME_PREFIX_VARIANT = (
    COUNTRY_WSDL_2_0
    .replace(
        'xmlns:tns="urn:country"',
        'xmlns:tns="urn:country"\n    xmlns:elements="urn:country"\n    xmlns:contracts="urn:country"\n    xmlns:bindings="urn:country"',
    )
    .replace('element="tns:GetCountryRequest"', 'element="elements:GetCountryRequest"')
    .replace('element="tns:GetCountryResponse"', 'element="elements:GetCountryResponse"')
    .replace('interface="tns:CountryInterface"', 'interface="contracts:CountryInterface"')
    .replace('binding="tns:CountryBinding"', 'binding="bindings:CountryBinding"')
)


COUNTRY_WSDL_ELEMENT_SCOPED_PREFIX_REBINDING = """
<wsdl:definitions
    xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
    xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:tns="urn:country"
    targetNamespace="urn:country"
    name="CountryService"
>
    <wsdl:types>
        <xs:schema targetNamespace="urn:country">
            <xs:element name="GetCountryRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="code" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetCountryResponse">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" type="xs:string" />
                        <xs:element name="population" type="xs:int" minOccurs="0" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
    </wsdl:types>
    <wsdl:message name="GetCountryInput">
        <wsdl:part xmlns:msg="urn:country" name="parameters" element="msg:GetCountryRequest" />
    </wsdl:message>
    <wsdl:message name="GetCountryOutput">
        <wsdl:part xmlns:msg="urn:country" name="parameters" element="msg:GetCountryResponse" />
    </wsdl:message>
    <wsdl:portType name="CountryPortType">
        <wsdl:operation name="GetCountry">
            <wsdl:input xmlns:msg="urn:country" message="msg:GetCountryInput" />
            <wsdl:output xmlns:msg="urn:country" message="msg:GetCountryOutput" />
        </wsdl:operation>
    </wsdl:portType>
    <wsdl:binding xmlns:bind="urn:country" name="CountryBinding" type="bind:CountryPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http" />
        <wsdl:operation name="GetCountry">
            <soap:operation soapAction="urn:country#GetCountry" />
        </wsdl:operation>
    </wsdl:binding>
    <wsdl:service name="CountryService">
        <wsdl:port xmlns:bind="urn:country" name="CountryPort" binding="bind:CountryBinding">
            <soap:address location="https://example.com/country" />
        </wsdl:port>
    </wsdl:service>
    <wsdl:documentation xmlns:msg="urn:other" xmlns:bind="urn:other">
        Prefix rebinding probe for M3.4.
    </wsdl:documentation>
</wsdl:definitions>
"""


COUNTRY_WSDL_2_0_NAMESPACE_VARIANT = (
    COUNTRY_WSDL_2_0
    .replace('xmlns:soap="http://www.w3.org/ns/wsdl/soap"', 'xmlns:soap12="http://www.w3.org/ns/wsdl/soap"')
    .replace('<soap:binding', '<soap12:binding')
    .replace('xmlns:wsoap="http://www.w3.org/ns/wsdl/soap"', 'xmlns:soapmeta="http://www.w3.org/ns/wsdl/soap"')
    .replace('wsoap:protocol', 'soapmeta:protocol')
)


COUNTRY_WSDL_2_0_DUPLICATE_BINDING = COUNTRY_WSDL_2_0.replace(
    '    <wsdl:service name="CountryService" interface="tns:CountryInterface">\n',
    '    <wsdl:binding name="CountryBinding" interface="tns:CountryInterface" type="http://www.w3.org/ns/wsdl/soap">\n'
    '        <soap:binding wsoap:protocol="http://www.w3.org/2003/05/soap/bindings/HTTP/" xmlns:wsoap="http://www.w3.org/ns/wsdl/soap" />\n'
    '    </wsdl:binding>\n'
    '    <wsdl:service name="CountryService" interface="tns:CountryInterface">\n',
)


COUNTRY_WSDL_2_0_DUPLICATE_SERVICE = COUNTRY_WSDL_2_0.replace(
    '</wsdl:description>\n',
    '    <wsdl:service name="CountryService" interface="tns:CountryInterface">\n'
    '        <wsdl:endpoint name="CountryEndpointSecondary" binding="tns:CountryBinding" address="https://example.com/country-secondary" />\n'
    '    </wsdl:service>\n'
    '</wsdl:description>\n',
)


SCALAR_TYPES_WSDL_2_0 = """
<wsdl:description
    xmlns:wsdl="http://www.w3.org/ns/wsdl"
    xmlns:soap="http://www.w3.org/ns/wsdl/soap"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:tns="urn:scalars"
    targetNamespace="urn:scalars"
    name="ScalarService"
>
    <wsdl:types>
        <xs:schema targetNamespace="urn:scalars">
            <xs:element name="GetScalarRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="id" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetScalarResponse">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" type="xs:string" />
                        <xs:element name="enabled" type="xs:boolean" />
                        <xs:element name="created_on" type="xs:date" />
                        <xs:element name="updated_at" type="xs:dateTime" />
                        <xs:element name="opens_at" type="xs:time" />
                        <xs:element name="score" type="xs:decimal" />
                        <xs:element name="count" type="xs:int" />
                        <xs:element name="website" type="xs:anyURI" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
    </wsdl:types>
    <wsdl:interface name="ScalarInterface">
        <wsdl:operation name="GetScalar" pattern="http://www.w3.org/ns/wsdl/in-out">
            <wsdl:input element="tns:GetScalarRequest" />
            <wsdl:output element="tns:GetScalarResponse" />
        </wsdl:operation>
    </wsdl:interface>
    <wsdl:binding name="ScalarBinding" interface="tns:ScalarInterface" type="http://www.w3.org/ns/wsdl/soap">
        <soap:binding wsoap:protocol="http://www.w3.org/2003/05/soap/bindings/HTTP/" xmlns:wsoap="http://www.w3.org/ns/wsdl/soap" />
    </wsdl:binding>
    <wsdl:service name="ScalarService" interface="tns:ScalarInterface">
        <wsdl:endpoint name="ScalarEndpoint" binding="tns:ScalarBinding" address="https://example.com/scalars" />
    </wsdl:service>
</wsdl:description>
"""


MULTI_OPERATION_WSDL = """
<wsdl:definitions
    xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
    xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:tns="urn:registry"
    targetNamespace="urn:registry"
    name="RegistryService"
>
    <wsdl:types>
        <xs:schema targetNamespace="urn:registry">
            <xs:element name="GetCountryRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="countryCode" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetCountryResponse">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="countryName" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="ListCountriesRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="regionCode" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="ListCountriesResponse">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="resultCount" type="xs:int" />
                        <xs:element name="statusText" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
    </wsdl:types>
    <wsdl:message name="GetCountryInput">
        <wsdl:part name="parameters" element="tns:GetCountryRequest" />
    </wsdl:message>
    <wsdl:message name="GetCountryOutput">
        <wsdl:part name="parameters" element="tns:GetCountryResponse" />
    </wsdl:message>
    <wsdl:message name="ListCountriesInput">
        <wsdl:part name="parameters" element="tns:ListCountriesRequest" />
    </wsdl:message>
    <wsdl:message name="ListCountriesOutput">
        <wsdl:part name="parameters" element="tns:ListCountriesResponse" />
    </wsdl:message>
    <wsdl:portType name="RegistryPortType">
        <wsdl:operation name="GetCountry">
            <wsdl:input message="tns:GetCountryInput" />
            <wsdl:output message="tns:GetCountryOutput" />
        </wsdl:operation>
        <wsdl:operation name="ListCountries">
            <wsdl:input message="tns:ListCountriesInput" />
            <wsdl:output message="tns:ListCountriesOutput" />
        </wsdl:operation>
    </wsdl:portType>
    <wsdl:binding name="RegistryBinding" type="tns:RegistryPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http" />
        <wsdl:operation name="GetCountry">
            <soap:operation soapAction="urn:registry#GetCountry" />
        </wsdl:operation>
        <wsdl:operation name="ListCountries">
            <soap:operation soapAction="urn:registry#ListCountries" />
        </wsdl:operation>
    </wsdl:binding>
    <wsdl:service name="RegistryService">
        <wsdl:port name="RegistryPort" binding="tns:RegistryBinding">
            <soap:address location="https://example.com/registry" />
        </wsdl:port>
    </wsdl:service>
</wsdl:definitions>
"""


MULTI_OPERATION_WSDL_2_0 = """
<wsdl:description
    xmlns:wsdl="http://www.w3.org/ns/wsdl"
    xmlns:soap="http://www.w3.org/ns/wsdl/soap"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:tns="urn:registry"
    targetNamespace="urn:registry"
    name="RegistryService"
>
    <wsdl:types>
        <xs:schema targetNamespace="urn:registry">
            <xs:element name="GetCountryRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="countryCode" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetCountryResponse">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="countryName" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="ListCountriesRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="regionCode" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="ListCountriesResponse">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="resultCount" type="xs:int" />
                        <xs:element name="statusText" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
    </wsdl:types>
    <wsdl:interface name="RegistryInterface">
        <wsdl:operation name="GetCountry" pattern="http://www.w3.org/ns/wsdl/in-out">
            <wsdl:input element="tns:GetCountryRequest" />
            <wsdl:output element="tns:GetCountryResponse" />
        </wsdl:operation>
        <wsdl:operation name="ListCountries" pattern="http://www.w3.org/ns/wsdl/in-out">
            <wsdl:input element="tns:ListCountriesRequest" />
            <wsdl:output element="tns:ListCountriesResponse" />
        </wsdl:operation>
    </wsdl:interface>
    <wsdl:binding name="RegistryBinding" interface="tns:RegistryInterface" type="http://www.w3.org/ns/wsdl/soap">
        <soap:binding wsoap:protocol="http://www.w3.org/2003/05/soap/bindings/HTTP/" xmlns:wsoap="http://www.w3.org/ns/wsdl/soap" />
    </wsdl:binding>
    <wsdl:service name="RegistryService" interface="tns:RegistryInterface">
        <wsdl:endpoint name="RegistryEndpoint" binding="tns:RegistryBinding" address="https://example.com/registry" />
    </wsdl:service>
</wsdl:description>
"""


NESTED_TYPES_WSDL_2_0 = """
<wsdl:description
    xmlns:wsdl="http://www.w3.org/ns/wsdl"
    xmlns:soap="http://www.w3.org/ns/wsdl/soap"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:tns="urn:nested"
    targetNamespace="urn:nested"
    name="NestedService"
>
    <wsdl:types>
        <xs:schema targetNamespace="urn:nested">
            <xs:element name="GetNestedRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="code" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetNestedResponse">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="location" minOccurs="0">
                            <xs:complexType>
                                <xs:sequence>
                                    <xs:element name="city" type="xs:string" />
                                    <xs:element name="zip" type="xs:int" />
                                </xs:sequence>
                            </xs:complexType>
                        </xs:element>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
    </wsdl:types>
    <wsdl:interface name="NestedInterface">
        <wsdl:operation name="GetNested" pattern="http://www.w3.org/ns/wsdl/in-out">
            <wsdl:input element="tns:GetNestedRequest" />
            <wsdl:output element="tns:GetNestedResponse" />
        </wsdl:operation>
    </wsdl:interface>
    <wsdl:binding name="NestedBinding" interface="tns:NestedInterface" type="http://www.w3.org/ns/wsdl/soap">
        <soap:binding wsoap:protocol="http://www.w3.org/2003/05/soap/bindings/HTTP/" xmlns:wsoap="http://www.w3.org/ns/wsdl/soap" />
    </wsdl:binding>
    <wsdl:service name="NestedService" interface="tns:NestedInterface">
        <wsdl:endpoint name="NestedEndpoint" binding="tns:NestedBinding" address="https://example.com/nested" />
    </wsdl:service>
</wsdl:description>
"""


FAULT_WSDL_2_0 = """
<wsdl:description
    xmlns:wsdl="http://www.w3.org/ns/wsdl"
    xmlns:soap="http://www.w3.org/ns/wsdl/soap"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:tns="urn:faults"
    targetNamespace="urn:faults"
    name="FaultService"
>
    <wsdl:types>
        <xs:schema targetNamespace="urn:faults">
            <xs:element name="GetCountryRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="code" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetCountryResponse">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetCountryFault">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="code" type="xs:string" />
                        <xs:element name="message" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
    </wsdl:types>
    <wsdl:interface name="FaultInterface">
        <wsdl:operation name="GetCountry" pattern="http://www.w3.org/ns/wsdl/in-out">
            <wsdl:input element="tns:GetCountryRequest" />
            <wsdl:output element="tns:GetCountryResponse" />
            <wsdl:outfault ref="tns:GetCountryFault" />
        </wsdl:operation>
    </wsdl:interface>
    <wsdl:binding name="FaultBinding" interface="tns:FaultInterface" type="http://www.w3.org/ns/wsdl/soap">
        <soap:binding wsoap:protocol="http://www.w3.org/2003/05/soap/bindings/HTTP/" xmlns:wsoap="http://www.w3.org/ns/wsdl/soap" />
    </wsdl:binding>
    <wsdl:service name="FaultService" interface="tns:FaultInterface">
        <wsdl:endpoint name="FaultEndpoint" binding="tns:FaultBinding" address="https://example.com/faults" />
    </wsdl:service>
</wsdl:description>
"""


NON_SOAP_BINDING_WSDL_2_0 = """
<wsdl:description
    xmlns:wsdl="http://www.w3.org/ns/wsdl"
    xmlns:http="http://www.w3.org/ns/wsdl/http"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:tns="urn:httpbinding"
    targetNamespace="urn:httpbinding"
    name="HttpBindingService"
>
    <wsdl:types>
        <xs:schema targetNamespace="urn:httpbinding">
            <xs:element name="GetCountryRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="code" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetCountryResponse">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
    </wsdl:types>
    <wsdl:interface name="HttpBindingInterface">
        <wsdl:operation name="GetCountry" pattern="http://www.w3.org/ns/wsdl/in-out">
            <wsdl:input element="tns:GetCountryRequest" />
            <wsdl:output element="tns:GetCountryResponse" />
        </wsdl:operation>
    </wsdl:interface>
    <wsdl:binding name="HttpBinding" interface="tns:HttpBindingInterface" type="http://www.w3.org/ns/wsdl/http">
        <http:binding whttp:methodDefault="GET" xmlns:whttp="http://www.w3.org/ns/wsdl/http" />
    </wsdl:binding>
    <wsdl:service name="HttpBindingService" interface="tns:HttpBindingInterface">
        <wsdl:endpoint name="HttpEndpoint" binding="tns:HttpBinding" address="https://example.com/httpbinding" />
    </wsdl:service>
</wsdl:description>
"""


def test_wsdl_backend_resolves_to_wsdl_manifest(rc: RawConfig):
    context = create_test_context(rc)
    commands.load(context, context.get("config"))

    assert commands.backend_to_manifest_type(context, "wsdl") is WsdlManifest


def test_wsdl_inspect_resource_uses_wsdl_manifest(rc: RawConfig, monkeypatch, tmp_path: Path):
    raw_url = "wsdl+https://example.com/country?wsdl"
    normalized_url = "https://example.com/country?wsdl"

    class FakeResponse:
        def __init__(self, data: bytes):
            self._data = data

        def read(self) -> bytes:
            return self._data

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(request_url: str):
        parsed = urlparse(request_url)
        assert parsed.scheme == "https"
        assert parsed.netloc == "example.com"
        assert parsed.path == "/country"
        assert parse_qs(parsed.query, keep_blank_values=True) == {"wsdl": [""]}
        return FakeResponse(COUNTRY_WSDL.encode())

    monkeypatch.setattr("spinta.manifests.wsdl.helpers.urllib.request.urlopen", fake_urlopen)

    config_path = tmp_path / "config"
    config_path.mkdir()
    rc = rc.fork({"config_path": config_path})

    context = create_test_context(rc)
    commands.load(context, context.get("config"))

    context, manifest = create_manifest_from_inspect(
        context=context,
        resources=("wsdl", raw_url),
        dataset="services/country_service",
    )

    dataset = commands.get_dataset(context, manifest, "services/country_service")
    assert dataset.resources["contract"].type == "wsdl"
    assert dataset.resources["contract"].external == normalized_url

    soap_resource = dataset.resources["country_port_get_country"]
    assert soap_resource.type == "soap"
    assert str(soap_resource.prepare) == "wsdl(contract)"
    assert soap_resource.external == "CountryService.CountryPort.CountryPortType.GetCountry"


def test_wsdl_manifest_load_syncs_nested_manifests(rc: RawConfig, monkeypatch):
    context = create_test_context(rc)
    manifest = WsdlManifest()
    manifest.path = "https://example.com/country?wsdl"
    sync_manifest = Manifest()
    sync_manifest.name = "sync-source"
    manifest.sync = [sync_manifest]

    load_calls: list[tuple[Any, dict[str, Any]]] = []

    monkeypatch.setattr(
        "spinta.manifests.wsdl.commands.load.read_schema",
        lambda context, manifest, path, dataset_name: iter(()),
    )
    monkeypatch.setattr("spinta.manifests.wsdl.commands.load.load_manifest_nodes", lambda *args, **kwargs: None)
    monkeypatch.setattr("spinta.manifests.wsdl.commands.load.commands.has_model", lambda *args, **kwargs: True)

    def fake_load(context, source, **kwargs):
        load_calls.append((source, kwargs))

    monkeypatch.setattr("spinta.manifests.wsdl.commands.load.commands.load", fake_load)

    load_wsdl_manifest(context, manifest, load_internal=False)

    assert load_calls == [
        (
            sync_manifest,
            {
                "into": manifest,
                "freezed": True,
                "rename_duplicates": False,
                "load_internal": False,
                "full_load": False,
            },
        ),
    ]


def test_wsdl_load_uses_read_schema_seam(rc: RawConfig, monkeypatch):
    context = create_test_context(rc)
    manifest = WsdlManifest()
    manifest.path = "https://example.com/country?wsdl"
    manifest.sync = []

    schema_calls: list[tuple[Any, Any, str, Any]] = []

    def fake_read_schema(context, manifest, path, dataset_name):
        schema_calls.append((context, manifest, path, dataset_name))
        return iter(())

    monkeypatch.setattr("spinta.manifests.wsdl.commands.load.read_schema", fake_read_schema)
    monkeypatch.setattr("spinta.manifests.wsdl.commands.load.load_manifest_nodes", lambda *args, **kwargs: None)
    monkeypatch.setattr("spinta.manifests.wsdl.commands.load.commands.has_model", lambda *args, **kwargs: True)

    load_wsdl_manifest(context, manifest, load_internal=False)

    assert len(schema_calls) == 1
    assert schema_calls[0][:3] == (context, manifest, manifest.path)


def test_wsdl_read_schema_defers_reader_work_until_iteration(rc: RawConfig, tmp_path: Path, monkeypatch):
    from spinta.manifests.wsdl import helpers as wsdl_helpers

    path = tmp_path / "nested.wsdl"
    path.write_text(NESTED_TYPES_WSDL)

    context = create_test_context(rc)
    manifest = WsdlManifest()
    manifest.path = str(path)

    original_read_xml = wsdl_helpers._read_xml
    original_extract = wsdl_helpers._extract_wsdl_read_context
    read_xml_calls: list[str] = []
    extract_calls: list[tuple[str, str]] = []

    def fake_read_xml(path: str):
        read_xml_calls.append(path)
        return original_read_xml(path)

    def fake_extract(context, manifest, normalized_path, dataset_name, root, prefixes, *, version):
        extract_calls.append((normalized_path, version))
        return original_extract(
            context,
            manifest,
            normalized_path,
            dataset_name,
            root,
            prefixes,
            version=version,
        )

    monkeypatch.setattr("spinta.manifests.wsdl.helpers._read_xml", fake_read_xml)
    monkeypatch.setattr("spinta.manifests.wsdl.helpers._extract_wsdl_read_context", fake_extract)

    schemas = read_schema(context, manifest, manifest.path)

    assert read_xml_calls == []
    assert extract_calls == []

    first_eid, first_schema = next(schemas)

    assert first_schema is not None
    assert first_eid is None
    assert first_schema["type"] == "dataset"
    assert first_schema["name"] == "services/nested_service"
    assert read_xml_calls == [str(path)]
    assert extract_calls == [(str(path), "1.1")]


def test_wsdl_read_schema_yields_raw_models_incrementally_after_dataset(
    rc: RawConfig,
    tmp_path: Path,
    monkeypatch,
):
    from spinta.manifests.wsdl import helpers as wsdl_helpers

    path = tmp_path / "nested.wsdl"
    path.write_text(NESTED_TYPES_WSDL)

    context = create_test_context(rc)
    manifest = WsdlManifest()
    manifest.path = str(path)

    original_build_raw = wsdl_helpers._build_raw_schema_manifest_schema
    build_calls: list[str] = []

    def fake_build_raw(wsdl_context, schema_name, raw_schema_model):
        build_calls.append(schema_name)
        return original_build_raw(wsdl_context, schema_name, raw_schema_model)

    monkeypatch.setattr(
        "spinta.manifests.wsdl.helpers._build_raw_schema_manifest_schema",
        fake_build_raw,
    )

    schemas = read_schema(context, manifest, manifest.path)

    _, first_schema = next(schemas)
    assert first_schema is not None
    assert first_schema["type"] == "dataset"
    assert build_calls == []

    _, second_schema = next(schemas)
    assert second_schema is not None
    assert second_schema["type"] == "model"
    assert second_schema["name"] == "services/nested_service/schema/GetNestedRequest"
    assert build_calls == ["GetNestedRequest"]

    _, third_schema = next(schemas)
    assert third_schema is not None
    assert third_schema["type"] == "model"
    assert third_schema["name"] == "services/nested_service/schema/GetNestedResponse"
    assert build_calls == ["GetNestedRequest", "GetNestedResponse"]


def test_wsdl_read_schema_reuses_single_shared_extraction_for_raw_and_operation_models(
    rc: RawConfig,
    tmp_path: Path,
    monkeypatch,
):
    from spinta.manifests.wsdl import helpers as wsdl_helpers

    path = tmp_path / "nested.wsdl"
    path.write_text(NESTED_TYPES_WSDL)

    context = create_test_context(rc)
    manifest = WsdlManifest()
    manifest.path = str(path)

    original_collect_xsd_types = wsdl_helpers._collect_xsd_types
    collect_calls: list[int] = []

    def fake_collect_xsd_types(manifest, schemas, *, dataset_name, schema_source_path):
        collect_calls.append(len(list(schemas)))
        return original_collect_xsd_types(
            manifest,
            schemas,
            dataset_name=dataset_name,
            schema_source_path=schema_source_path,
        )

    monkeypatch.setattr("spinta.manifests.wsdl.helpers._collect_xsd_types", fake_collect_xsd_types)

    schemas = [schema for _, schema in read_schema(context, manifest, manifest.path) if schema is not None]
    observed = [(schema["type"], schema.get("name")) for schema in schemas]

    assert collect_calls == [1]
    assert observed == [
        ("dataset", "services/nested_service"),
        ("model", "services/nested_service/schema/GetNestedRequest"),
        ("model", "services/nested_service/schema/GetNestedResponse"),
        ("model", "services/nested_service/schema/Location"),
        ("model", "services/nested_service/GetNestedRequest"),
        ("model", "services/nested_service/GetNestedResponse"),
    ]


def test_wsdl_raw_and_operation_models_do_not_collide(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "nested.wsdl"
    path.write_text(NESTED_TYPES_WSDL)

    context = create_test_context(rc)
    manifest = WsdlManifest()
    manifest.path = str(path)

    schemas = [schema for _, schema in read_schema(context, manifest, manifest.path) if schema is not None]
    model_names = {schema["name"] for schema in schemas[1:] if schema["type"] == "model"}

    assert "services/nested_service/schema/GetNestedRequest" in model_names
    assert "services/nested_service/schema/GetNestedResponse" in model_names
    assert "services/nested_service/GetNestedRequest" in model_names
    assert "services/nested_service/GetNestedResponse" in model_names
    assert "services/nested_service/schema/GetNestedRequest_1" not in model_names
    assert "services/nested_service/schema/GetNestedResponse_1" not in model_names
    assert "services/nested_service/GetNestedRequest_1" not in model_names
    assert "services/nested_service/GetNestedResponse_1" not in model_names

    loaded_context, loaded_manifest = load_manifest_and_context(rc, path)
    raw_response_model = commands.get_model(loaded_context, loaded_manifest, "services/nested_service/schema/GetNestedResponse")
    operation_response_model = commands.get_model(loaded_context, loaded_manifest, "services/nested_service/GetNestedResponse")

    assert raw_response_model.name == "services/nested_service/schema/GetNestedResponse"
    assert operation_response_model.name == "services/nested_service/GetNestedResponse"
    assert raw_response_model.external.resource is not None
    assert operation_response_model.external.resource is not None
    assert raw_response_model.external.resource.name == "contract"
    assert operation_response_model.external.resource.name != "contract"


def test_wsdl_operation_models_preserve_behavior_when_raw_schema_models_exist(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "nested.wsdl"
    path.write_text(NESTED_TYPES_WSDL)

    context, manifest = load_manifest_and_context(rc, path)

    raw_response_model = commands.get_model(context, manifest, "services/nested_service/schema/GetNestedResponse")
    request_model = commands.get_model(context, manifest, "services/nested_service/GetNestedRequest")
    response_model = commands.get_model(context, manifest, "services/nested_service/GetNestedResponse")

    assert raw_response_model.external.resource is not None
    assert raw_response_model.external.resource.name == "contract"

    assert request_model.name == "services/nested_service/GetNestedRequest"
    assert response_model.name == "services/nested_service/GetNestedResponse"
    assert request_model.external.resource is not None
    assert response_model.external.resource is not None
    assert request_model.external.resource.name == "nested_port_get_nested"
    assert response_model.external.resource.name == "nested_port_get_nested"

    assert {"location_city", "location_zip"}.issubset(response_model.properties)
    assert response_model.properties["location_city"].external.name == "location/city"
    assert response_model.properties["location_zip"].external.name == "location/zip"
    assert response_model.properties["location_city"].dtype.name == "string"
    assert response_model.properties["location_zip"].dtype.name == "integer"
    assert response_model.properties["location_city"].dtype.required is False
    assert response_model.properties["location_zip"].dtype.required is False


def test_wsdl_1_1_shared_extraction_context_contains_operation_links(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country.wsdl"
    path.write_text(COUNTRY_WSDL)

    context = create_test_context(rc)
    manifest = WsdlManifest()
    manifest.path = str(path)
    normalized_path = normalize_wsdl_path(manifest.path)
    root, prefixes = _read_xml(normalized_path)

    wsdl_context = _extract_wsdl_read_context(
        context,
        manifest,
        normalized_path,
        None,
        root,
        prefixes,
        version="1.1",
    )

    assert wsdl_context.version == "1.1"
    assert wsdl_context.definitions_name == "CountryService"
    assert wsdl_context.dataset == "services/country_service"
    assert wsdl_context.resources["contract"]["type"] == "wsdl"
    assert len(wsdl_context.operation_links) == 1

    operation_link = wsdl_context.operation_links[0]
    assert operation_link.operation_name == "GetCountry"
    assert operation_link.interface_name == "CountryPortType"
    assert operation_link.resource_key == "country_port_get_country"
    assert [item.suffix for item in operation_link.messages] == ["Request", "Response"]
    assert operation_link.messages[0].message is not None
    assert operation_link.messages[1].message is not None
    assert operation_link.messages[0].message.element_qname is not None
    assert operation_link.messages[1].message.element_qname is not None
    assert operation_link.messages[0].message.element_qname.local_name == "GetCountryRequest"
    assert operation_link.messages[1].message.element_qname.local_name == "GetCountryResponse"


def test_wsdl_2_0_shared_extraction_context_contains_operation_links(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country-v2.wsdl"
    path.write_text(COUNTRY_WSDL_2_0)

    context = create_test_context(rc)
    manifest = WsdlManifest()
    manifest.path = str(path)
    normalized_path = normalize_wsdl_path(manifest.path)
    root, prefixes = _read_xml(normalized_path)

    wsdl_context = _extract_wsdl_read_context(
        context,
        manifest,
        normalized_path,
        None,
        root,
        prefixes,
        version="2.0",
    )

    assert wsdl_context.version == "2.0"
    assert wsdl_context.definitions_name == "CountryService"
    assert wsdl_context.dataset == "services/country_service"
    assert wsdl_context.resources["contract"]["type"] == "wsdl"
    assert len(wsdl_context.operation_links) == 1

    operation_link = wsdl_context.operation_links[0]
    assert operation_link.operation_name == "GetCountry"
    assert operation_link.interface_name == "CountryInterface"
    assert operation_link.resource_key == "country_endpoint_get_country"
    assert [item.suffix for item in operation_link.messages] == ["Request", "Response"]
    assert operation_link.messages[0].message is not None
    assert operation_link.messages[1].message is not None
    assert operation_link.messages[0].message.element_qname is not None
    assert operation_link.messages[1].message.element_qname is not None
    assert operation_link.messages[0].message.element_qname.local_name == "GetCountryRequest"
    assert operation_link.messages[1].message.element_qname.local_name == "GetCountryResponse"


@pytest.mark.parametrize(
    "filename, wsdl, expected_element_raw, expected_binding_raw",
    [
        (
            "country-qname-prefix-variant.wsdl",
            COUNTRY_WSDL_QNAME_PREFIX_VARIANT,
            "elements:GetCountryRequest",
            "bindings:CountryBinding",
        ),
        (
            "country-default-namespace.wsdl",
            COUNTRY_WSDL_DEFAULT_NAMESPACE_REFERENCES,
            "GetCountryRequest",
            "CountryBinding",
        ),
    ],
)
def test_wsdl_shared_extraction_context_preserves_qname_metadata(
    rc: RawConfig,
    tmp_path: Path,
    filename: str,
    wsdl: str,
    expected_element_raw: str,
    expected_binding_raw: str,
):
    path = tmp_path / filename
    path.write_text(wsdl)

    context = create_test_context(rc)
    manifest = WsdlManifest()
    manifest.path = str(path)
    normalized_path = normalize_wsdl_path(manifest.path)
    root, prefixes = _read_xml(normalized_path)

    wsdl_context = _extract_wsdl_read_context(
        context,
        manifest,
        normalized_path,
        None,
        root,
        prefixes,
        version="1.1",
    )

    input_message_key = "{urn:country}GetCountryInput"
    binding_key = "{urn:country}CountryBinding"
    service_key = "{urn:country}CountryService"
    port_key = "{urn:country}CountryPort"

    input_message = wsdl_context.messages[input_message_key]
    binding = wsdl_context.bindings[binding_key]
    port = wsdl_context.services[service_key][port_key]
    operation_link = wsdl_context.operation_links[0]

    assert wsdl_context.target_namespace == "urn:country"
    assert "http://schemas.xmlsoap.org/wsdl/" in wsdl_context.namespace_map.values()
    assert "GetCountryInput" not in wsdl_context.messages
    assert "CountryBinding" not in wsdl_context.bindings
    assert "CountryService" not in wsdl_context.services
    assert input_message.qname is not None
    assert input_message.qname.raw == "GetCountryInput"
    assert input_message.qname.local_name == "GetCountryInput"
    assert input_message.qname.namespace == "urn:country"
    assert input_message.element_qname is not None
    assert input_message.element_qname.raw == expected_element_raw
    assert input_message.element_qname.local_name == "GetCountryRequest"
    assert input_message.element_qname.namespace == "urn:country"
    assert binding.qname is not None
    assert binding.qname.raw == "CountryBinding"
    assert binding.qname.namespace == "urn:country"
    assert binding.type_qname is not None
    assert binding.type_qname.local_name == "CountryPortType"
    assert binding.type_qname.namespace == "urn:country"
    assert port.qname is not None
    assert port.qname.raw == "CountryPort"
    assert port.qname.namespace == "urn:country"
    assert port.binding_qname is not None
    assert port.binding_qname.raw == expected_binding_raw
    assert port.binding_qname.local_name == "CountryBinding"
    assert port.binding_qname.namespace == "urn:country"
    assert operation_link.interface_qname is not None
    assert operation_link.interface_qname.local_name == "CountryPortType"
    assert operation_link.interface_qname.namespace == "urn:country"
    assert operation_link.operation_qname is not None
    assert operation_link.operation_qname.local_name == "GetCountry"
    assert operation_link.operation_qname.namespace == "urn:country"


@pytest.mark.parametrize(
    "filename, wsdl, version, expected_external, expected_interface, expected_port, expected_element_raw",
    [
        (
            "country-qname-prefix-variant.wsdl",
            COUNTRY_WSDL_QNAME_PREFIX_VARIANT,
            "1.1",
            "CountryService.CountryPort.CountryPortType.GetCountry",
            "CountryPortType",
            "CountryPort",
            "elements:GetCountryRequest",
        ),
        (
            "country-v2-qname-prefix-variant.wsdl",
            COUNTRY_WSDL_2_0_QNAME_PREFIX_VARIANT,
            "2.0",
            "CountryService.CountryEndpoint.CountryInterface.GetCountry",
            "CountryInterface",
            "CountryEndpoint",
            "elements:GetCountryRequest",
        ),
    ],
)
def test_wsdl_shared_extraction_context_resolves_operation_links_by_namespace_identity(
    rc: RawConfig,
    tmp_path: Path,
    filename: str,
    wsdl: str,
    version: str,
    expected_external: str,
    expected_interface: str,
    expected_port: str,
    expected_element_raw: str,
):
    path = tmp_path / filename
    path.write_text(wsdl)

    context = create_test_context(rc)
    manifest = WsdlManifest()
    manifest.path = str(path)
    normalized_path = normalize_wsdl_path(manifest.path)
    root, prefixes = _read_xml(normalized_path)

    wsdl_context = _extract_wsdl_read_context(
        context,
        manifest,
        normalized_path,
        None,
        root,
        prefixes,
        version=version,
    )

    assert len(wsdl_context.operation_links) == 1

    operation_link = wsdl_context.operation_links[0]
    request_message = operation_link.messages[0].message
    response_message = operation_link.messages[1].message

    assert operation_link.interface_name == expected_interface
    assert operation_link.port_name == expected_port
    assert operation_link.resource["external"] == expected_external
    assert operation_link.interface_qname is not None
    assert operation_link.interface_qname.expanded_name == f"{{urn:country}}{expected_interface}"
    assert operation_link.operation_qname is not None
    assert operation_link.operation_qname.expanded_name == "{urn:country}GetCountry"
    assert request_message is not None
    assert request_message.element_qname is not None
    assert request_message.element_qname.raw == expected_element_raw
    assert request_message.element_qname.expanded_name == "{urn:country}GetCountryRequest"
    assert response_message is not None
    assert response_message.element_qname is not None
    assert response_message.element_qname.expanded_name == "{urn:country}GetCountryResponse"


def test_wsdl_shared_extraction_context_keeps_cross_namespace_same_local_names_distinct(
    rc: RawConfig,
    tmp_path: Path,
):
    path = tmp_path / "country-cross-namespace-collision.wsdl"
    referenced_schema = tmp_path / "country-types.xsd"
    path.write_text(_build_cross_namespace_same_local_name_wsdl(referenced_schema.name))
    referenced_schema.write_text(CROSS_NAMESPACE_COLLIDING_RESPONSE_TYPES_XSD)

    context = create_test_context(rc)
    manifest = WsdlManifest()
    manifest.path = str(path)
    normalized_path = normalize_wsdl_path(manifest.path)
    root, prefixes = _read_xml(normalized_path)

    wsdl_context = _extract_wsdl_read_context(
        context,
        manifest,
        normalized_path,
        None,
        root,
        prefixes,
        version="1.1",
    )

    assert "{urn:country}GetCountryResponse" in wsdl_context.element_types
    assert "{urn:country-types}GetCountryResponse" in wsdl_context.element_types
    assert {field["name"] for field in wsdl_context.element_types["{urn:country}GetCountryResponse"]} == {"name", "population"}
    assert {field["name"] for field in wsdl_context.element_types["{urn:country-types}GetCountryResponse"]} == {"foreign_name"}

    response_message = wsdl_context.messages["{urn:country}GetCountryOutput"]
    assert response_message.element_qname is not None
    assert response_message.element_qname.expanded_name == "{urn:country}GetCountryResponse"
    assert {field["name"] for field in response_message.fields or []} == {"name", "population"}


def test_wsdl_shared_extraction_context_collects_multipart_message_parts(
    rc: RawConfig,
    tmp_path: Path,
):
    path = tmp_path / "multipart.wsdl"
    path.write_text(MULTIPART_ELEMENT_WSDL)

    context = create_test_context(rc)
    manifest = WsdlManifest()
    manifest.path = str(path)
    normalized_path = normalize_wsdl_path(manifest.path)
    root, prefixes = _read_xml(normalized_path)

    wsdl_context = _extract_wsdl_read_context(
        context,
        manifest,
        normalized_path,
        None,
        root,
        prefixes,
        version="1.1",
    )

    assert {
        "GetCountryRequestHeader",
        "GetCountryRequestBody",
        "GetCountryResponseMeta",
        "GetCountryResponseBody",
    }.issubset(wsdl_context.raw_schema_models)

    input_message = wsdl_context.messages["{urn:multipart}GetCountryInput"]
    output_message = wsdl_context.messages["{urn:multipart}GetCountryOutput"]
    operation_link = wsdl_context.operation_links[0]
    request_message = operation_link.messages[0].message
    response_message = operation_link.messages[1].message

    assert input_message.element_qname is None
    assert input_message.parts is not None
    assert [part.name for part in input_message.parts] == ["header", "body"]
    assert [part.element_qname.expanded_name if part.element_qname else None for part in input_message.parts] == [
        "{urn:multipart}GetCountryRequestHeader",
        "{urn:multipart}GetCountryRequestBody",
    ]
    assert {field["name"] for field in input_message.fields or []} == {"request_id", "code"}
    assert {field["source"] for field in input_message.fields or []} == {"header/request_id", "body/code"}

    assert output_message.element_qname is None
    assert output_message.parts is not None
    assert [part.name for part in output_message.parts] == ["meta", "payload"]
    assert [part.element_qname.expanded_name if part.element_qname else None for part in output_message.parts] == [
        "{urn:multipart}GetCountryResponseMeta",
        "{urn:multipart}GetCountryResponseBody",
    ]
    assert {field["name"] for field in output_message.fields or []} == {"status", "name"}
    assert {field["source"] for field in output_message.fields or []} == {"meta/status", "payload/name"}

    assert request_message is not None
    assert request_message.parts is not None
    assert {field["name"] for field in request_message.fields or []} == {"request_id", "code"}
    assert response_message is not None
    assert response_message.parts is not None
    assert {field["name"] for field in response_message.fields or []} == {"status", "name"}


def test_wsdl_shared_extraction_context_resolves_element_scoped_rebound_prefixes(
    rc: RawConfig,
    tmp_path: Path,
):
    path = tmp_path / "country-prefix-rebinding.wsdl"
    path.write_text(COUNTRY_WSDL_ELEMENT_SCOPED_PREFIX_REBINDING)

    context = create_test_context(rc)
    manifest = WsdlManifest()
    manifest.path = str(path)
    normalized_path = normalize_wsdl_path(manifest.path)
    root, prefixes = _read_xml(normalized_path)

    wsdl_context = _extract_wsdl_read_context(
        context,
        manifest,
        normalized_path,
        None,
        root,
        prefixes,
        version="1.1",
    )

    assert len(wsdl_context.operation_links) == 1

    operation_link = wsdl_context.operation_links[0]
    request_message = operation_link.messages[0].message
    response_message = operation_link.messages[1].message

    assert operation_link.service_name == "CountryService"
    assert operation_link.port_name == "CountryPort"
    assert operation_link.resource["external"] == "CountryService.CountryPort.CountryPortType.GetCountry"
    assert request_message is not None
    assert request_message.element_qname is not None
    assert request_message.element_qname.expanded_name == "{urn:country}GetCountryRequest"
    assert response_message is not None
    assert response_message.element_qname is not None
    assert response_message.element_qname.expanded_name == "{urn:country}GetCountryResponse"


def test_wsdl_shared_extraction_context_carries_raw_schema_models(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "nested.wsdl"
    path.write_text(NESTED_TYPES_WSDL)

    context = create_test_context(rc)
    manifest = WsdlManifest()
    manifest.path = str(path)
    normalized_path = normalize_wsdl_path(manifest.path)
    root, prefixes = _read_xml(normalized_path)

    wsdl_context = _extract_wsdl_read_context(
        context,
        manifest,
        normalized_path,
        None,
        root,
        prefixes,
        version="1.1",
    )

    assert {"GetNestedRequest", "GetNestedResponse", "Location"}.issubset(wsdl_context.raw_schema_models)
    assert wsdl_context.raw_schema_models["GetNestedRequest"]["properties"]["code"]["type"] == "string"
    assert wsdl_context.raw_schema_models["GetNestedResponse"]["properties"]["location"]["type"] == "ref"
    assert wsdl_context.raw_schema_models["GetNestedResponse"]["properties"]["location"]["model"] == "Location"
    assert wsdl_context.raw_schema_models["Location"]["features"] == "/:part"
    assert wsdl_context.raw_schema_models["Location"]["properties"]["city"]["type"] == "string"
    assert wsdl_context.raw_schema_models["Location"]["properties"]["zip"]["type"] == "integer"


def test_wsdl_shared_extraction_context_collects_referenced_schema_diagnostics(
    rc: RawConfig,
    tmp_path: Path,
):
    path = tmp_path / "country-partial-ref.wsdl"
    path.write_text(_build_partial_local_reference_wsdl("missing-country-types.xsd"))

    context = create_test_context(rc)
    manifest = WsdlManifest()
    manifest.path = str(path)
    normalized_path = normalize_wsdl_path(manifest.path)
    root, prefixes = _read_xml(normalized_path)

    wsdl_context = _extract_wsdl_read_context(
        context,
        manifest,
        normalized_path,
        None,
        root,
        prefixes,
        version="1.1",
    )

    assert len(wsdl_context.schema_diagnostics) == 1
    assert isinstance(wsdl_context.schema_diagnostics[0], ReferencedSchemaFileDoesNotExist)


def test_wsdl_store_schema_diagnostics_command_normalizes_path_for_context_storage(
    rc: RawConfig,
    tmp_path: Path,
):
    path = tmp_path / "country-partial-ref.wsdl"
    path.write_text(_build_partial_local_reference_wsdl("missing-country-types.xsd"))

    context = create_test_context(rc)
    manifest = WsdlManifest()
    diagnostic = ReferencedSchemaFileDoesNotExist(
        path=str(tmp_path / "missing-country-types.xsd"),
    )

    commands.store_schema_diagnostics(
        context,
        manifest,
        f"wsdl:{path}",
        [diagnostic],
    )

    assert manifest.schema_diagnostics == [diagnostic]
    assert context.get("wsdl.schema_diagnostics") == [
        (str(path), str(diagnostic)),
    ]


def test_wsdl_referenced_schema_loading_is_owned_by_wsdl_seam(
    rc: RawConfig,
    monkeypatch,
    tmp_path: Path,
):
    path = tmp_path / "country-remote-ref.wsdl"
    schema_url = "https://example.com/country-types.xsd"
    path.write_text(_build_remote_reference_wsdl(schema_url))

    def fake_urlopen(request_url: str):
        assert request_url == schema_url
        return BytesIO(REMOTE_REFERENCED_RESPONSE_TYPES_XSD.encode())

    def fail_xsd2_urlopen(*args, **kwargs):
        raise AssertionError("WSDL referenced-schema loading should not use xsd2.helpers.urlopen.")

    monkeypatch.setattr("spinta.manifests.wsdl.xsd.helpers.urlopen", fake_urlopen)
    monkeypatch.setattr("spinta.manifests.xsd.helpers.urlopen", fail_xsd2_urlopen)

    context, manifest = load_manifest_and_context(rc, path)

    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")

    assert {"name", "population"}.issubset(response_model.properties)


def test_wsdl_embedded_schemas_delegate_to_xsd2_parser_seam(
    rc: RawConfig,
    monkeypatch,
    tmp_path: Path,
):
    path = tmp_path / "country.wsdl"
    path.write_text(COUNTRY_WSDL)

    captured: dict[str, Any] = {}

    from spinta.manifests.xsd.helpers import XSDReader

    original_start = XSDReader.start

    def track_xsd2_reader_start(self):
        captured.setdefault("reader_paths", []).append(self._path)
        return original_start(self)

    monkeypatch.setattr("spinta.manifests.wsdl.xsd.helpers.XSDReader.start", track_xsd2_reader_start)

    context, manifest = load_manifest_and_context(rc, path)

    request_model = commands.get_model(context, manifest, "services/country_service/GetCountryRequest")
    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")

    assert captured["reader_paths"]
    assert all(reader_path.endswith(".xsd") for reader_path in captured["reader_paths"])
    assert request_model.properties["code"].dtype.name == "string"
    assert response_model.properties["population"].dtype.name == "integer"
    assert response_model.properties["population"].dtype.required is False
