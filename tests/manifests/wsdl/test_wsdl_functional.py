from io import BytesIO
from pathlib import Path
from typing import Any
from typing import cast
from urllib.error import URLError
from urllib.parse import parse_qs, urlparse

import pytest

from spinta import commands
from spinta.components import Model
from spinta.components import Property
from spinta.core.config import RawConfig
from spinta.exceptions import AmbiguousWsdlReference
from spinta.exceptions import MalformedWsdlFile
from spinta.exceptions import ManifestFileDoesNotExist
from spinta.exceptions import UnsupportedWsdlVersion
from spinta.manifests.dict.components import XmlManifest
from spinta.manifests.helpers import clone_manifest
from spinta.manifests.helpers import detect_manifest_from_path
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.wsdl.components import WsdlManifest
from spinta.manifests.wsdl.helpers import normalize_wsdl_path
from spinta.manifests.wsdl.helpers import read_schema
from spinta.manifests.wsdl.helpers import read_wsdl_document_version
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


def _without_wsdl_types(wsdl: str) -> str:
    start = wsdl.index("    <wsdl:types>\n")
    end = wsdl.index("    </wsdl:types>\n") + len("    </wsdl:types>\n")
    return wsdl[:start] + wsdl[end:]


COUNTRY_WSDL_WITHOUT_EMBEDDED_XSD = _without_wsdl_types(COUNTRY_WSDL)


def _user_property_names(model: Model) -> set[str]:
    return {name for name in model.properties if not name.startswith("_")}


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


def test_wsdl_detect_from_path_prefers_explicit_prefix(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "service.xml"
    path.write_text('<wsdl:definitions xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/" />')

    manifest = detect_manifest_from_path(rc, f"wsdl+file://{path}")

    assert manifest is WsdlManifest


@pytest.mark.parametrize(
    "path_factory",
    [
        lambda local_path: f"wsdl+http://example.com/{local_path.name}",
        lambda local_path: f"wsdl+https://example.com/{local_path.name}",
        lambda local_path: f"wsdl:{local_path}",
    ],
)
def test_wsdl_detect_from_path_supports_all_explicit_markers(
    rc: RawConfig,
    tmp_path: Path,
    path_factory,
):
    path = tmp_path / "service.xml"
    path.write_text('<wsdl:definitions xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/" />')

    manifest = detect_manifest_from_path(rc, path_factory(path))

    assert manifest is WsdlManifest


def test_wsdl_detect_from_path_supports_wsdl_extension(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "service.wsdl"
    path.write_text('<wsdl:definitions xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/" />')

    manifest = detect_manifest_from_path(rc, str(path))

    assert manifest is WsdlManifest


def test_wsdl_detect_from_path_supports_remote_query_marker(rc: RawConfig):
    manifest = detect_manifest_from_path(rc, "https://example.com/service?wsdl")

    assert manifest is WsdlManifest


def test_wsdl_reader_detects_wsdl_1_1_namespace(tmp_path: Path):
    path = tmp_path / "country.wsdl"
    path.write_text(COUNTRY_WSDL)

    assert read_wsdl_document_version(str(path)) == "1.1"


def test_wsdl_reader_detects_wsdl_2_0_namespace(tmp_path: Path):
    path = tmp_path / "country-v2.wsdl"
    path.write_text(WSDL_2_0_DETECTION_WSDL)

    assert read_wsdl_document_version(str(path)) == "2.0"


def test_wsdl_reader_rejects_unknown_wsdl_namespace_cleanly(tmp_path: Path):
    path = tmp_path / "unknown.wsdl"
    path.write_text('<wsdl:definitions xmlns:wsdl="urn:unsupported:wsdl" name="BrokenService" />')

    with pytest.raises(UnsupportedWsdlVersion, match="urn:unsupported:wsdl"):
        read_wsdl_document_version(str(path))


def test_plain_xml_detects_generic_xml_manifest(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "service.xml"
    path.write_text("<root><value>1</value></root>")

    manifest = detect_manifest_from_path(rc, str(path))

    assert manifest is XmlManifest


def test_wsdl_normalize_local_prefixed_path(tmp_path: Path):
    path = tmp_path / "service.wsdl"
    path.write_text('<wsdl:definitions xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/" />')

    assert normalize_wsdl_path(f"wsdl+file://{path}") == str(path)


@pytest.mark.parametrize(
    ("raw_path", "normalized_path"),
    [
        ("wsdl+http://example.com/service.wsdl", "http://example.com/service.wsdl"),
        ("wsdl+https://example.com/service.wsdl", "https://example.com/service.wsdl"),
    ],
)
def test_wsdl_normalize_remote_prefixed_paths(raw_path: str, normalized_path: str):
    assert normalize_wsdl_path(raw_path) == normalized_path


def test_wsdl_manifest_rejects_missing_local_file(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "missing.wsdl"

    with pytest.raises(ManifestFileDoesNotExist):
        load_manifest_and_context(rc, path)


def test_wsdl_manifest_loads_remote_url_without_local_path_check(rc: RawConfig, monkeypatch):
    url = "https://example.com/country?wsdl"

    def fail_if_checked(*args, **kwargs):
        raise AssertionError("remote WSDL path should not be validated as a local file")

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

    monkeypatch.setattr("spinta.manifests.wsdl.commands.configure.check_manifest_path", fail_if_checked)
    monkeypatch.setattr("spinta.manifests.wsdl.helpers.urllib.request.urlopen", fake_urlopen)

    context, manifest = load_manifest_and_context(rc, url)

    dataset = commands.get_dataset(context, manifest, "services/country_service")
    assert dataset.resources["contract"].type == "wsdl"
    assert dataset.resources["contract"].external == url


def test_wsdl_malformed_local_xml_is_user_visible(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "broken.wsdl"
    path.write_text(
        """
        <wsdl:definitions xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/">
            <wsdl:types>
                <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
            </wsdl:types>
        </wsdl:definitions>
        """
    )

    with pytest.raises(MalformedWsdlFile) as excinfo:
        load_manifest_and_context(rc, path)

    assert str(path) in str(excinfo.value)
    assert "mismatched tag" in str(excinfo.value)


def test_wsdl_malformed_remote_xml_is_user_visible(rc: RawConfig, monkeypatch):
    url = "https://example.com/broken?wsdl"

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
        assert request_url == url
        return FakeResponse(b'<wsdl:definitions xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"><wsdl:types></wsdl:definitions>')

    monkeypatch.setattr("spinta.manifests.wsdl.helpers.urllib.request.urlopen", fake_urlopen)

    with pytest.raises(MalformedWsdlFile) as excinfo:
        load_manifest_and_context(rc, url)

    assert url in str(excinfo.value)
    assert "mismatched tag" in str(excinfo.value)


def test_wsdl_read_schema_yields_manifest_schema_dicts(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country.wsdl"
    path.write_text(COUNTRY_WSDL)

    context = create_test_context(rc)
    manifest = WsdlManifest()
    manifest.path = str(path)

    schemas = [schema for _, schema in read_schema(context, manifest, manifest.path) if schema is not None]

    assert schemas
    assert schemas[0]["type"] == "dataset"
    assert all(isinstance(schema, dict) for schema in schemas)
    assert all(not isinstance(schema, Model) for schema in schemas)
    assert any(schema["type"] == "model" for schema in schemas[1:])


def test_wsdl_read_schema_yields_dataset_then_raw_and_operation_models(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "nested.wsdl"
    path.write_text(NESTED_TYPES_WSDL)

    context = create_test_context(rc)
    manifest = WsdlManifest()
    manifest.path = str(path)

    schemas = [schema for _, schema in read_schema(context, manifest, manifest.path) if schema is not None]
    observed = [(schema["type"], schema.get("name")) for schema in schemas]

    assert observed == [
        ("dataset", "services/nested_service"),
        ("model", "services/nested_service/schema/GetNestedRequest"),
        ("model", "services/nested_service/schema/GetNestedResponse"),
        ("model", "services/nested_service/schema/Location"),
        ("model", "services/nested_service/GetNestedRequest"),
        ("model", "services/nested_service/GetNestedResponse"),
    ]


def test_wsdl_2_0_read_schema_yields_dataset_then_raw_and_operation_models(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "nested-2-0.wsdl"
    path.write_text(NESTED_TYPES_WSDL_2_0)

    context = create_test_context(rc)
    manifest = WsdlManifest()
    manifest.path = str(path)

    schemas = [schema for _, schema in read_schema(context, manifest, manifest.path) if schema is not None]
    observed = [(schema["type"], schema.get("name")) for schema in schemas]

    assert observed == [
        ("dataset", "services/nested_service"),
        ("model", "services/nested_service/schema/GetNestedRequest"),
        ("model", "services/nested_service/schema/GetNestedResponse"),
        ("model", "services/nested_service/schema/Location"),
        ("model", "services/nested_service/GetNestedRequest"),
        ("model", "services/nested_service/GetNestedResponse"),
    ]


def test_wsdl_loaded_manifest_materializes_runtime_models_and_properties(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country.wsdl"
    path.write_text(COUNTRY_WSDL)

    context, manifest = load_manifest_and_context(rc, path)

    request_model = commands.get_model(context, manifest, "services/country_service/GetCountryRequest")
    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")

    assert isinstance(request_model, Model)
    assert isinstance(response_model, Model)
    assert all(isinstance(prop, Property) for prop in request_model.properties.values())
    assert all(isinstance(prop, Property) for prop in response_model.properties.values())


def test_wsdl_without_embedded_xsd_still_generates_operation_models(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country-no-types.wsdl"
    path.write_text(COUNTRY_WSDL_WITHOUT_EMBEDDED_XSD)

    context, manifest = load_manifest_and_context(rc, path)
    source_manifest = WsdlManifest()
    source_manifest.path = str(path)
    schemas = [schema for _, schema in read_schema(context, source_manifest, str(path)) if schema is not None]
    observed = [(schema["type"], schema.get("name")) for schema in schemas]
    request_model = commands.get_model(context, manifest, "services/country_service/GetCountryRequest")
    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")
    dataset = commands.get_dataset(context, manifest, "services/country_service")

    assert observed == [
        ("dataset", "services/country_service"),
        ("model", "services/country_service/GetCountryRequest"),
        ("model", "services/country_service/GetCountryResponse"),
    ]
    assert dataset.resources["contract"].type == "wsdl"
    assert dataset.resources["country_port_get_country"].type == "soap"
    assert _user_property_names(request_model) == set()
    assert _user_property_names(response_model) == set()


def test_wsdl_read_schema_output_loads_into_runtime_models_via_generic_loader(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "nested.wsdl"
    path.write_text(NESTED_TYPES_WSDL)

    context, _ = load_manifest_and_context(rc, path)
    source_manifest = WsdlManifest()
    source_manifest.path = str(path)
    target_manifest = clone_manifest(cast(Any, context), "wsdl-generic-loader")

    schemas = read_schema(context, source_manifest, str(path))
    load_manifest_nodes(cast(Any, context), target_manifest, schemas, source=source_manifest, link=True)

    raw_response_model = commands.get_model(context, target_manifest, "services/nested_service/schema/GetNestedResponse")
    raw_location_model = commands.get_model(context, target_manifest, "services/nested_service/schema/Location")
    operation_request_model = commands.get_model(context, target_manifest, "services/nested_service/GetNestedRequest")
    operation_response_model = commands.get_model(context, target_manifest, "services/nested_service/GetNestedResponse")

    assert isinstance(raw_response_model, Model)
    assert isinstance(raw_location_model, Model)
    assert isinstance(operation_request_model, Model)
    assert isinstance(operation_response_model, Model)
    assert raw_response_model.external.resource.name == "contract"
    assert raw_location_model.external.resource.name == "contract"
    assert operation_request_model.external.resource.name == "nested_port_get_nested"
    assert operation_response_model.external.resource.name == "nested_port_get_nested"


def test_wsdl_raw_schema_models_attach_to_contract_resource(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "nested.wsdl"
    path.write_text(NESTED_TYPES_WSDL)

    context, manifest = load_manifest_and_context(rc, path)

    raw_response_model = commands.get_model(context, manifest, "services/nested_service/schema/GetNestedResponse")
    raw_location_model = commands.get_model(context, manifest, "services/nested_service/schema/Location")
    operation_response_model = commands.get_model(context, manifest, "services/nested_service/GetNestedResponse")
    raw_location_dtype = raw_response_model.properties["location"].dtype
    raw_location_ref_model = getattr(raw_location_dtype, "model", None)

    assert raw_response_model.external.resource.name == "contract"
    assert raw_location_model.external.resource.name == "contract"
    assert raw_location_dtype.name == "ref"
    assert raw_location_ref_model is not None
    assert raw_location_ref_model.name == "services/nested_service/schema/Location"
    assert operation_response_model.external.resource.name != "contract"


def test_wsdl_raw_schema_models_keep_contract_ownership_when_multiple_operations_exist(
    rc: RawConfig,
    tmp_path: Path,
):
    path = tmp_path / "registry.wsdl"
    path.write_text(MULTI_OPERATION_WSDL)

    context, manifest = load_manifest_and_context(rc, path)

    raw_model_names = {
        "services/registry_service/schema/GetCountryRequest",
        "services/registry_service/schema/GetCountryResponse",
        "services/registry_service/schema/ListCountriesRequest",
        "services/registry_service/schema/ListCountriesResponse",
    }
    operation_model_resources = {
        "services/registry_service/GetCountryRequest": "registry_port_get_country",
        "services/registry_service/GetCountryResponse": "registry_port_get_country",
        "services/registry_service/ListCountriesRequest": "registry_port_list_countries",
        "services/registry_service/ListCountriesResponse": "registry_port_list_countries",
    }

    for model_name in raw_model_names:
        model = commands.get_model(context, manifest, model_name)
        assert model.external.resource.name == "contract"

    for model_name, resource_name in operation_model_resources.items():
        model = commands.get_model(context, manifest, model_name)
        assert model.external.resource.name == resource_name


def test_wsdl_operation_models_preserve_behavior_when_raw_schema_models_exist(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "nested.wsdl"
    path.write_text(NESTED_TYPES_WSDL)

    context, manifest = load_manifest_and_context(rc, path)

    raw_response_model = commands.get_model(context, manifest, "services/nested_service/schema/GetNestedResponse")
    request_model = commands.get_model(context, manifest, "services/nested_service/GetNestedRequest")
    response_model = commands.get_model(context, manifest, "services/nested_service/GetNestedResponse")

    assert raw_response_model.external.resource.name == "contract"
    assert request_model.external.resource.name == "nested_port_get_nested"
    assert response_model.external.resource.name == "nested_port_get_nested"
    assert {"location_city", "location_zip"}.issubset(response_model.properties)
    assert response_model.properties["location_city"].external.name == "location/city"
    assert response_model.properties["location_zip"].external.name == "location/zip"


def test_wsdl_manifest_generates_contract_and_operation_models(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country.wsdl"
    path.write_text(COUNTRY_WSDL)

    context, manifest = load_manifest_and_context(rc, path)

    dataset = commands.get_dataset(context, manifest, "services/country_service")
    soap_resource = dataset.resources["country_port_get_country"]
    request_model = commands.get_model(context, manifest, "services/country_service/GetCountryRequest")
    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")

    assert dataset.resources["contract"].type == "wsdl"
    assert dataset.resources["contract"].external == str(path)
    assert soap_resource.type == "soap"
    assert str(soap_resource.prepare) == "wsdl(contract)"
    assert soap_resource.external == "CountryService.CountryPort.CountryPortType.GetCountry"
    assert request_model.external.resource.name == "country_port_get_country"
    assert response_model.external.resource.name == "country_port_get_country"
    assert {"code"}.issubset(request_model.properties)
    assert {"name", "population"}.issubset(response_model.properties)


def test_wsdl_reader_resolves_same_namespace_with_different_prefixes(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country-qname-prefix-variant.wsdl"
    path.write_text(COUNTRY_WSDL_QNAME_PREFIX_VARIANT)

    context, manifest = load_manifest_and_context(rc, path)

    dataset = commands.get_dataset(context, manifest, "services/country_service")
    soap_resource = dataset.resources["country_port_get_country"]
    request_model = commands.get_model(context, manifest, "services/country_service/GetCountryRequest")
    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")

    assert dataset.resources["contract"].type == "wsdl"
    assert soap_resource.external == "CountryService.CountryPort.CountryPortType.GetCountry"
    assert request_model.external.resource.name == "country_port_get_country"
    assert response_model.external.resource.name == "country_port_get_country"


def test_wsdl_reader_resolves_default_namespace_references(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country-default-namespace.wsdl"
    path.write_text(COUNTRY_WSDL_DEFAULT_NAMESPACE_REFERENCES)

    context, manifest = load_manifest_and_context(rc, path)

    dataset = commands.get_dataset(context, manifest, "services/country_service")
    soap_resource = dataset.resources["country_port_get_country"]
    request_model = commands.get_model(context, manifest, "services/country_service/GetCountryRequest")
    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")

    assert dataset.resources["contract"].type == "wsdl"
    assert soap_resource.external == "CountryService.CountryPort.CountryPortType.GetCountry"
    assert request_model.external.resource.name == "country_port_get_country"
    assert response_model.external.resource.name == "country_port_get_country"


def test_wsdl_embedded_schema_resolves_default_namespace_types(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country-default-schema-types.wsdl"
    path.write_text(COUNTRY_WSDL_DEFAULT_NAMESPACE_SCHEMA_TYPES)

    context, manifest = load_manifest_and_context(rc, path)

    request_model = commands.get_model(context, manifest, "services/country_service/GetCountryRequest")
    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")

    assert request_model.properties["code"].dtype.name == "string"
    assert response_model.properties["name"].dtype.name == "string"
    assert response_model.properties["population"].dtype.name == "integer"
    assert response_model.properties["population"].dtype.required is False


def test_wsdl_embedded_schema_resolves_cross_namespace_qname_references(
    rc: RawConfig,
    tmp_path: Path,
):
    path = tmp_path / "country-cross-namespace.wsdl"
    referenced_schema = tmp_path / "country-types.xsd"
    path.write_text(_build_cross_namespace_embedded_reference_wsdl(referenced_schema.name))
    referenced_schema.write_text(CROSS_NAMESPACE_REFERENCED_TYPES_XSD)

    context, manifest = load_manifest_and_context(rc, path)

    request_model = commands.get_model(context, manifest, "services/country_service/GetCountryRequest")
    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")

    assert request_model.properties["code"].dtype.name == "string"
    assert response_model.properties["name"].dtype.name == "string"
    assert response_model.properties["population"].dtype.name == "integer"
    assert response_model.properties["population"].dtype.required is False


def test_wsdl_reader_reports_namespace_ambiguity_cleanly(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country-duplicate-message.wsdl"
    path.write_text(COUNTRY_WSDL_DUPLICATE_MESSAGE)

    with pytest.raises(AmbiguousWsdlReference) as excinfo:
        load_manifest_and_context(rc, path)

    message = str(excinfo.value)
    assert str(path) in message
    assert "GetCountryInput" in message
    assert "Duplicate expanded QName conflict" in message


def test_wsdl_embedded_schema_reports_duplicate_qname_conflict_cleanly(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country-duplicate-embedded-element.wsdl"
    path.write_text(COUNTRY_WSDL_DUPLICATE_EMBEDDED_ELEMENT)

    with pytest.raises(AmbiguousWsdlReference) as excinfo:
        load_manifest_and_context(rc, path)

    message = str(excinfo.value)
    assert str(path) in message
    assert "GetCountryResponse" in message
    assert "wsdl:types/xs:element" in message
    assert "Duplicate expanded QName conflict" in message


def test_wsdl_embedded_schema_reports_type_resolution_ambiguity_cleanly(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country-duplicate-embedded-type.wsdl"
    path.write_text(COUNTRY_WSDL_DUPLICATE_EMBEDDED_TYPE)

    with pytest.raises(AmbiguousWsdlReference) as excinfo:
        load_manifest_and_context(rc, path)

    message = str(excinfo.value)
    assert str(path) in message
    assert "GetCountryResponse" in message
    assert "multiple embedded schema model candidates" in message


def test_wsdl_2_0_duplicate_binding_reports_ambiguity_cleanly(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country-duplicate-binding-v2.wsdl"
    path.write_text(COUNTRY_WSDL_2_0_DUPLICATE_BINDING)

    with pytest.raises(AmbiguousWsdlReference) as excinfo:
        load_manifest_and_context(rc, path)

    message = str(excinfo.value)
    assert str(path) in message
    assert "CountryBinding" in message
    assert "wsdl:binding" in message
    assert "Duplicate expanded QName conflict" in message


def test_wsdl_2_0_duplicate_service_reports_ambiguity_cleanly(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country-duplicate-service-v2.wsdl"
    path.write_text(COUNTRY_WSDL_2_0_DUPLICATE_SERVICE)

    with pytest.raises(AmbiguousWsdlReference) as excinfo:
        load_manifest_and_context(rc, path)

    message = str(excinfo.value)
    assert str(path) in message
    assert "CountryService" in message
    assert "wsdl:service" in message
    assert "Duplicate expanded QName conflict" in message


def test_wsdl_2_0_manifest_generates_contract_and_operation_models(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country-v2.wsdl"
    path.write_text(COUNTRY_WSDL_2_0)

    context, manifest = load_manifest_and_context(rc, path)

    dataset = commands.get_dataset(context, manifest, "services/country_service")
    soap_resource = dataset.resources["country_endpoint_get_country"]
    request_model = commands.get_model(context, manifest, "services/country_service/GetCountryRequest")
    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")

    assert dataset.resources["contract"].type == "wsdl"
    assert dataset.resources["contract"].external == str(path)
    assert soap_resource.type == "soap"
    assert soap_resource.external == "CountryService.CountryEndpoint.CountryInterface.GetCountry"
    assert request_model.external.resource.name == "country_endpoint_get_country"
    assert response_model.external.resource.name == "country_endpoint_get_country"


def test_wsdl_manifest_maps_xsd_scalar_types(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "scalars.wsdl"
    path.write_text(SCALAR_TYPES_WSDL)

    context, manifest = load_manifest_and_context(rc, path)

    response_model = commands.get_model(context, manifest, "services/scalar_service/GetScalarResponse")

    assert response_model.properties["name"].dtype.name == "string"
    assert response_model.properties["enabled"].dtype.name == "boolean"
    assert response_model.properties["created_on"].dtype.name == "date"
    assert response_model.properties["updated_at"].dtype.name == "datetime"
    assert response_model.properties["opens_at"].dtype.name == "time"
    assert response_model.properties["score"].dtype.name == "number"
    assert response_model.properties["count"].dtype.name == "integer"
    assert response_model.properties["website"].dtype.name == "url"


def test_wsdl_2_0_manifest_maps_xsd_scalar_types(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "scalars-v2.wsdl"
    path.write_text(SCALAR_TYPES_WSDL_2_0)

    context, manifest = load_manifest_and_context(rc, path)

    response_model = commands.get_model(context, manifest, "services/scalar_service/GetScalarResponse")

    assert response_model.properties["name"].dtype.name == "string"
    assert response_model.properties["enabled"].dtype.name == "boolean"
    assert response_model.properties["created_on"].dtype.name == "date"
    assert response_model.properties["updated_at"].dtype.name == "datetime"
    assert response_model.properties["opens_at"].dtype.name == "time"
    assert response_model.properties["score"].dtype.name == "number"
    assert response_model.properties["count"].dtype.name == "integer"
    assert response_model.properties["website"].dtype.name == "url"


def test_wsdl_manifest_flattens_nested_complex_types(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "nested.wsdl"
    path.write_text(NESTED_TYPES_WSDL)

    context, manifest = load_manifest_and_context(rc, path)

    response_model = commands.get_model(context, manifest, "services/nested_service/GetNestedResponse")

    assert {"location_city", "location_zip"}.issubset(response_model.properties)
    assert response_model.properties["location_city"].external.name == "location/city"
    assert response_model.properties["location_zip"].external.name == "location/zip"


@pytest.mark.parametrize(
    "filename, wsdl, expected_resources",
    [
        (
            "registry.wsdl",
            MULTI_OPERATION_WSDL,
            {
                "registry_port_get_country": "RegistryService.RegistryPort.RegistryPortType.GetCountry",
                "registry_port_list_countries": "RegistryService.RegistryPort.RegistryPortType.ListCountries",
            },
        ),
        (
            "registry-v2.wsdl",
            MULTI_OPERATION_WSDL_2_0,
            {
                "registry_endpoint_get_country": "RegistryService.RegistryEndpoint.RegistryInterface.GetCountry",
                "registry_endpoint_list_countries": "RegistryService.RegistryEndpoint.RegistryInterface.ListCountries",
            },
        ),
    ],
)
def test_wsdl_operation_structure_is_preserved_per_operation(
    rc: RawConfig,
    tmp_path: Path,
    filename: str,
    wsdl: str,
    expected_resources: dict[str, str],
):
    path = tmp_path / filename
    path.write_text(wsdl)

    context, manifest = load_manifest_and_context(rc, path)

    dataset = commands.get_dataset(context, manifest, "services/registry_service")
    assert dataset.resources["contract"].type == "wsdl"
    assert dataset.resources["contract"].external == str(path)

    for resource_name, resource_external in expected_resources.items():
        resource = dataset.resources[resource_name]
        assert resource.type == "soap"
        assert resource.external == resource_external


@pytest.mark.parametrize(
    "filename, wsdl, expected_resource_paths",
    [
        (
            "registry.wsdl",
            MULTI_OPERATION_WSDL,
            {
                "registry_port_get_country": (
                    "RegistryService",
                    "RegistryPort",
                    "RegistryPortType",
                    "GetCountry",
                ),
                "registry_port_list_countries": (
                    "RegistryService",
                    "RegistryPort",
                    "RegistryPortType",
                    "ListCountries",
                ),
            },
        ),
        (
            "registry-v2.wsdl",
            MULTI_OPERATION_WSDL_2_0,
            {
                "registry_endpoint_get_country": (
                    "RegistryService",
                    "RegistryEndpoint",
                    "RegistryInterface",
                    "GetCountry",
                ),
                "registry_endpoint_list_countries": (
                    "RegistryService",
                    "RegistryEndpoint",
                    "RegistryInterface",
                    "ListCountries",
                ),
            },
        ),
    ],
)
def test_wsdl_soap_operation_identification_still_follows_supported_binding_contract(
    rc: RawConfig,
    tmp_path: Path,
    filename: str,
    wsdl: str,
    expected_resource_paths: dict[str, tuple[str, str, str, str]],
):
    path = tmp_path / filename
    path.write_text(wsdl)

    context, manifest = load_manifest_and_context(rc, path)

    dataset = commands.get_dataset(context, manifest, "services/registry_service")
    for resource_name, expected_path in expected_resource_paths.items():
        resource = dataset.resources[resource_name]
        assert resource.type == "soap"
        assert tuple(resource.external.split(".")) == expected_path


@pytest.mark.parametrize(
    "filename, wsdl, expected_resource_params",
    [
        (
            "registry.wsdl",
            MULTI_OPERATION_WSDL,
            {
                "registry_port_get_country": {
                    "style": "document",
                    "transport": "http://schemas.xmlsoap.org/soap/http",
                    "address": "https://example.com/registry",
                    "soapAction": "urn:registry#GetCountry",
                },
                "registry_port_list_countries": {
                    "style": "document",
                    "transport": "http://schemas.xmlsoap.org/soap/http",
                    "address": "https://example.com/registry",
                    "soapAction": "urn:registry#ListCountries",
                },
            },
        ),
        (
            "registry-v2.wsdl",
            MULTI_OPERATION_WSDL_2_0,
            {
                "registry_endpoint_get_country": {
                    "protocol": "http://www.w3.org/2003/05/soap/bindings/HTTP/",
                    "address": "https://example.com/registry",
                },
                "registry_endpoint_list_countries": {
                    "protocol": "http://www.w3.org/2003/05/soap/bindings/HTTP/",
                    "address": "https://example.com/registry",
                },
            },
        ),
    ],
)
def test_wsdl_soap_binding_metadata_is_preserved_in_generated_manifest(
    rc: RawConfig,
    tmp_path: Path,
    filename: str,
    wsdl: str,
    expected_resource_params: dict[str, dict[str, str]],
):
    path = tmp_path / filename
    path.write_text(wsdl)

    context, manifest = load_manifest_and_context(rc, path)

    dataset = commands.get_dataset(context, manifest, "services/registry_service")
    for resource_name, expected_params in expected_resource_params.items():
        resource = dataset.resources[resource_name]
        params = {param.name: param for param in resource.params}

        assert set(params) == set(expected_params)
        for param_name, expected_value in expected_params.items():
            assert params[param_name].source == [expected_value]


def test_wsdl_optional_soap_action_is_omitted_when_not_declared(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country-no-soap-action.wsdl"
    path.write_text(COUNTRY_WSDL_WITHOUT_SOAP_ACTION)

    context, manifest = load_manifest_and_context(rc, path)

    dataset = commands.get_dataset(context, manifest, "services/country_service")
    resource = dataset.resources["country_port_get_country"]
    params = {param.name: param for param in resource.params}

    assert set(params) == {"style", "transport", "address"}
    assert "soapAction" not in params


@pytest.mark.parametrize(
    "filename, wsdl, resource_name, expected_external, expected_params",
    [
        (
            "country-namespace-variant.wsdl",
            COUNTRY_WSDL_NAMESPACE_VARIANT,
            "country_port_get_country",
            "CountryService.CountryPort.CountryPortType.GetCountry",
            {
                "style": "document",
                "transport": "http://schemas.xmlsoap.org/soap/http",
                "address": "https://example.com/country",
                "soapAction": "urn:country#GetCountry",
            },
        ),
        (
            "country-v2-namespace-variant.wsdl",
            COUNTRY_WSDL_2_0_NAMESPACE_VARIANT,
            "country_endpoint_get_country",
            "CountryService.CountryEndpoint.CountryInterface.GetCountry",
            {
                "protocol": "http://www.w3.org/2003/05/soap/bindings/HTTP/",
                "address": "https://example.com/country",
            },
        ),
    ],
)
def test_wsdl_soap_namespace_handling_is_preserved_for_supported_bindings(
    rc: RawConfig,
    tmp_path: Path,
    filename: str,
    wsdl: str,
    resource_name: str,
    expected_external: str,
    expected_params: dict[str, str],
):
    path = tmp_path / filename
    path.write_text(wsdl)

    context, manifest = load_manifest_and_context(rc, path)

    dataset = commands.get_dataset(context, manifest, "services/country_service")
    resource = dataset.resources[resource_name]
    params = {param.name: param for param in resource.params}

    assert resource.type == "soap"
    assert resource.external == expected_external
    assert set(params) == set(expected_params)


def test_wsdl_local_schema_references_are_loaded_through_xsd2(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country-local-ref.wsdl"
    referenced_schema = tmp_path / "country-types.xsd"
    path.write_text(_build_local_reference_wsdl(referenced_schema.name))
    referenced_schema.write_text(LOCAL_REFERENCED_RESPONSE_TYPES_XSD)

    context, manifest = load_manifest_and_context(rc, path)

    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")

    assert {"name", "population"}.issubset(response_model.properties)
    assert response_model.properties["population"].dtype.required is False


def test_wsdl_remote_schema_references_are_loaded_through_xsd2(
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

    monkeypatch.setattr("spinta.manifests.wsdl.xsd.helpers.urlopen", fake_urlopen)

    context, manifest = load_manifest_and_context(rc, path)

    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")

    assert {"name", "population"}.issubset(response_model.properties)
    assert response_model.properties["population"].dtype.required is False


def test_wsdl_schema_reference_resolution_is_recursion_safe(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country-cyclic-ref.wsdl"
    primary_schema = tmp_path / "country-types-primary.xsd"
    secondary_schema = tmp_path / "country-types-secondary.xsd"
    path.write_text(_build_local_reference_wsdl(primary_schema.name))
    primary_schema.write_text(CYCLIC_REFERENCED_PRIMARY_TYPES_XSD)
    secondary_schema.write_text(CYCLIC_REFERENCED_SECONDARY_TYPES_XSD)

    context, manifest = load_manifest_and_context(rc, path)

    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")

    assert {"name", "population"}.issubset(response_model.properties)


def test_wsdl_missing_schema_fields_degrade_to_empty_operation_properties(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country-missing-ref.wsdl"
    path.write_text(_build_local_reference_wsdl("missing-country-types.xsd"))

    context, manifest = load_manifest_and_context(rc, path)
    source_manifest = WsdlManifest()
    source_manifest.path = str(path)
    request_model = commands.get_model(context, manifest, "services/country_service/GetCountryRequest")
    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")
    schemas = [schema for _, schema in read_schema(context, source_manifest, str(path)) if schema is not None]
    observed = [(schema["type"], schema.get("name")) for schema in schemas]

    assert observed == [
        ("dataset", "services/country_service"),
        ("model", "services/country_service/GetCountryRequest"),
        ("model", "services/country_service/GetCountryResponse"),
    ]
    assert _user_property_names(request_model) == set()
    assert _user_property_names(response_model) == set()


def test_wsdl_malformed_referenced_schema_degrades_to_empty_operation_properties(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country-malformed-ref.wsdl"
    referenced_schema = tmp_path / "country-types.xsd"
    path.write_text(_build_local_reference_wsdl(referenced_schema.name))
    referenced_schema.write_text('<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"><xs:element')

    context, manifest = load_manifest_and_context(rc, path)
    request_model = commands.get_model(context, manifest, "services/country_service/GetCountryRequest")
    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")

    assert _user_property_names(request_model) == set()
    assert _user_property_names(response_model) == set()


def test_wsdl_unreachable_remote_schema_reference_degrades_to_empty_operation_properties(
    rc: RawConfig,
    monkeypatch,
    tmp_path: Path,
):
    path = tmp_path / "country-remote-failure.wsdl"
    schema_url = "https://example.com/country-types.xsd"
    path.write_text(_build_remote_reference_wsdl(schema_url))

    def fail_urlopen(request_url: str):
        raise URLError("temporary failure in name resolution")

    monkeypatch.setattr("spinta.manifests.wsdl.xsd.helpers.urlopen", fail_urlopen)

    context, manifest = load_manifest_and_context(rc, path)
    request_model = commands.get_model(context, manifest, "services/country_service/GetCountryRequest")
    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")

    assert _user_property_names(request_model) == set()
    assert _user_property_names(response_model) == set()


def test_wsdl_partial_schema_graph_still_emits_available_models(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "country-partial-ref.wsdl"
    path.write_text(_build_partial_local_reference_wsdl("missing-country-types.xsd"))

    context, manifest = load_manifest_and_context(rc, path)
    source_manifest = WsdlManifest()
    source_manifest.path = str(path)
    schemas = [schema for _, schema in read_schema(context, source_manifest, str(path)) if schema is not None]
    model_names = {schema["name"] for schema in schemas[1:] if schema["type"] == "model"}
    request_model = commands.get_model(context, manifest, "services/country_service/GetCountryRequest")
    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")
    raw_request_model = commands.get_model(context, manifest, "services/country_service/schema/GetCountryRequest")

    assert "services/country_service/schema/GetCountryRequest" in model_names
    assert "services/country_service/schema/GetCountryResponse" not in model_names
    assert _user_property_names(request_model) == {"code"}
    assert _user_property_names(response_model) == set()
    assert raw_request_model.external.resource.name == "contract"