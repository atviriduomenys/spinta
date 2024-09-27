from __future__ import annotations

from typing import Any

from lxml.etree import _Element

from spinta.components import Context

xsd_example = """
    <xs:schema xmlns="http://eTaarPlat.ServiceContracts/2007/08/Messages" elementFormDefault="qualified" targetNamespace="http://eTaarPlat.ServiceContracts/2007/08/Messages" xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <xs:element name="getDocumentsByWagonResponse" nillable="true" type="getDocumentsByWagonResponse" />
        <xs:complexType name="getDocumentsByWagonResponse">
            <xs:sequence>
                <xs:element minOccurs="0" maxOccurs="1" name="searchParameters" type="getDocumentsByWagonSearchParams" />
                <xs:choice minOccurs="1" maxOccurs="1">
                    <xs:element minOccurs="0" maxOccurs="1" name="klaida" type="Klaida" />
                    <xs:element minOccurs="0" maxOccurs="1" name="extract" type="Extract" />
                </xs:choice>
            </xs:sequence>
        </xs:complexType>
        <xs:complexType name="getDocumentsByWagonSearchParams">
            <xs:sequence>
                <xs:element minOccurs="0" maxOccurs="1" name="searchType" type="xs:string" />
                <xs:element minOccurs="0" maxOccurs="1" name="code" type="xs:string" />
            </xs:sequence>
        </xs:complexType>
        <xs:complexType name="Klaida">
                    <xs:sequence>
                        <xs:element minOccurs="0" maxOccurs="1" name="Aprasymas" type="xs:string" />
                    </xs:sequence>
        </xs:complexType>
        <xs:complexType name="Extract">
                    <xs:sequence>
                        <xs:element minOccurs="1" maxOccurs="1" name="extractPreparationTime" type="xs:dateTime" />
                        <xs:element minOccurs="1" maxOccurs="1" name="lastUpdateTime" type="xs:dateTime" />
                    </xs:sequence>
        </xs:complexType>
    </xs:schema>
"""

class State:
    complex_types: dict[str, Any]
    simple_types: dict[str, Any]
    path = list[str]

    def __init__(self, context: Context):
        self.complex_types = {}

state = State()

# pirmas praėjimas - XML ir XSD logika
def process_element(state: State, node: _Element):
    name = ""
    state.path.append(node.g)
    return {"name": "getDocumentsByWagonResponse", "type": "model", "xsd":{"nillable": True, "node_type": "element", "type": "getDocumentsByWagonSearchParams"}}

def process_complex_type(state: State, node: _Element):
    state.complex_types[node.get("name")] = {}  # sudedam info
    # jei turi pavadinim1 - dedam pavadinima, jei ne - kitas atvejis (vidinis complexType)
    return {"xsd":{"name"}}

def process_sequence(state: State, node: _Element):
    pass



# antras praėjimas - DSA logika
def process_element_2(state: State, node: _Element):
    xsd_type = node.get("type")
    type = state.complex_types[xsd_type]
    # pagal simple type



    return {"type": "property", "name": node.get("name"), "dtype": "string"}

    if state._is_root:


    return {"type": "model", "name": node.get("name")}



    return {"type": "property", "name": node.get("name"), "dtype": "ref"}

