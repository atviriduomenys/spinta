from lxml import etree

from spinta.manifests.xsd.helpers import _get_description, _get_property_type, _node_to_partial_property, \
    _attributes_to_properties, _get_external_info, _simple_element_to_property, \
    _get_dataset_and_resource_info, _node_is_simple_type_or_inline, _is_array, _extract_custom_types, \
    _properties_from_references, _is_element


def test_get_description():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="klaida" type="xs:string">
        <xs:annotation><xs:documentation>Klaidos atveju - klaidos pranešimas</xs:documentation></xs:annotation>
    </xs:element>
    </xs:schema>
    """

    schema = etree.fromstring(element_string)
    element = schema.xpath('*[local-name() = "element"]')[0]
    result = _get_description(element)

    assert result == "Klaidos atveju - klaidos pranešimas"


def test_get_property_type():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="klaida" type="xs:string">
        <xs:annotation><xs:documentation>Klaidos atveju - klaidos pranešimas</xs:documentation></xs:annotation>
    </xs:element>
    </xs:schema>
    """

    schema = etree.fromstring(element_string)
    element = schema.xpath('*[local-name() = "element"]')[0]
    result = _get_property_type(element)

    assert result == "string"


def test_get_property_type_ref():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="klientu_saraso_rezultatas">
      <xs:complexType mixed="true">
        <xs:sequence>
          <xs:element ref="asmenys" minOccurs="0" maxOccurs="1" />
        </xs:sequence>
      </xs:complexType>
    </xs:element>
    </xs:schema>
    """

    schema = etree.fromstring(element_string)
    element = schema.xpath('//*[@ref="asmenys"]', )[0]
    print("ELEMENT:", element)
    result = _get_property_type(element)

    assert result == "ref"


def test_get_property_type_simple_type():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="CT_E200_FORMA">
      <xs:annotation><xs:documentation>E200 medicininės formos pavadinimas</xs:documentation></xs:annotation>
      <xs:simpleType>
        <xs:restriction base="xs:string">
          <xs:maxLength value="4"/>
        </xs:restriction>
      </xs:simpleType>
    </xs:element>
    </xs:schema>
    """

    schema = etree.fromstring(element_string)
    element = schema.xpath('*[local-name() = "element"]')[0]
    result = _get_property_type(element)

    assert result == "string"


def test_node_to_partial_property():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="CT_E200_FORMA">
      <xs:annotation><xs:documentation>E200 medicininės formos pavadinimas</xs:documentation></xs:annotation>
      <xs:simpleType>
        <xs:restriction base="xs:string">
          <xs:maxLength value="4"/>
        </xs:restriction>
      </xs:simpleType>
    </xs:element>
    </xs:schema>
    """

    schema = etree.fromstring(element_string)
    element = schema.xpath('*[local-name() = "element"]')[0]
    result1, result2 = _node_to_partial_property(element)

    assert result1 == "ct_e200_forma"
    assert result2 == {
        "description": "E200 medicininės formos pavadinimas",
        "external": {
            "name": "CT_E200_FORMA",
        },
        "type": "string"
    }
# todo test properties with refs


def test_simple_element_to_property():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="CT_E200_FORMA">
      <xs:annotation><xs:documentation>E200 medicininės formos pavadinimas</xs:documentation></xs:annotation>
      <xs:simpleType>
        <xs:restriction base="xs:string">
          <xs:maxLength value="4"/>
        </xs:restriction>
      </xs:simpleType>
    </xs:element>
    </xs:schema>
    """
    schema = etree.fromstring(element_string)
    element = schema.xpath('*[local-name() = "element"]')[0]
    result1, result2 = _simple_element_to_property(element)
    assert result1 == "ct_e200_forma"
    assert result2 == {
        "description": "E200 medicininės formos pavadinimas",
        "external": {
            "name": "CT_E200_FORMA/text()",
        },
        "type": "string",
        "required": True
    }


def test_attributes_to_properties():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="FIZINIAI_ASMENYS_NEID">
<xs:annotation>
<xs:documentation> Neidentifikuoti fiziniai asmenys. Atributų reikšmės: NEID_FIZ_ID - Neidentifikuoto fizinio asmens dirbtinis identifikatorius. FIZ_SAL_KODAS - Šalies kodas. FIZ_PASTABOS - Pastabos. FIZ_GIMIMO_DATA - Gimimo data. FAV_VARDAS - Vardas. FAV_PAVARDE - Pavardė. ASM_ADRESAS - Adresas. ARO_KODAS - Juridinio asmens aktualaus adreso kodas. ADR_BUS - Adreso būsena. Reikšmės: 1 - adresas užregistruotas adresų registre, 0 - Adreso Adresų registre nėra arba jis negaliojantis. </xs:documentation>
</xs:annotation>
<xs:complexType>
<xs:attribute name="NEID_FIZ_ID" type="xs:int" use="required"/>
<xs:attribute name="FIZ_SAL_KODAS" type="xs:short"/>
<xs:attribute name="FIZ_PASTABOS">
<xs:simpleType>
<xs:restriction base="xs:string">
<xs:maxLength value="250"/>
</xs:restriction>
</xs:simpleType>
</xs:attribute>
<xs:attribute name="FIZ_GIMIMO_DATA" type="xs:date"/>
<xs:attribute name="FAV_VARDAS" use="required">
<xs:simpleType>
<xs:restriction base="xs:string">
<xs:maxLength value="50"/>
</xs:restriction>
</xs:simpleType>
</xs:attribute>
<xs:attribute name="FAV_PAVARDE" use="required">
<xs:simpleType>
<xs:restriction base="xs:string">
<xs:maxLength value="50"/>
</xs:restriction>
</xs:simpleType>
</xs:attribute>
<xs:attribute name="ASM_ADRESAS">
<xs:simpleType>
<xs:restriction base="xs:string">
<xs:maxLength value="250"/>
</xs:restriction>
</xs:simpleType>
</xs:attribute>
<xs:attribute name="ARO_KODAS" type="xs:int"/>
<xs:attribute name="ADR_BUS"/>
</xs:complexType>
</xs:element>
</xs:schema>
"""
    schema = etree.fromstring(element_string)
    element = schema.xpath('*[local-name() = "element"]')[0]
    result = _attributes_to_properties(element)
    assert "neid_fiz_id" in result.keys()
    assert "fiz_pastabos" in result.keys()
    assert result["neid_fiz_id"] == {
        "description": "",
        "type": "integer",
        "required": True,
        "external":
            {
                "name": "@NEID_FIZ_ID"
            }
    }
    assert result["fiz_pastabos"]["type"] == "string"


def test_get_external_info():
    result = _get_external_info()
    assert result == {
        "dataset": "dataset1",
        "resource": "resource1"
    }


def test_get_external_info_kwargs():
    result = _get_external_info(name = "data")
    assert result == {
        "dataset": "dataset1",
        "resource": "resource1",
        "name": "data"
    }


# def test_get_document_root():
#     # todo finish this. creat temp file and test with it

def test_get_dataset_and_resource_info():
    given_name = "test_name"
    result = _get_dataset_and_resource_info(given_name)
    assert result == {
        'type': 'dataset',
        'name': "dataset1",
        'resources': {
            "resource1": {
                'type': 'xml',
            },
        },
        'given_name': "test_name"
    }


def test_node_is_simple_type_or_inline():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="CT_E200_FORMA">
      <xs:annotation><xs:documentation>E200 medicininės formos pavadinimas</xs:documentation></xs:annotation>
      <xs:simpleType>
        <xs:restriction base="xs:string">
          <xs:maxLength value="4"/>
        </xs:restriction>
      </xs:simpleType>
    </xs:element>
    </xs:schema>
    """
    schema = etree.fromstring(element_string)
    element = schema.xpath('*[local-name() = "element"]')[0]
    result = _node_is_simple_type_or_inline(element)
    assert result is True


def test_node_is_simple_type_or_inline_annotation_only():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="CT_E200_FORMA">
      <xs:annotation><xs:documentation>E200 medicininės formos pavadinimas</xs:documentation></xs:annotation>
    </xs:element>
    </xs:schema>
    """
    schema = etree.fromstring(element_string)
    element = schema.xpath('*[local-name() = "element"]')[0]
    result = _node_is_simple_type_or_inline(element)
    assert result is True


def test_node_is_simple_type_or_inline_inline():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="CT_E200_FORMA" />
    </xs:schema>
    """
    schema = etree.fromstring(element_string)
    element = schema.xpath('*[local-name() = "element"]')[0]
    result = _node_is_simple_type_or_inline(element)
    assert result is True


def test_node_is_simple_type_or_inline_complex():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="klientu_saraso_rezultatas">
      <xs:complexType mixed="true">
        <xs:sequence>
          <xs:element ref="asmenys" minOccurs="0" maxOccurs="1" />
        </xs:sequence>
      </xs:complexType>
    </xs:element>
    </xs:schema>
    """
    schema = etree.fromstring(element_string)
    element = schema.xpath('*[local-name() = "element"]')[0]
    result = _node_is_simple_type_or_inline(element)
    assert result is False


def test_is_array():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="klientu_saraso_rezultatas">
      <xs:complexType mixed="true">
        <xs:sequence>
          <xs:element ref="asmenys" minOccurs="0" maxOccurs="unbounded" />
        </xs:sequence>
      </xs:complexType>
    </xs:element>
    </xs:schema>
    """
    schema = etree.fromstring(element_string)
    element = schema.xpath('//*[local-name() = "element"]')[1]
    result = _is_array(element)
    assert result is True


def test_is_array_false():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="klientu_saraso_rezultatas">
      <xs:complexType mixed="true">
        <xs:sequence>
          <xs:element ref="asmenys" minOccurs="0" />
        </xs:sequence>
      </xs:complexType>
    </xs:element>
    </xs:schema>
    """
    schema = etree.fromstring(element_string)
    element = schema.xpath('//*[local-name() = "element"]')[1]
    result = _is_array(element)
    assert result is False


def test_is_required():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="klientu_saraso_rezultatas">
      <xs:complexType mixed="true">
        <xs:sequence>
          <xs:element ref="asmenys" minOccurs="0" />
        </xs:sequence>
      </xs:complexType>
    </xs:element>
    </xs:schema>
    """
    schema = etree.fromstring(element_string)
    element = schema.xpath('//*[local-name() = "element"]')[1]
    result = _is_array(element)
    assert result is False

def test_extract_custom_types():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
        <xs:simpleType name="data_laikas">
            <xs:annotation><xs:documentation>Data ir laikas</xs:documentation></xs:annotation>
            <xs:restriction base="xs:string">
                <xs:pattern value="\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"/>
            </xs:restriction>
        </xs:simpleType>
    </xs:schema>
    """
    schema = etree.fromstring(element_string)
    result = _extract_custom_types(schema)
    assert result == {
        "data_laikas":
            {
                "base": "string"
            }
    }


def test_properties_from_references():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="TYRIMAS">
<xs:annotation>
<xs:documentation/>
</xs:annotation>
<xs:complexType>
<xs:sequence>
<xs:element minOccurs="1" maxOccurs="1" ref="CT_ID"/>
<xs:element minOccurs="1" maxOccurs="1" ref="CT_E200_FC_ID"/>

</xs:sequence>
</xs:complexType>
</xs:element>
<xs:element name="CT_ID" type="xs:long">
<xs:annotation>
<xs:documentation>Lentelės įrašų identifikatorius, pirminis raktas</xs:documentation>
</xs:annotation>
</xs:element>
<xs:element name="CT_E200_FC_ID" type="xs:long">
<xs:annotation>
<xs:documentation>E200 duomenų kompozicijos unikalus identifikatorius</xs:documentation>
</xs:annotation>
</xs:element>
</xs:schema>
"""
    schema = etree.fromstring(element_string)
    element = schema.xpath('//*[local-name() = "element"]')[0]
    sequence = element.xpath('//*[local-name() = "complexType"]')[0].xpath('//*[local-name() = "sequence"]')[0]
    result = _properties_from_references(sequence, "test", [], source_path="tst", root=schema)

    # assert result == {
    #        'ct_e200_fc_id': {
    #            'description': '',
    #            'external': {
    #                 'name': 'CT_E200_FC_ID/text()',
    #             },
    #            'required': True,
    #            'type': 'integer',
    #        },
    #        'ct_id': {
    #          'description': '',
    #          'external': {
    #            'name': 'CT_ID/text()',
    #          },
    #          'required': True,
    #          'type': 'integer',
    #        },
    #      }


def test_is_element():
    element_string = """
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
<xs:element name="CT_E200_FC_ID" type="xs:long">
<xs:annotation>
<xs:documentation>E200 duomenų kompozicijos unikalus identifikatorius</xs:documentation>
</xs:annotation>
</xs:element>
</xs:schema>
    """
    schema = etree.fromstring(element_string)
    element = schema.xpath('//*[local-name() = "element"]')[0]
    assert _is_element(element) is True
