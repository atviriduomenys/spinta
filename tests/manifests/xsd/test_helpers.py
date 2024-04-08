from lxml import etree

from spinta.manifests.xsd.helpers import XSDReader


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
    xsd = XSDReader("test.xsd", "dataset1")
    result = xsd._get_description(element)

    assert result == "Klaidos atveju - klaidos pranešimas"


def test_get_description_longer():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="klaida" type="xs:string">
        <xs:annotation>
            <xs:documentation>Klaidos atveju - klaidos pranešimas</xs:documentation>
            <xs:documentation>Klaidos atveju - klaidos pranešimas</xs:documentation>
        </xs:annotation>
    </xs:element>
    </xs:schema>
    """

    schema = etree.fromstring(element_string)
    element = schema.xpath('*[local-name() = "element"]')[0]
    xsd = XSDReader("test.xsd", "dataset1")
    result = xsd._get_description(element)

    assert result == "Klaidos atveju - klaidos pranešimas Klaidos atveju - klaidos pranešimas"


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
    xsd = XSDReader("test.xsd", "dataset1")
    result = xsd._get_property_type(element)

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
    xsd = XSDReader("test.xsd", "dataset1")
    result = xsd._get_property_type(element)

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
    xsd = XSDReader("test.xsd", "dataset1")
    result = xsd._get_property_type(element)

    assert result == "string"


def test_get_property_type_custom():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="CT_E200_FORMA" type="some_type">
      <xs:annotation><xs:documentation>E200 medicininės formos pavadinimas</xs:documentation></xs:annotation>
    </xs:element>
    </xs:schema>
    """
    schema = etree.fromstring(element_string)
    element = schema.xpath('*[local-name() = "element"]')[0]
    xsd = XSDReader("test.xsd", "dataset1")
    xsd._custom_types = {"some_type": {"base": "string"}}
    result = xsd._get_property_type(element)

    assert result == "string"


def test_get_property_type_unknown():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="CT_E200_FORMA" type="some_type">
      <xs:annotation><xs:documentation>E200 medicininės formos pavadinimas</xs:documentation></xs:annotation>
    </xs:element>
    </xs:schema>
    """
    schema = etree.fromstring(element_string)
    element = schema.xpath('*[local-name() = "element"]')[0]
    xsd = XSDReader("test.xsd", "dataset1")
    result = xsd._get_property_type(element)

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
    xsd = XSDReader("test.xsd", "dataset1")
    result1, result2 = xsd._node_to_partial_property(element)

    assert result1 == "ct_e200_forma"
    assert result2 == {
        "description": "E200 medicininės formos pavadinimas",
        "enums": {'': {}},
        "external": {
            "name": "CT_E200_FORMA",
        },
        "type": "string"
    }
# todo test properties with refs


def test_node_to_partial_property_gYear():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="CT_E200_FORMA" type="gYear">
    </xs:element>
    </xs:schema>
    """

    schema = etree.fromstring(element_string)
    element = schema.xpath('*[local-name() = "element"]')[0]
    xsd = XSDReader("test.xsd", "dataset1")
    result1, result2 = xsd._node_to_partial_property(element)

    assert result1 == "ct_e200_forma"
    assert result2 == {
        "description": "",
        "enums": {},
        "external": {
            "name": "CT_E200_FORMA",
        },
        "type": "date",
        "ref": "Y"
    }


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
    xsd = XSDReader("test.xsd", "dataset1")
    result1, result2 = xsd._simple_element_to_property(element)
    assert result1 == "ct_e200_forma"
    assert result2 == {
        "description": "E200 medicininės formos pavadinimas",
        "enums": {'': {}},
        "external": {
            "name": "CT_E200_FORMA/text()",
        },
        "type": "string",
        "required": True
    }


def test_simple_element_to_property_ref():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
	<xs:element name="JAR">
		<xs:complexType>
			<xs:sequence>
				<xs:element ref="OBJEKTAI"/>
				<xs:element ref="FIZINIAI_ASMENYS" minOccurs="0" maxOccurs="unbounded"/>
			</xs:sequence>
		</xs:complexType>
	</xs:element>
	<xs:element name="OBJEKTAI">
		<xs:annotation>
			<xs:documentation>Pagrindiniai juridinio asmens duomenys.</xs:documentation>
		</xs:annotation>
		<xs:complexType>
			<xs:sequence>
				<xs:element ref="OBJEKTU_ASMENYS" minOccurs="0" maxOccurs="unbounded"/>
				<xs:element ref="TEKSTINIAI_DUOMENYS" minOccurs="0" maxOccurs="unbounded"/>
				<xs:element ref="FAKTAI" minOccurs="0" maxOccurs="unbounded"/>
			</xs:sequence>
		</xs:complexType>
	</xs:element>
	<xs:element name="FIZINIAI_ASMENYS">
		<xs:annotation>
			<xs:documentation>Pagrindiniai juridinio asmens duomenys. Atributų reikšmės: OBJ_ID - Juridinio asmens identifikatorius (dirbtinis) OBJ_KODAS - 9 skaitmenų juridinio asmens kodas OBJ_REJESTRO_KODAS - 7 skaitmenų juridinio asmens kodas OBJ_PAV -Juridinio asmens pavadinimas FORM_KODAS - Teisinės formos kodas iš klasifikatoriaus FORMOS STAT_STATUSAS - Juridinio asmens statusas iš klasifikatoriaus STATUSAI JST_DATA_NUO - Statuso galiojimo data NUO. JST_IGIJIMO_DATA - Statuso Įgijimo data. JAD_TEKSTAS - Juridinio asmens adresas JA_E_PRIST_DEZUTES_ADR - 9 skaitmenų elektroninio pristatymo dėžutės adresas. OBJ_REG_DATA - Juridinio asmens Įregistravimo registre data OBJ_STEIGIMO_DATA - Įsteigimo data OBJ_ISREG_DATA - Juridinio asmens išregistravimo data OBJ_PAGRINDINIS - Juridinio asmens požymis: 0 - pagrindinis, 1 - filialas, 2 - atstovybė OBJ_ID_PRIKLAUSO - Nuoroda į kitą juridinį asmenį: filialo ar atstovybės aukštesnę instituciją PAGR_OBJ_KODAS - Pagrindinio juridinio asmens 9 skaitmenų kodas (tik filialui, atstovybei) PAGR_OBJ_REJESTRO_KODAS - Pagrindinio juridinio asmens 7 skaitmenų kodas (tik filialui, atstovybei) PAGR_OBJ_PAV - Pagrindinio juridinio asmens pavadinimas (tik filialui, atstovybei) DBUK_KODAS - Juridinio asmens duomenų būklė iš klasifikatoriaus DUOMENU_BUKLES VER_DATA_NUO - Versijos data VER_VERSIJA - Versijos nr ISR_DATA_PATVIRTINO - Išrašo - registravimo pažymėjimo patvirtinimo data ITIP_TIPAS - Išrašo tipas iš klasifikatoriaus ISRASU_TIPAI ITIP_PAV_I - Išrašo tipo pavadinimas iš klasifikatoriaus ISRASU_TIPAI TARN_NR - Juridinių asmenų registro tvarkytojo registravimo tarnybos numeris TARN_PAV_I - Registravimo tarnybos pavadinimas</xs:documentation>
		</xs:annotation>
		<xs:complexType>
			<xs:sequence>
				<xs:element ref="OBJEKTU_ASMENYS" minOccurs="0" maxOccurs="unbounded"/>
				<xs:element ref="TEKSTINIAI_DUOMENYS" minOccurs="0" maxOccurs="unbounded"/>
				<xs:element ref="FAKTAI" minOccurs="0" maxOccurs="unbounded"/>
			</xs:sequence>
		</xs:complexType>
	</xs:element>
	</xs:schema>	
    """
    schema = etree.fromstring(element_string)
    element = schema.xpath('*[local-name() = "element"]')[1]
    xsd = XSDReader("test.xsd", "dataset1")
    result1, result2 = xsd._simple_element_to_property(element)
    assert result1 == "objektai"
    assert result2 == {
        "description": "Pagrindiniai juridinio asmens duomenys.",
        "enums": {},
        "external": {
            "name": "OBJEKTAI/text()",
        },
        "type": "string",
        "required": True
    }


def test_simple_element_to_property_array():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="CT_E200_FORMA" maxOccurs="unbounded">
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
    xsd = XSDReader("test.xsd", "dataset1")
    result1, result2 = xsd._simple_element_to_property(element)
    assert result1 == "ct_e200_forma[]"
    assert result2 == {
        "description": "E200 medicininės formos pavadinimas",
        "enums": {'': {}},
        "external": {
            "name": "CT_E200_FORMA/text()",
        },
        "type": "string",
        "required": True
    }


def test_simple_element_to_property_required():
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
    xsd = XSDReader("test.xsd", "dataset1")
    result1, result2 = xsd._simple_element_to_property(element)
    assert result1 == "ct_e200_forma"
    assert result2 == {
        "description": "E200 medicininės formos pavadinimas",
        "enums": {'': {}},
        "external": {
            "name": "CT_E200_FORMA/text()",
        },
        "type": "string",
        "required": True
    }


def test_simple_element_to_property_not_required():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="CT_E200_FORMA" minOccurs="0">
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
    xsd = XSDReader("test.xsd", "dataset1")
    result1, result2 = xsd._simple_element_to_property(element)
    assert result1 == "ct_e200_forma"
    assert result2 == {
        "description": "E200 medicininės formos pavadinimas",
        "enums": {'': {}},
        "external": {
            "name": "CT_E200_FORMA/text()",
        },
        "type": "string",
        "required": False
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
    xsd = XSDReader("test.xsd", "dataset1")
    result = xsd._attributes_to_properties(element)
    assert "neid_fiz_id" in result.keys()
    assert "fiz_pastabos" in result.keys()
    assert result["neid_fiz_id"] == {
        "description": "",
        "type": "integer",
        "required": True,
        "enums": {},
        "external":
            {
                "name": "@NEID_FIZ_ID"
            }
    }
    assert result["fiz_pastabos"]["type"] == "string"


def test_get_external_info():
    xsd = XSDReader("test.xsd", "dataset1")
    result = xsd._get_model_external_info("test")
    assert result == {
        "dataset": "test",
        "resource": "resource1",
        "name": "test"
    }

# def test_get_document_root():
#     # todo finish this. creat temp file and test with it


def test_set_dataset_and_resource_info():
    xsd = XSDReader("test.xsd", "dataset_name")
    xsd._set_dataset_and_resource_info()
    assert xsd.dataset_and_resource_info == {
        'type': 'dataset',
        'name': "test",
        'resources': {
            "resource1": {
                'type': 'xml',
            },
        },
        'given_name': "dataset_name"
    }


def test_set_dataset_and_resource_info_url():
    xsd = XSDReader("https://example.com/something/test.php?a=b", "dataset_name")
    xsd._set_dataset_and_resource_info()
    assert xsd.dataset_and_resource_info == {
        'type': 'dataset',
        'name': "test",
        'resources': {
            "resource1": {
                'type': 'xml',
            },
        },
        'given_name': "dataset_name"
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
    xsd = XSDReader("test.xsd", "dataset1")
    xsd.root = schema
    result = xsd._node_is_simple_type_or_inline(element)
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
    xsd = XSDReader("test.xsd", "dataset1")
    xsd.root = schema
    result = xsd._node_is_simple_type_or_inline(element)
    assert result is True


def test_node_is_simple_type_or_inline_inline():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="CT_E200_FORMA" />
    </xs:schema>
    """
    schema = etree.fromstring(element_string)
    element = schema.xpath('*[local-name() = "element"]')[0]
    xsd = XSDReader("test.xsd", "dataset1")
    xsd.root = schema
    result = xsd._node_is_simple_type_or_inline(element)
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
    xsd = XSDReader("test.xsd", "dataset1")
    xsd.root = schema
    result = xsd._node_is_simple_type_or_inline(element)
    assert result is False


def test_node_is_simple_type_or_inline_complex_separate():
    element_string = """
    <xs:schema version="1.0" xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:element name="personDetailInformation1" type="personDetailInformation1"/>

  <xs:complexType name="personDetailInformation1">
    <xs:sequence>
      <xs:element name="ats_adr_eil" type="xs:string" minOccurs="0"/>
      <xs:element name="ats_adr_salis" type="xs:string" minOccurs="0"/>
      <xs:element name="ats_asm_gim" type="xs:string" minOccurs="0"/>
    </xs:sequence>
  </xs:complexType>
</xs:schema>
    """
    schema = etree.fromstring(element_string)
    element = schema.xpath('*[local-name() = "element"]')[0]
    xsd = XSDReader("test.xsd", "dataset1")
    xsd.root = schema
    result = xsd._node_is_simple_type_or_inline(element)
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
    xsd = XSDReader("test.xsd", "dataset1")
    result = xsd._is_array(element)
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
    xsd = XSDReader("test.xsd", "dataset1")
    result = xsd._is_array(element)
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
    xsd = XSDReader("test.xsd", "dataset1")
    result = xsd._is_required(element)
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
    xsd = XSDReader("test.xsd", "dataset1")
    xsd.root = schema
    result = xsd._extract_custom_types(schema)
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
    xsd = XSDReader("test.xsd", "dataset1")
    xsd.root = schema
    result = xsd._properties_from_references(sequence, "test", source_path="tst")

    assert result == {
           'ct_e200_fc_id': {
               'description': 'E200 duomenų kompozicijos unikalus identifikatorius',
               'enums': {},
               'external': {
                    'name': 'CT_E200_FC_ID/text()',
                },
               'required': True,
               'type': 'integer',
           },
           'ct_id': {
             'description': 'Lentelės įrašų identifikatorius, pirminis raktas',
             'enums': {},
             'external': {
               'name': 'CT_ID/text()',
             },
             'required': True,
             'type': 'integer',
           },
         }


def test_properties_from_references_complex_not_array():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
	<xs:element name="JAR">
		<xs:complexType>
			<xs:sequence>
				<xs:element ref="OBJEKTAI"/>
				<xs:element ref="FIZINIAI_ASMENYS"/>
				<xs:element name="FIZINIAI_ASMENYS_NEID" minOccurs="0" maxOccurs="unbounded"/>
			</xs:sequence>
		</xs:complexType>
	</xs:element>
	<xs:element name="FIZINIAI_ASMENYS">
		<xs:annotation>
            <xs:documentation>Pagrindiniai juridinio asmens duomenys. </xs:documentation>		
        </xs:annotation>
		<xs:complexType>
			<xs:sequence>
				<xs:element name="OBJEKTU_ASMENYS" minOccurs="0" maxOccurs="unbounded"/>
				<xs:element name="TEKSTINIAI_DUOMENYS" minOccurs="0" maxOccurs="unbounded"/>
			</xs:sequence>
		</xs:complexType>
	</xs:element>
		<xs:element name="OBJEKTAI">
		<xs:annotation>
			<xs:documentation>Pagrindiniai juridinio asmens duomenys. </xs:documentation>
		</xs:annotation>
		<xs:complexType>
			<xs:sequence>
				<xs:element name="OBJEKTU_ASMENYS" minOccurs="0" maxOccurs="unbounded"/>
				<xs:element name="TEKSTINIAI_DUOMENYS" minOccurs="0" maxOccurs="unbounded"/>
			</xs:sequence>
		</xs:complexType>
	</xs:element>
</xs:schema>
"""
    schema = etree.fromstring(element_string)
    element = schema.xpath('//*[local-name() = "element"]')[0]
    sequence = element.xpath('//*[local-name() = "complexType"]')[0].xpath('//*[local-name() = "sequence"]')[0]
    xsd = XSDReader("test.xsd", "dataset1")
    xsd.root = schema
    result = xsd._properties_from_references(sequence, "test_model", source_path="tst")

    assert result == {
   'fiziniai_asmenys': {
     'description': '',
     'enums': {},
     'external': {'name': ''},
     'model': 'test/FiziniaiAsmenys',
     'required': True,
     'type': 'ref',
   },
   'objektai': {
     'description': '',
     'enums': {},
     'external': {'name': ''},
     'model': 'test/Objektai',
     'required': True,
     'type': 'ref',
   },
 }

    assert xsd.models == [
   {
     'description': 'Pagrindiniai juridinio asmens duomenys.',
     'external': {
       'dataset': 'test',
       'name': 'tst/OBJEKTAI',
       'resource': 'resource1',
     },
     'name': 'test/Objektai',
     'properties': {
       'objektu_asmenys[]': {
         'description': '',
         'enums': {},
         'external': {
           'name': 'OBJEKTU_ASMENYS/text()',
         },
         'required': False,
         'type': 'string',
       },
       'tekstiniai_duomenys[]': {
         'description': '',
         'enums': {},
         'external': {
           'name': 'TEKSTINIAI_DUOMENYS/text()',
         },
         'required': False,
         'type': 'string',
       },
     },
     'type': 'model',
   },
   {
     'description': 'Pagrindiniai juridinio asmens duomenys.',
     'external': {
       'dataset': 'test',
       'name': 'tst/FIZINIAI_ASMENYS',
       'resource': 'resource1',
     },
     'name': 'test/FiziniaiAsmenys',
     'properties': {
       'objektu_asmenys[]': {
         'description': '',
         'enums': {},
         'external': {
           'name': 'OBJEKTU_ASMENYS/text()',
         },
         'required': False,
         'type': 'string',
       },
       'tekstiniai_duomenys[]': {
         'description': '',
         'enums': {},
         'external': {
           'name': 'TEKSTINIAI_DUOMENYS/text()',
         },
         'required': False,
         'type': 'string',
       },
     },
     'type': 'model',
   },
 ]


def test_properties_from_references_complex_array():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
	<xs:element name="JAR">
		<xs:complexType>
			<xs:sequence>
				<xs:element ref="OBJEKTAI" maxOccurs="unbounded"/>
				<xs:element ref="FIZINIAI_ASMENYS" minOccurs="0" maxOccurs="unbounded"/>
				<xs:element name="FIZINIAI_ASMENYS_NEID" minOccurs="0" maxOccurs="unbounded"/>
			</xs:sequence>
		</xs:complexType>
	</xs:element>
	<xs:element name="FIZINIAI_ASMENYS">
		<xs:annotation>
            <xs:documentation>Pagrindiniai juridinio asmens duomenys. </xs:documentation>		
        </xs:annotation>
		<xs:complexType>
			<xs:sequence>
				<xs:element name="OBJEKTU_ASMENYS" minOccurs="0" maxOccurs="unbounded"/>
				<xs:element name="TEKSTINIAI_DUOMENYS" minOccurs="0" maxOccurs="unbounded"/>
			</xs:sequence>
		</xs:complexType>
	</xs:element>
		<xs:element name="OBJEKTAI">
		<xs:annotation>
			<xs:documentation>Pagrindiniai juridinio asmens duomenys. </xs:documentation>
		</xs:annotation>
		<xs:complexType>
			<xs:sequence>
				<xs:element name="OBJEKTU_ASMENYS" minOccurs="0" maxOccurs="unbounded"/>
				<xs:element name="TEKSTINIAI_DUOMENYS" minOccurs="0" maxOccurs="unbounded"/>
			</xs:sequence>
		</xs:complexType>
	</xs:element>
</xs:schema>
"""

    schema = etree.fromstring(element_string)
    element = schema.xpath('//*[local-name() = "element"]')[0]
    sequence = element.xpath('//*[local-name() = "complexType"]')[0].xpath('//*[local-name() = "sequence"]')[0]
    xsd = XSDReader("test.xsd", "dataset1")
    xsd.root = schema
    result = xsd._properties_from_references(sequence, "test", source_path="tst")

    assert result == {
   'fiziniai_asmenys[]': {
     'description': '',
     'enums': {},
     'external': {'name': ''},
     'model': 'test/FiziniaiAsmenys1',
     'required': False,
     'type': 'backref',
   },
   'objektai[]': {
     'description': '',
     'enums': {},
     'external': {'name': ''},
     'model': 'test/Objektai1',
     'required': True,
     'type': 'backref',
   },
 }

    assert xsd.models == [
   {
     'description': 'Pagrindiniai juridinio asmens duomenys.',
     'external': {
       'dataset': 'test',
       'name': 'tst/OBJEKTAI',
       'resource': 'resource1',
     },
     'name': 'test/Objektai1',
     'properties': {
       'objektu_asmenys[]': {
         'description': '',
         'enums': {},
         'external': {
           'name': 'OBJEKTU_ASMENYS/text()',
         },
         'required': False,
         'type': 'string',
       },
       'tekstiniai_duomenys[]': {
         'description': '',
         'enums': {},
         'external': {
           'name': 'TEKSTINIAI_DUOMENYS/text()',
         },
         'required': False,
         'type': 'string',
       },
       'test': {
         'model': 'test/test',
         'type': 'ref',
       },
     },
     'type': 'model',
   },
   {
     'description': 'Pagrindiniai juridinio asmens duomenys.',
     'external': {
       'dataset': 'test',
       'name': 'tst/FIZINIAI_ASMENYS',
       'resource': 'resource1',
     },
     'name': 'test/FiziniaiAsmenys1',
     'properties': {
       'objektu_asmenys[]': {
         'description': '',
         'enums': {},
         'external': {
           'name': 'OBJEKTU_ASMENYS/text()',
         },
         'required': False,
         'type': 'string',
       },
       'tekstiniai_duomenys[]': {
         'description': '',
         'enums': {},
         'external': {
           'name': 'TEKSTINIAI_DUOMENYS/text()',
         },
         'required': False,
         'type': 'string',
       },
       'test': {
         'model': 'test/test',
         'type': 'ref',
       },
     },
     'type': 'model',
   },
 ]



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
    xsd = XSDReader("test.xsd", "dataset1")
    xsd.root = schema
    assert xsd._is_element(element) is True


def test_is_element_false():
    element_string = """
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
<xs:attribute name="CT_E200_FC_ID" type="xs:long">
<xs:annotation>
<xs:documentation>E200 duomenų kompozicijos unikalus identifikatorius</xs:documentation>
</xs:annotation>
</xs:attribute>
</xs:schema>
    """
    schema = etree.fromstring(element_string)
    element = schema.xpath('//*[local-name() = "attribute"]')[0]
    xsd = XSDReader("test.xsd", "dataset1")
    xsd.root = schema
    assert xsd._is_element(element) is False


def test_get_text_property():
    xsd = XSDReader("test.xsd", "dataset1")
    assert xsd._get_text_property() == {
        'text': {
            'type': 'string',
            'external': {
                'name': 'text()',
            }
        }}


def test_get_separate_complex_type_node():
    element_string = """
    <xs:schema version="1.0" xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:element name="personDetailInformation1" type="personDetailInformation1"/>

  <xs:complexType name="personDetailInformation1">
    <xs:sequence>
      <xs:element name="ats_adr_eil" type="xs:string" minOccurs="0"/>
      <xs:element name="ats_adr_salis" type="xs:string" minOccurs="0"/>
      <xs:element name="ats_asm_gim" type="xs:string" minOccurs="0"/>
    </xs:sequence>
  </xs:complexType>
</xs:schema>
    """
    schema = etree.fromstring(element_string)
    element = schema.xpath('//*[local-name() = "element"]')[0]
    xsd = XSDReader("test.xsd", "dataset1")
    xsd.root = schema
    node = xsd._get_separate_complex_type_node(element)
    assert node.get("name") == "personDetailInformation1"


def test_node_has_separate_complex_type_node():
    element_string = """
    <xs:schema version="1.0" xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:element name="personDetailInformation1" type="personDetailInformation1"/>

  <xs:complexType name="personDetailInformation1">
    <xs:sequence>
      <xs:element name="ats_adr_eil" type="xs:string" minOccurs="0"/>
      <xs:element name="ats_adr_salis" type="xs:string" minOccurs="0"/>
      <xs:element name="ats_asm_gim" type="xs:string" minOccurs="0"/>
    </xs:sequence>
  </xs:complexType>
</xs:schema>
    """
    schema = etree.fromstring(element_string)
    element = schema.xpath('//*[local-name() = "element"]')[0]
    xsd = XSDReader("test.xsd", "dataset1")
    xsd.root = schema
    result = xsd._node_has_separate_complex_type(element)
    assert result is True


def test_node_has_separate_complex_type_node_false():
    element_string = """
    <xs:schema version="1.0" xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:element name="personDetailInformation1" type="personDetailInformation1"/>
    </xs:schema>
    """
    schema = etree.fromstring(element_string)
    element = schema.xpath('//*[local-name() = "element"]')[0]
    xsd = XSDReader("test.xsd", "dataset1")
    xsd.root = schema
    result = xsd._node_has_separate_complex_type(element)
    assert result is False


def test_node_is_ref():
    element_string = """
        <xs:schema version="1.0" xmlns:xs="http://www.w3.org/2001/XMLSchema">
    	<xs:element name="JAR">
		<xs:complexType>
			<xs:sequence>
				<xs:element ref="OBJEKTAI"/>
				<xs:element ref="DOKUMENTAI" minOccurs="0" maxOccurs="unbounded"/>
				<xs:element ref="FIZINIAI_ASMENYS" minOccurs="0" maxOccurs="unbounded"/>
			</xs:sequence>
		</xs:complexType>
	</xs:element>
	</xs:schema>
    """
    schema = etree.fromstring(element_string)
    element = schema.xpath('//*[local-name() = "element"]')[1]
    xsd = XSDReader("test.xsd", "dataset1")
    xsd.root = schema
    result = xsd._node_is_ref(element)
    assert result is True


def test_node_is_ref_false():
    element_string = """
        <xs:schema version="1.0" xmlns:xs="http://www.w3.org/2001/XMLSchema">
    	<xs:element name="JAR">
		<xs:complexType>
			<xs:sequence>
				<xs:element ref="OBJEKTAI"/>
				<xs:element ref="DOKUMENTAI" minOccurs="0" maxOccurs="unbounded"/>
				<xs:element ref="FIZINIAI_ASMENYS" minOccurs="0" maxOccurs="unbounded"/>
			</xs:sequence>
		</xs:complexType>
	</xs:element>
	</xs:schema>
    """
    schema = etree.fromstring(element_string)
    element = schema.xpath('//*[local-name() = "element"]')[0]
    xsd = XSDReader("test.xsd", "dataset1")
    xsd.root = schema
    result = xsd._node_is_ref(element)
    assert result is False


def test_properties_from_simple_elements():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="CT_E200_FORMA" />
    </xs:schema>
    """
    schema = etree.fromstring(element_string)
    # element = schema.xpath('*[local-name() = "element"]')[0]
    xsd = XSDReader("test.xsd", "dataset1")
    xsd.root = schema
    result = xsd._properties_from_simple_elements(schema)
    assert result == {
        'ct_e200_forma': {
            'description': '',
            'enums': {},
            'external': {
                'name': 'CT_E200_FORMA/text()',
            },
            'required': True,
            'type': 'string',
        },
    }


def test_properties_from_simple_elements_mix():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="CT_E200_FORMA" />
    <xs:element name="JAR">
		<xs:complexType>
			<xs:sequence>
				<xs:element ref="OBJEKTAI"/>
				<xs:element ref="DOKUMENTAI" minOccurs="0" maxOccurs="unbounded"/>
				<xs:element ref="FIZINIAI_ASMENYS" minOccurs="0" maxOccurs="unbounded"/>
			</xs:sequence>
		</xs:complexType>
	</xs:element>
    </xs:schema>
    """
    schema = etree.fromstring(element_string)
    # element = schema.xpath('*[local-name() = "element"]')[0]
    xsd = XSDReader("test.xsd", "dataset1")
    xsd.root = schema
    result = xsd._properties_from_simple_elements(schema)
    assert result == {
        'ct_e200_forma': {
            'description': '',
            'enums': {},
            'external': {
                'name': 'CT_E200_FORMA/text()',
            },
            'required': True,
            'type': 'string',
        },
    }


def test_properties_from_simple_elements_empty():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="JAR">
		<xs:complexType>
			<xs:sequence>
				<xs:element ref="OBJEKTAI"/>
				<xs:element ref="DOKUMENTAI" minOccurs="0" maxOccurs="unbounded"/>
				<xs:element ref="FIZINIAI_ASMENYS" minOccurs="0" maxOccurs="unbounded"/>
			</xs:sequence>
		</xs:complexType>
	</xs:element>
    </xs:schema>
    """
    schema = etree.fromstring(element_string)
    # element = schema.xpath('*[local-name() = "element"]')[0]
    xsd = XSDReader("test.xsd", "dataset1")
    xsd.root = schema
    result = xsd._properties_from_simple_elements(schema)
    assert result == {}


def test_properties_from_simple_elements_not_from_sequence():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="CT_E200_FORMA" />
    </xs:schema>
    """
    schema = etree.fromstring(element_string)
    # element = schema.xpath('*[local-name() = "element"]')[0]
    xsd = XSDReader("test.xsd", "dataset1")
    xsd.root = schema
    result = xsd._properties_from_simple_elements(schema, from_sequence=False)
    assert result == {
        'ct_e200_forma': {
            'description': '',
            'enums': {},
            'external': {
                'name': 'CT_E200_FORMA/text()',
            },
            'required': False,
            'type': 'string',
        },
    }


def test_get_enums():
    element_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
    <xs:element name="OBJEKTU_ATRIBUTAI">
		<xs:annotation>
			<xs:documentation>Juridinio asmens atributai (akcijų skaičius, stebėtojų tarybos narių skaičius ir t.t.). Atributų reikšmės: ATRI_KODAS - Atributo kodas iš klasifikatoriaus ATRIBUTAI. ATR_REIKSME - Atributo reikšmė. VIEN_KODAS - Matavimo vieneto kodas iš klasifikatoriaus MATAVIMU_VIENETAI.</xs:documentation>
		</xs:annotation>
		<xs:complexType>
			<xs:attribute name="ATRI_KODAS">
				<xs:simpleType>
					<xs:restriction base="xs:int">
						<xs:enumeration value="202"/>
						<xs:enumeration value="203"/>
					</xs:restriction>
				</xs:simpleType>
			</xs:attribute>
			<xs:attribute name="ATR_REIKSME">
				<xs:simpleType>
					<xs:restriction base="xs:decimal">
						<xs:totalDigits value="14"/>
						<xs:fractionDigits value="2"/>
					</xs:restriction>
				</xs:simpleType>
			</xs:attribute>
			<xs:attribute name="VIEN_KODAS" type="xs:int"/>
		</xs:complexType>
	</xs:element>
	</xs:schema>
    """

    schema = etree.fromstring(element_string)
    element = schema.xpath('//*[local-name() = "attribute"]')[0]
    xsd = XSDReader("test.xsd", "dataset1")
    result = xsd._get_enums(element)
    assert result == {
        "": {
            "202": {"source": "202"},
            "203": {"source": "203"}
        }}
