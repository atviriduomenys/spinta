from pathlib import Path

from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest


def test_xsd(rc: RawConfig, tmp_path: Path):
    xsd = """
    <xs:schema attributeFormDefault="unqualified" elementFormDefault="qualified" xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="ADMINISTRACINIAI">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="ADMINISTRACINIS" maxOccurs="unbounded" minOccurs="0">
          <xs:complexType>
            <xs:sequence>
              <xs:element type="xs:long" name="ADM_KODAS"/>
              <xs:element type="xs:long" name="ADM_ID"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
  <xs:element name="GYVENVIETES">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="GYVENVIETE" maxOccurs="unbounded" minOccurs="0">
          <xs:complexType>
            <xs:sequence>
              <xs:element type="xs:long" name="GYV_KODAS"/>
              <xs:element type="xs:long" name="GYV_ID"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>
    """

    table = """
 id | d | r | b | m | property        | type             | ref | source                            | prepare | level | access | uri | title | description
    | manifest                        |                  |     |                                   |         |       |        |     |       |
    |   | resource1                   | dask/xml         |     |                                   |         |       |        |     |       |
    |                                 |                  |     |                                   |         |       |        |     |       |
    |   |   |   | Administracinis     |                  |     | /ADMINISTRACINIAI/ADMINISTRACINIS |         |       |        |     |       |
    |   |   |   |   | adm_id          | integer required |     | ADM_ID/text()                     |         |       |        |     |       |
    |   |   |   |   | adm_kodas       | integer required |     | ADM_KODAS/text()                  |         |       |        |     |       |
    |                                 |                  |     |                                   |         |       |        |     |       |
    |   |   |   | Gyvenviete          |                  |     | /GYVENVIETES/GYVENVIETE           |         |       |        |     |       |
    |   |   |   |   | gyv_id          | integer required |     | GYV_ID/text()                     |         |       |        |     |       |
    |   |   |   |   | gyv_kodas       | integer required |     | GYV_KODAS/text()                  |         |       |        |     |       |
"""
    path = tmp_path / "manifest.xsd"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table


def test_xsd_ref(rc: RawConfig, tmp_path: Path):
    xsd = """

<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
<xs:element name="asmenys">
  <xs:complexType mixed="true">
    <xs:sequence>
      <xs:element ref="asmuo" minOccurs="0" maxOccurs="unbounded" />
    </xs:sequence>
    <xs:attribute name="puslapis" type="xs:long" use="required">
      <xs:annotation><xs:documentation>rezultatu puslapio numeris</xs:documentation></xs:annotation>
    </xs:attribute>
  </xs:complexType>
</xs:element>

<xs:element name="asmuo">
  <xs:complexType mixed="true">

      <xs:attribute name="id"     type="xs:string" use="required">
      </xs:attribute>
      <xs:attribute name="ak"  type="xs:string" use="required">
      </xs:attribute>

  </xs:complexType>
</xs:element>
</xs:schema>
    """

    table = """
 id | d | r | b | m | property     | type             | ref     | source         | prepare | level | access | uri | title | description
    | manifest                     |                  |         |                |         |       |        |     |       |
    |   | resource1                | dask/xml         |         |                |         |       |        |     |       |
    |                              |                  |         |                |         |       |        |     |       |
    |   |   |   | Asmenys          |                  |         | /asmenys       |         |       |        |     |       |
    |   |   |   |   | asmuo[]      | backref          | Asmuo   | asmuo          |         |       |        |     |       |
    |   |   |   |   | asmuo[].ak   | string required  |         | @ak            |         |       |        |     |       |
    |   |   |   |   | asmuo[].id   | string required  |         | @id            |         |       |        |     |       |
    |   |   |   |   | asmuo[].text | string           |         | text()         |         |       |        |     |       |
    |   |   |   |   | puslapis     | integer required |         | @puslapis      |         |       |        |     |       | rezultatu puslapio numeris
    |   |   |   |   | text         | string           |         | text()         |         |       |        |     |       |
    |                              |                  |         |                |         |       |        |     |       |
    |   |   |   | Asmuo            |                  |         |                |         |       |        |     |       |
    |   |   |   |   | ak           | string required  |         | @ak            |         |       |        |     |       |
    |   |   |   |   | asmenys      | ref              | Asmenys |                |         |       |        |     |       |
    |   |   |   |   | id           | string required  |         | @id            |         |       |        |     |       |
    |   |   |   |   | text         | string           |         | text()         |         |       |        |     |       |

"""

    path = tmp_path / "manifest.xsd"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    print(manifest)
    assert manifest == table


def test_xsd_resource_model(rc: RawConfig, tmp_path: Path):
    xsd = """

<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">

<xs:element name="klaida" type="xs:string">
  <xs:annotation><xs:documentation>Klaidos atveju - klaidos pranešimas</xs:documentation></xs:annotation>
</xs:element>

<xs:element name="asmenys">
  <xs:complexType mixed="true">

    <xs:attribute name="puslapis" type="xs:long" use="required">
      <xs:annotation><xs:documentation>rezultatu puslapio numeris</xs:documentation></xs:annotation>
    </xs:attribute>

  </xs:complexType>
</xs:element>

</xs:schema>
    """

    table = """
 id | d | r | b | m | property | type             | ref | source        | prepare | level | access | uri                                           | title | description
    | manifest                 |                  |     |               |         |       |        |                                               |       |
    |   | resource1            | dask/xml         |     |               |         |       |        |                                               |       |
    |                          |                  |     |               |         |       |        |                                               |       |
    |   |   |   | Asmenys      |                  |     | /asmenys      |         |       |        |                                               |       |
    |   |   |   |   | puslapis | integer required |     | @puslapis     |         |       |        |                                               |       | rezultatu puslapio numeris
    |   |   |   |   | text     | string           |     | text()        |         |       |        |                                               |       |
    |                          |                  |     |               |         |       |        |                                               |       |
    |   |   |   | Resource     |                  |     | /             |         |       |        | http://www.w3.org/2000/01/rdf-schema#Resource |       | Įvairūs duomenys
    |   |   |   |   | klaida   | string           |     | klaida/text() |         |       |        |                                               |       | Klaidos atveju - klaidos pranešimas

"""

    path = tmp_path / "manifest.xsd"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    print(manifest)
    assert manifest == table


def test_xsd_separate_type(rc: RawConfig, tmp_path: Path):
    xsd = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">

<xs:element name="SKIEPAS_EU">
  <xs:annotation><xs:documentation></xs:documentation></xs:annotation>
    <xs:complexType>
      <xs:sequence>

      <xs:element minOccurs="0" maxOccurs="1" name="PACIENTO_AK">
        <xs:annotation><xs:documentation>Paciento asmens kodas (LTU)</xs:documentation></xs:annotation>
        <xs:simpleType>
          <xs:restriction base="xs:string"><xs:maxLength value="1024"/></xs:restriction>
        </xs:simpleType>
      </xs:element>

      <xs:element minOccurs="1" maxOccurs="1" name="SKIEPIJIMO_DATA" type="t_data">
        <xs:annotation><xs:documentation>Skiepijimo data</xs:documentation></xs:annotation>
      </xs:element>

    </xs:sequence>
  </xs:complexType>
</xs:element>


<xs:simpleType name="t_data">
  <xs:annotation><xs:documentation>Data</xs:documentation></xs:annotation>
  <xs:restriction base="xs:string">
    <xs:pattern value="\d{4}-\d{2}-\d{2}"/>
  </xs:restriction>
</xs:simpleType>


</xs:schema>
    """

    table = """
id | d | r | b | m | property            | type            | ref              | source                         | prepare | level | access | uri | title | description
   | manifest                            |                 |                  |                                |         |       |        |     |       |
   |   | resource1                       | dask/xml        |                  |                                |         |       |        |     |       |
   |                                     |                 |                  |                                |         |       |        |     |       |
   |   |   |   | SkiepasEu               |                 |                  | /SKIEPAS_EU                    |         |       |        |     |       |
   |   |   |   |   | paciento_ak         | string          |                  | PACIENTO_AK/text()             |         |       |        |     |       | Paciento asmens kodas (LTU)
   |   |   |   |   | skiepijimo_data     | string required |                  | SKIEPIJIMO_DATA/text()         |         |       |        |     |       | Skiepijimo data
"""

    path = tmp_path / "manifest.xsd"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    print(manifest)
    assert manifest == table


def test_xsd_choice(rc: RawConfig, tmp_path: Path):
    xsd = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
	<xs:element name="parcel">
		<xs:annotation>
			<xs:documentation>Žemės sklypo pasikeitimo informacija</xs:documentation>
		</xs:annotation>
		<xs:complexType mixed="true">
			<xs:choice>
				<xs:element name="parcel_unique_number" minOccurs="1" maxOccurs="1">
					<xs:annotation>
						<xs:documentation>Žemės sklypo unikalus numeris</xs:documentation>
					</xs:annotation>
					<xs:simpleType>
						<xs:restriction base="xs:positiveInteger"> <!-- https://www.oreilly.com/library/view/xml-schema/0596002521/re90.html -->
							<xs:totalDigits value="12"/>
						</xs:restriction>
					</xs:simpleType>
				</xs:element>
				<xs:element name="sign_of_change" minOccurs="1" maxOccurs="1">
					<xs:annotation>
						<xs:documentation>Žemės sklypo pasikeitimo požymis</xs:documentation>
					</xs:annotation>
					<xs:simpleType>
						<xs:restriction base="xs:int">
							<xs:enumeration value="1"/> <!-- nauji sklypai 	  -->
							<xs:enumeration value="2"/> <!-- redaguoti sklypai -->
						</xs:restriction>
					</xs:simpleType>
				</xs:element>
			</xs:choice>
		</xs:complexType>
	</xs:element>
</xs:schema>
    """

    table = """
 id | d | r | b | m | property             | type             | ref | source                      | prepare | level | access | uri | title | description
    | manifest                             |                  |     |                             |         |       |        |     |       |
    |   | resource1                        | dask/xml         |     |                             |         |       |        |     |       |
    |                                      |                  |     |                             |         |       |        |     |       |
    |   |   |   | Parcel1                  |                  |     | /parcel                     |         |       |        |     |       | Žemės sklypo pasikeitimo informacija
    |   |   |   |   | parcel_unique_number | integer required |     | parcel_unique_number/text() |         |       |        |     |       | Žemės sklypo unikalus numeris
    |   |   |   |   | text                 | string           |     | text()                      |         |       |        |     |       |
    |                                      |                  |     |                             |         |       |        |     |       |
    |   |   |   | Parcel2                  |                  |     | /parcel                     |         |       |        |     |       | Žemės sklypo pasikeitimo informacija
    |   |   |   |   | sign_of_change       | integer required |     | sign_of_change/text()       |         |       |        |     |       | Žemės sklypo pasikeitimo požymis
    |                                      | enum             |     | 1                           | '1'     |       |        |     |       |
    |                                      |                  |     | 2                           | '2'     |       |        |     |       |
    |   |   |   |   | text                 | string           |     | text()                      |         |       |        |     |       |

"""

    path = tmp_path / "manifest.xsd"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    print(manifest)
    assert manifest == table


def test_xsd_choice_max_occurs_unbounded(rc: RawConfig, tmp_path: Path):
    xsd = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
	<xs:element name="parcel">
		<xs:annotation>
			<xs:documentation>Žemės sklypo pasikeitimo informacija</xs:documentation>
		</xs:annotation>
		<xs:complexType mixed="true">
			<xs:choice maxOccurs="unbounded">
				<xs:element name="parcel_unique_number" minOccurs="1" maxOccurs="1">
					<xs:annotation>
						<xs:documentation>Žemės sklypo unikalus numeris</xs:documentation>
					</xs:annotation>
					<xs:simpleType>
						<xs:restriction base="xs:positiveInteger"> <!-- https://www.oreilly.com/library/view/xml-schema/0596002521/re90.html -->
							<xs:totalDigits value="12"/>
						</xs:restriction>
					</xs:simpleType>
				</xs:element>
				<xs:element name="sign_of_change" minOccurs="1" maxOccurs="1">
					<xs:annotation>
						<xs:documentation>Žemės sklypo pasikeitimo požymis</xs:documentation>
					</xs:annotation>
					<xs:simpleType>
						<xs:restriction base="xs:int">
							<xs:enumeration value="1"/> <!-- nauji sklypai 	  -->
							<xs:enumeration value="2"/> <!-- redaguoti sklypai -->
						</xs:restriction>
					</xs:simpleType>
				</xs:element>
			</xs:choice>
		</xs:complexType>
	</xs:element>
</xs:schema>
    """

    table = """
 id | d | r | b | m | property               | type     | ref | source                      | prepare | level | access | uri | title | description
    | manifest                               |          |     |                             |         |       |        |     |       |
    |   | resource1                          | dask/xml |     |                             |         |       |        |     |       |
    |                                        |          |     |                             |         |       |        |     |       |
    |   |   |   | Parcel                     |          |     | /parcel                     |         |       |        |     |       | Žemės sklypo pasikeitimo informacija
    |   |   |   |   | parcel_unique_number[] | integer  |     | parcel_unique_number/text() |         |       |        |     |       | Žemės sklypo unikalus numeris
    |   |   |   |   | sign_of_change[]       | integer  |     | sign_of_change/text()       |         |       |        |     |       | Žemės sklypo pasikeitimo požymis
    |                                        | enum     |     | 1                           | '1'     |       |        |     |       |
    |                                        |          |     | 2                           | '2'     |       |        |     |       |
    |   |   |   |   | text                   | string   |     | text()                      |         |       |        |     |       |

"""

    path = tmp_path / "manifest.xsd"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    print(manifest)
    assert manifest == table


def test_xsd_attributes(rc: RawConfig, tmp_path: Path):
    xsd = """

<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">

<xs:element name="SALYGA">
  <xs:complexType>
    <xs:attribute name="kodas"    type="xs:string"  use="optional" />
    <xs:attribute name="pavadinimas"    type="xs:string"  use="optional" />
  </xs:complexType>
</xs:element>

</xs:schema>
    """

    table = """
 id | d | r | b | m | property             | type            | ref     | source             | prepare | level | access | uri | title | description
    | manifest                             |                 |         |                    |         |       |        |     |       |
    |   | resource1                        | dask/xml        |         |                    |         |       |        |     |       |
    |                                      |                 |         |                    |         |       |        |     |       |
    |   |   |   | Salyga                   |                 |         | /SALYGA            |         |       |        |     |       |
    |   |   |   |   | kodas                | string          |         | @kodas             |         |       |        |     |       |
    |   |   |   |   | pavadinimas          | string          |         | @pavadinimas       |         |       |        |     |       |
"""

    path = tmp_path / "manifest.xsd"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    print(manifest)
    assert manifest == table


def test_xsd_model_one_property(rc: RawConfig, tmp_path: Path):
    xsd = """
    <xs:schema xmlns="http://eTaarPlat.ServiceContracts/2007/08/Messages" xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified" targetNamespace="http://eTaarPlat.ServiceContracts/2007/08/Messages">

        <xs:element name="Response">
            <xs:complexType>
                <xs:sequence>
                    <xs:element minOccurs="0" maxOccurs="1" name="searchParameters" type="SearchParametersTRA"/>
                    <xs:element minOccurs="0" maxOccurs="1" name="klaida">
                        <xs:complexType>
                            <xs:sequence>
                                <xs:element minOccurs="0" name="Aprasymas" type="xs:string" />
                            </xs:sequence>
                        </xs:complexType>
                    </xs:element>  
                </xs:sequence>
            </xs:complexType>
        </xs:element>
        
    </xs:schema>
    """

    table = """
 id | d | r | b | m | property          | type     | ref | source                  | prepare | level | access | uri | title | description
    | manifest                          |          |     |                         |         |       |        |     |       |
    |   | resource1                     | dask/xml |     |                         |         |       |        |     |       |
    |                                   |          |     |                         |         |       |        |     |       |
    |   |   |   | Response              |          |     | /Response               |         |       |        |     |       |
    |   |   |   |   | aprasymas         | string   |     | klaida/Aprasymas/text() |         |       |        |     |       |
    |   |   |   |   | search_parameters | string   |     | searchParameters/text() |         |       |        |     |       |

"""
    path = tmp_path / "manifest.xsd"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table


def test_xsd_separate_simple_type(rc: RawConfig, tmp_path: Path):
    xsd = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">

<xs:element name="TYRIMAS">
  <xs:annotation><xs:documentation></xs:documentation></xs:annotation>
    <xs:complexType>
      <xs:sequence>
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_E200ATS_DUOM_SUKURTI" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_PACIENTO_SPI" />
    </xs:sequence>
  </xs:complexType>
</xs:element>

<xs:element name="CT_E200ATS_DUOM_SUKURTI" type="data_laikas">
  <xs:annotation><xs:documentation>E200-ats duomenų sukūrimo data ir laikas</xs:documentation></xs:annotation>
</xs:element>

<xs:element name="CT_PACIENTO_SPI">
  <xs:annotation><xs:documentation>Paciento prisirašymo įstaigos pavadinimas</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="300"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:simpleType name="data_laikas">
  <xs:annotation><xs:documentation>Data ir laikas</xs:documentation></xs:annotation>
  <xs:restriction base="xs:string">
    <xs:pattern value="\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"/>
  </xs:restriction>
</xs:simpleType>

</xs:schema>
"""

    table = """
 id | d | r | b | m | property                | type     | ref | source                         | prepare | level | access | uri | title | description
    | manifest                                |          |     |                                |         |       |        |     |       |
    |   | resource1                           | dask/xml |     |                                |         |       |        |     |       |
    |                                         |          |     |                                |         |       |        |     |       |
    |   |   |   | Tyrimas                     |          |     | /TYRIMAS                       |         |       |        |     |       |
    |   |   |   |   | ct_e200ats_duom_sukurti | string   |     | CT_E200ATS_DUOM_SUKURTI/text() |         |       |        |     |       | E200-ats duomenų sukūrimo data ir laikas
    |   |   |   |   | ct_paciento_spi         | string   |     | CT_PACIENTO_SPI/text()         |         |       |        |     |       | Paciento prisirašymo įstaigos pavadinimas
"""
    path = tmp_path / "manifest.xsd"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table


def test_xsd_sequence_choice_sequence(rc: RawConfig, tmp_path: Path):
    # choice in a sequence with a sequence inside
    xsd = """
<s:schema xmlns:s="http://www.w3.org/2001/XMLSchema" xmlns:tns="http://rc/ireg/1.0/" targetNamespace="http://rc/ireg/1.0/" elementFormDefault="qualified" attributeFormDefault="unqualified">

    <s:element name="person">
        <s:complexType>
            <s:sequence>
                <s:element minOccurs="0" maxOccurs="1" name="iltu_code" nillable="true" type="s:string" />
                <s:choice>
                    <s:sequence>
                        <s:element minOccurs="0" maxOccurs="1" name="firstName" />
                        <s:element minOccurs="0" maxOccurs="1" name="lastName" />
                    </s:sequence>
                    <s:sequence>
                        <s:element minOccurs="0" maxOccurs="1" name="businessName" />
                    </s:sequence>
                </s:choice>
            </s:sequence>
        </s:complexType>
    </s:element>
</s:schema>
"""

    table = """
 id | d | r | b | m | property      | type     | ref | source              | prepare | level | access | uri | title | description
    | manifest                      |          |     |                     |         |       |        |     |       |
    |   | resource1                 | dask/xml |     |                     |         |       |        |     |       |
    |                               |          |     |                     |         |       |        |     |       |
    |   |   |   | Person1           |          |     | /person             |         |       |        |     |       |
    |   |   |   |   | first_name    | string   |     | firstName/text()    |         |       |        |     |       |
    |   |   |   |   | iltu_code     | string   |     | iltu_code/text()    |         |       |        |     |       |
    |   |   |   |   | last_name     | string   |     | lastName/text()     |         |       |        |     |       |
    |                               |          |     |                     |         |       |        |     |       |
    |   |   |   | Person2           |          |     | /person             |         |       |        |     |       |
    |   |   |   |   | business_name | string   |     | businessName/text() |         |       |        |     |       |
    |   |   |   |   | iltu_code     | string   |     | iltu_code/text()    |         |       |        |     |       |

"""
    path = tmp_path / "manifest.xsd"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table


def test_xsd_complex_ontent(rc: RawConfig, tmp_path: Path):
    xsd = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
        <xs:element name="BE_FULL" nillable="true" type="BE_FULL"/>
        <xs:complexType name="BE_FULL">
            <xs:complexContent mixed="false">
                <xs:extension base="BusinessEntityOfBE_FULL">
                    <xs:sequence>
                        <xs:element minOccurs="0" maxOccurs="1" name="title1" type="xs:string"/>
                        <xs:element minOccurs="0" maxOccurs="1" name="printeddate" type="xs:string"/>
                    </xs:sequence>
                </xs:extension>
            </xs:complexContent>
        </xs:complexType>
        <xs:complexType name="BusinessEntityOfBE_FULL" abstract="true"/>
    </xs:schema>
"""

    table = """
 id | d | r | b | m | property    | type     | ref | source             | prepare | level | access | uri | title | description
    | manifest                    |          |     |                    |         |       |        |     |       |
    |   | resource1               | dask/xml |     |                    |         |       |        |     |       |
    |                             |          |     |                    |         |       |        |     |       |
    |   |   |   | BeFull          |          |     | /BE_FULL           |         |       |        |     |       |
    |   |   |   |   | printeddate | string   |     | printeddate/text() |         |       |        |     |       |
    |   |   |   |   | title1      | string   |     | title1/text()      |         |       |        |     |       |

"""
    path = tmp_path / "manifest.xsd"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table


def test_xsd_recursion(rc: RawConfig, tmp_path: Path):
    # recursion in XSD
    xsd = """
<s:schema xmlns:s="http://www.w3.org/2001/XMLSchema" xmlns:tns="http://rc/ireg/1.0/" targetNamespace="http://rc/ireg/1.0/" elementFormDefault="qualified" attributeFormDefault="unqualified">
        <s:element name="data">
            <s:complexType>
                <s:sequence>
                    <s:element minOccurs="0" maxOccurs="1" name="responseData" type="tns:responseData"/>
                    <s:element minOccurs="0" maxOccurs="1" name="responseMessage" type="s:string"/>
                </s:sequence>
            </s:complexType>
        </s:element>
    
        <s:complexType name="responseData">
            <s:sequence>
                <s:element minOccurs="0" maxOccurs="unbounded" name="actions" type="tns:actions"/>
            </s:sequence>
        </s:complexType>
    
        <s:complexType name="children">
            <s:sequence>
                <s:element minOccurs="0" maxOccurs="unbounded" name="action" type="tns:action"/>
            </s:sequence>
        </s:complexType>
    
        <s:complexType name="actions">
            <s:sequence>
                <s:element minOccurs="0" maxOccurs="unbounded" name="action" type="tns:action"/>
            </s:sequence>
        </s:complexType>
    
        <s:complexType name="action">
            <s:sequence>
                <s:element minOccurs="1" maxOccurs="1" name="code" type="s:string">
                    <s:annotation>
                        <s:documentation>Paslaugos kodas (RC kodas)</s:documentation>
                    </s:annotation>
                </s:element>
    
                <s:element minOccurs="1" maxOccurs="unbounded" name="children" type="tns:children" />
            </s:sequence>
        </s:complexType>
</s:schema>
"""

    table = """
 id | d | r | b | m | property               | type            | ref     | source                     | prepare | level | access | uri | title | description
    | manifest                               |                 |         |                            |         |       |        |     |       |
    |   | resource1                          | dask/xml        |         |                            |         |       |        |     |       |
    |                                        |                 |         |                            |         |       |        |     |       |
    |   |   |   | Actions                    |                 |         |                            |         |       |        |     |       |
    |   |   |   |   | code[]                 | string required |         | action/code/text()         |         |       |        |     |       | Paslaugos kodas (RC kodas)
    |   |   |   |   | data                   | ref             | Data    |                            |         |       |        |     |       |
    |                                        |                 |         |                            |         |       |        |     |       |
    |   |   |   | Data                       |                 |         | /data                      |         |       |        |     |       |
    |   |   |   |   | response_data[]        | backref         | Actions | responseData/actions       |         |       |        |     |       |
    |   |   |   |   | response_data[].code[] | string required |         | action/code/text()         |         |       |        |     |       | Paslaugos kodas (RC kodas)
    |   |   |   |   | response_message       | string          |         | responseMessage/text()     |         |       |        |     |       |

"""
    path = tmp_path / "manifest.xsd"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table


def test_xsd_enumeration(rc: RawConfig, tmp_path: Path):
    # recursion in XSD
    xsd = """
    <s:schema xmlns:s="http://www.w3.org/2001/XMLSchema" xmlns:tns="http://rc/ireg/1.0/" targetNamespace="http://rc/ireg/1.0/" elementFormDefault="qualified" attributeFormDefault="unqualified">
        <s:element name="data">
            <s:complexType>
                <s:sequence>
                    <s:element minOccurs="0" maxOccurs="1" name="responseData" type="tns:action"/>
                    <s:element minOccurs="0" maxOccurs="1" name="responseMessage" type="s:string"/>
                </s:sequence>
            </s:complexType>
        </s:element>
    
        <s:complexType name="action">
            <s:sequence>
                <s:element name="who_may_consitute" minOccurs="1" maxOccurs="1">
                    <s:simpleType>
                        <s:restriction base="s:string">
                            <s:enumeration value="fiz"/>
                            <s:enumeration value="fiz-notarial"/>
                        </s:restriction>
                    </s:simpleType>
                </s:element>
                
            </s:sequence>
        </s:complexType>
    </s:schema>
"""

    table = """
 id | d | r | b | m | property          | type            | ref | source                                | prepare | level | access | uri | title | description
    | manifest                          |                 |     |                                       |         |       |        |     |       |
    |   | resource1                     | dask/xml        |     |                                       |         |       |        |     |       |
    |                                   |                 |     |                                       |         |       |        |     |       |
    |   |   |   | Data                  |                 |     | /data                                 |         |       |        |     |       |
    |   |   |   |   | response_message  | string          |     | responseMessage/text()                |         |       |        |     |       |
    |   |   |   |   | who_may_consitute | string required |     | responseData/who_may_consitute/text() |         |       |        |     |       |

"""
    path = tmp_path / "manifest.xsd"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table


def test_duplicate_removal(rc: RawConfig, tmp_path: Path):
    xsd = """
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

    table = """
 id | d | r | b | m | property                         | type              | ref              | source                                        | prepare | level | access | uri | title | description
    | manifest                                         |                   |                  |                                               |         |       |        |     |       |
    |   | resource1                                    | dask/xml          |                  |                                               |         |       |        |     |       |
    |                                                  |                   |                  |                                               |         |       |        |     |       |
    |   |   |   | Extract                              |                   |                  |                                               |         |       |        |     |       |
    |   |   |   |   | extract_preparation_time         | datetime required |                  | extractPreparationTime/text()                 |         |       |        |     |       |
    |   |   |   |   | last_update_time                 | datetime required |                  | lastUpdateTime/text()                         |         |       |        |     |       |
    |                                                  |                   |                  |                                               |         |       |        |     |       |
    |   |   |   | GetDocumentsByWagonResponse1         |                   |                  | /getDocumentsByWagonResponse                  |         |       |        |     |       |
    |   |   |   |   | aprasymas                        | string            |                  | klaida/Aprasymas/text()                       |         |       |        |     |       |
    |   |   |   |   | search_parameters                | ref               | SearchParameters | searchParameters                              |         |       |        |     |       |
    |   |   |   |   | search_parameters.code           | string            |                  | searchParameters/code/text()                  |         |       |        |     |       |
    |   |   |   |   | search_parameters.search_type    | string            |                  | searchParameters/searchType/text()            |         |       |        |     |       |
    |                                                  |                   |                  |                                               |         |       |        |     |       |
    |   |   |   | GetDocumentsByWagonResponse2         |                   |                  | /getDocumentsByWagonResponse                  |         |       |        |     |       |
    |   |   |   |   | extract                          | ref               | Extract          | extract                                       |         |       |        |     |       |
    |   |   |   |   | extract.extract_preparation_time | datetime required |                  | extract/extractPreparationTime/text()         |         |       |        |     |       |
    |   |   |   |   | extract.last_update_time         | datetime required |                  | extract/lastUpdateTime/text()                 |         |       |        |     |       |
    |   |   |   |   | search_parameters                | ref               | SearchParameters | searchParameters                              |         |       |        |     |       |
    |   |   |   |   | search_parameters.code           | string            |                  | searchParameters/code/text()                  |         |       |        |     |       |
    |   |   |   |   | search_parameters.search_type    | string            |                  | searchParameters/searchType/text()            |         |       |        |     |       |
    |                                                  |                   |                  |                                               |         |       |        |     |       |
    |   |   |   | SearchParameters                     |                   |                  |                                               |         |       |        |     |       |
    |   |   |   |   | code                             | string            |                  | code/text()                                   |         |       |        |     |       |
    |   |   |   |   | search_type                      | string            |                  | searchType/text()                             |         |       |        |     |       |

  """

    path = tmp_path / "manifest.xsd"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table


def test_duplicate_removal_backref(rc: RawConfig, tmp_path: Path):
    xsd = """
    <xs:schema xmlns="http://eTaarPlat.ServiceContracts/2007/08/Messages" elementFormDefault="qualified" targetNamespace="http://eTaarPlat.ServiceContracts/2007/08/Messages" xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <xs:element name="getDocumentsByWagonResponse" nillable="true" type="getDocumentsByWagonResponse" />
        <xs:complexType name="getDocumentsByWagonResponse">
            <xs:sequence>
                <xs:element minOccurs="0" maxOccurs="unbounded" name="searchParameters" type="getDocumentsByWagonSearchParams" />
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

    table = """
 id | d | r | b | m | property                         | type              | ref              | source                                        | prepare | level | access | uri | title | description
    | manifest                                         |                   |                  |                                               |         |       |        |     |       |
    |   | resource1                                    | dask/xml          |                  |                                               |         |       |        |     |       |
    |                                                  |                   |                  |                                               |         |       |        |     |       |
    |   |   |   | Extract                              |                   |                  |                                               |         |       |        |     |       |
    |   |   |   |   | extract_preparation_time         | datetime required |                  | extractPreparationTime/text()                 |         |       |        |     |       |
    |   |   |   |   | last_update_time                 | datetime required |                  | lastUpdateTime/text()                         |         |       |        |     |       |
    |                                                  |                   |                  |                                               |         |       |        |     |       |
    |   |   |   | GetDocumentsByWagonResponse1         |                   |                  | /getDocumentsByWagonResponse                  |         |       |        |     |       |
    |   |   |   |   | aprasymas                        | string            |                  | klaida/Aprasymas/text()                       |         |       |        |     |       |
    |   |   |   |   | search_parameters[]              | backref           | SearchParameters | searchParameters                              |         |       |        |     |       |
    |   |   |   |   | search_parameters[].code         | string            |                  | code/text()                                   |         |       |        |     |       |
    |   |   |   |   | search_parameters[].search_type  | string            |                  | searchType/text()                             |         |       |        |     |       |
    |                                                  |                   |                  |                                               |         |       |        |     |       |
    |   |   |   | GetDocumentsByWagonResponse2         |                   |                  | /getDocumentsByWagonResponse                  |         |       |        |     |       |
    |   |   |   |   | extract                          | ref               | Extract          | extract                                       |         |       |        |     |       |
    |   |   |   |   | extract.extract_preparation_time | datetime required |                  | extract/extractPreparationTime/text()         |         |       |        |     |       |
    |   |   |   |   | extract.last_update_time         | datetime required |                  | extract/lastUpdateTime/text()                 |         |       |        |     |       |
    |   |   |   |   | search_parameters[]              | backref           | SearchParameters | searchParameters                              |         |       |        |     |       |
    |   |   |   |   | search_parameters[].code         | string            |                  | code/text()                                   |         |       |        |     |       |
    |   |   |   |   | search_parameters[].search_type  | string            |                  | searchType/text()                             |         |       |        |     |       |
    |                                                  |                   |                  |                                               |         |       |        |     |       |
    |   |   |   | SearchParameters                     |                   |                  |                                               |         |       |        |     |       |
    |   |   |   |   | code                             | string            |                  | code/text()                                   |         |       |        |     |       |
    |   |   |   |   | get_documents_by_wagon_response1 | ref               | GetDocumentsByWagonResponse1 |                                               |         |       |        |     |       |
    |   |   |   |   | get_documents_by_wagon_response2 | ref               | GetDocumentsByWagonResponse2 |                                               |         |       |        |     |       |
    |   |   |   |   | search_type                      | string            |                  | searchType/text()                             |         |       |        |     |       |

  """

    path = tmp_path / "manifest.xsd"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table


def test_duplicate_removal_different_models(rc: RawConfig, tmp_path: Path):
    """
    in this situation, "Extract" model has to be only once
    """
    xsd = """
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
        <xs:element name="getDocumentsByAirCraftResponse" nillable="true" type="getDocumentsByAirCraftResponse" />
        <xs:complexType name="getDocumentsByAirCraftResponse">
            <xs:sequence>
                <xs:element minOccurs="0" maxOccurs="1" name="searchParameters" type="getDocumentsByAirCraftSearchParams" />
                <xs:choice minOccurs="1" maxOccurs="1">
                    <xs:element minOccurs="0" maxOccurs="1" name="klaida" type="Klaida" />
                    <xs:element minOccurs="0" maxOccurs="1" name="extract" type="Extract" />
                </xs:choice>
            </xs:sequence>
        </xs:complexType>
        <xs:complexType name="getDocumentsByAirCraftSearchParams">
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

    table = """
 id | d | r | b | m | property                         | type              | ref              | source                                                            | prepare | level | access | uri | title | description
    | manifest                                         |                   |                  |                                                                   |         |       |        |     |       |
    |   | resource1                                    | dask/xml          |                  |                                                                   |         |       |        |     |       |
    |                                                  |                   |                  |                                                                   |         |       |        |     |       |
    |   |   |   | Extract                              |                   |                  |                                                                   |         |       |        |     |       |
    |   |   |   |   | extract_preparation_time         | datetime required |                  | extractPreparationTime/text()                                     |         |       |        |     |       |
    |   |   |   |   | last_update_time                 | datetime required |                  | lastUpdateTime/text()                                             |         |       |        |     |       |
    |                                                  |                   |                  |                                                                   |         |       |        |     |       |
    |   |   |   | GetDocumentsByAirCraftResponse1      |                   |                  | /getDocumentsByAirCraftResponse                                   |         |       |        |     |       |
    |   |   |   |   | aprasymas                        | string            |                  | klaida/Aprasymas/text()                                           |         |       |        |     |       |
    |   |   |   |   | search_parameters                | ref               | SearchParameters | searchParameters                                                  |         |       |        |     |       |
    |   |   |   |   | search_parameters.code           | string            |                  | getDocumentsByWagonResponse/searchParameters/code/text()          |         |       |        |     |       |
    |   |   |   |   | search_parameters.search_type    | string            |                  | getDocumentsByWagonResponse/searchParameters/searchType/text()    |         |       |        |     |       |
    |                                                  |                   |                  |                                                                   |         |       |        |     |       |
    |   |   |   | GetDocumentsByAirCraftResponse2      |                   |                  | /getDocumentsByAirCraftResponse                                   |         |       |        |     |       |
    |   |   |   |   | extract                          | ref               | Extract          | extract                                                           |         |       |        |     |       |
    |   |   |   |   | extract.extract_preparation_time | datetime required |                  | getDocumentsByWagonResponse/extract/extractPreparationTime/text() |         |       |        |     |       |
    |   |   |   |   | extract.last_update_time         | datetime required |                  | getDocumentsByWagonResponse/extract/lastUpdateTime/text()         |         |       |        |     |       |
    |   |   |   |   | search_parameters                | ref               | SearchParameters | searchParameters                                                  |         |       |        |     |       |
    |   |   |   |   | search_parameters.code           | string            |                  | getDocumentsByWagonResponse/searchParameters/code/text()          |         |       |        |     |       |
    |   |   |   |   | search_parameters.search_type    | string            |                  | getDocumentsByWagonResponse/searchParameters/searchType/text()    |         |       |        |     |       |
    |                                                  |                   |                  |                                                                   |         |       |        |     |       |
    |   |   |   | GetDocumentsByWagonResponse1         |                   |                  | /getDocumentsByWagonResponse                                      |         |       |        |     |       |
    |   |   |   |   | aprasymas                        | string            |                  | klaida/Aprasymas/text()                                           |         |       |        |     |       |
    |   |   |   |   | search_parameters                | ref               | SearchParameters | searchParameters                                                  |         |       |        |     |       |
    |   |   |   |   | search_parameters.code           | string            |                  | searchParameters/code/text()                                      |         |       |        |     |       |
    |   |   |   |   | search_parameters.search_type    | string            |                  | searchParameters/searchType/text()                                |         |       |        |     |       |
    |                                                  |                   |                  |                                                                   |         |       |        |     |       |
    |   |   |   | GetDocumentsByWagonResponse2         |                   |                  | /getDocumentsByWagonResponse                                      |         |       |        |     |       |
    |   |   |   |   | extract                          | ref               | Extract          | extract                                                           |         |       |        |     |       |
    |   |   |   |   | extract.extract_preparation_time | datetime required |                  | extract/extractPreparationTime/text()                             |         |       |        |     |       |
    |   |   |   |   | extract.last_update_time         | datetime required |                  | extract/lastUpdateTime/text()                                     |         |       |        |     |       |
    |   |   |   |   | search_parameters                | ref               | SearchParameters | searchParameters                                                  |         |       |        |     |       |
    |   |   |   |   | search_parameters.code           | string            |                  | searchParameters/code/text()                                      |         |       |        |     |       |
    |   |   |   |   | search_parameters.search_type    | string            |                  | searchParameters/searchType/text()                                |         |       |        |     |       |
    |                                                  |                   |                  |                                                                   |         |       |        |     |       |
    |   |   |   | SearchParameters                     |                   |                  |                                                                   |         |       |        |     |       |
    |   |   |   |   | code                             | string            |                  | code/text()                                                       |         |       |        |     |       |
    |   |   |   |   | search_type                      | string            |                  | searchType/text()                                                 |         |       |        |     |       |

  """

    path = tmp_path / "manifest.xsd"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table


def test_duplicate_removal_two_level(rc: RawConfig, tmp_path: Path):
    """
    if a ref refers to another model, and this model refers to yet another, and they both produce duplicates,
    all those duplicates should be removed.
    """
    xsd = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">

    <xs:element name="Maker">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="name" type="xs:string"/>
                <xs:element name="code" type="xs:string"/>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    
    <xs:element name="Documentation">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="country" type="xs:string"/>
                <xs:element ref="Maker"/>
            </xs:sequence>
        </xs:complexType>
    
    </xs:element>

    <xs:element name="Car">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="colour" type="xs:string"/>
                <xs:element name="make" type="xs:string"/>
                <xs:element ref="Documentation"/>
            </xs:sequence>
        </xs:complexType>
        
    </xs:element>
    
        <xs:element name="Ship">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="colour" type="xs:string"/>
                <xs:element name="make" type="xs:string"/>
                <xs:element ref="Documentation"/>
            </xs:sequence>
        </xs:complexType>
        
    </xs:element>

</xs:schema>
"""

    table = """
 id | d | r | b | m | property                 | type            | ref           | source                              | prepare | level | access | uri | title | description
    | manifest                                 |                 |               |                                     |         |       |        |     |       |
    |   | resource1                            | dask/xml        |               |                                     |         |       |        |     |       |
    |                                          |                 |               |                                     |         |       |        |     |       |
    |   |   |   | Car                          |                 |               | /Car                                |         |       |        |     |       |
    |   |   |   |   | colour                   | string required |               | colour/text()                       |         |       |        |     |       |
    |   |   |   |   | documentation            | ref required    | Documentation | Documentation                       |         |       |        |     |       |
    |   |   |   |   | documentation.country    | string required |               | Documentation/country/text()        |         |       |        |     |       |
    |   |   |   |   | documentation.maker      | ref required    | Maker         | Documentation/Maker                 |         |       |        |     |       |
    |   |   |   |   | documentation.maker.code | string required |               | Documentation/Maker/code/text()     |         |       |        |     |       |
    |   |   |   |   | documentation.maker.name | string required |               | Documentation/Maker/name/text()     |         |       |        |     |       |
    |   |   |   |   | make                     | string required |               | make/text()                         |         |       |        |     |       |
    |                                          |                 |               |                                     |         |       |        |     |       |
    |   |   |   | Documentation                |                 |               |                                     |         |       |        |     |       |
    |   |   |   |   | country                  | string required |               | country/text()                      |         |       |        |     |       |
    |   |   |   |   | maker                    | ref required    | Maker         | Maker                               |         |       |        |     |       |
    |                                          |                 |               |                                     |         |       |        |     |       |
    |   |   |   | Maker                        |                 |               |                                     |         |       |        |     |       |
    |   |   |   |   | code                     | string required |               | code/text()                         |         |       |        |     |       |
    |   |   |   |   | name                     | string required |               | name/text()                         |         |       |        |     |       |
    |                                          |                 |               |                                     |         |       |        |     |       |
    |   |   |   | Ship                         |                 |               | /Ship                               |         |       |        |     |       |
    |   |   |   |   | colour                   | string required |               | colour/text()                       |         |       |        |     |       |
    |   |   |   |   | documentation            | ref required    | Documentation | Documentation                       |         |       |        |     |       |
    |   |   |   |   | documentation.country    | string required |               | Car/Documentation/country/text()    |         |       |        |     |       |
    |   |   |   |   | documentation.maker      | ref required    | Maker         | Car/Documentation/Maker             |         |       |        |     |       |
    |   |   |   |   | documentation.maker.code | string required |               | Car/Documentation/Maker/code/text() |         |       |        |     |       |
    |   |   |   |   | documentation.maker.name | string required |               | Car/Documentation/Maker/name/text() |         |       |        |     |       |
    |   |   |   |   | make                     | string required |               | make/text()                         |         |       |        |     |       |

  """

    path = tmp_path / "manifest.xsd"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table


def test_xsd_resource_model_only(rc: RawConfig, tmp_path: Path):
    # recursion in XSD
    xsd = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
<xs:element name="name" type="xs:string"/>
<xs:element name="population" type="xs:int"/>
</xs:schema>
"""

    table = """
id | d | r | b | m | property   | type             | ref | source            | prepare | level | access | uri                                           | title | description
   | manifest                   |                  |     |                   |         |       |        |                                               |       |
   |   | resource1              | dask/xml         |     |                   |         |       |        |                                               |       |
   |                            |                  |     |                   |         |       |        |                                               |       |
   |   |   |   | Resource       |                  |     | /                 |         |       |        | http://www.w3.org/2000/01/rdf-schema#Resource |       | Įvairūs duomenys
   |   |   |   |   | name       | string           |     | name/text()       |         |       |        |                                               |       |
   |   |   |   |   | population | integer          |     | population/text() |         |       |        |                                               |       |
"""
    # todo actually, those properties should be required. Fixed in the new version

    path = tmp_path / "manifest.xsd"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table
