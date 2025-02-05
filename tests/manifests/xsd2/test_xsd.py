from pathlib import Path

import pytest

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
 id | d | r | b | m | property          | type             | ref              | source            | source.type | prepare  | level | access | uri | title | description | status  | visibility | eli | count | origin
    | manifest                          |                  |                  |                   |             |          |       |        |     |       |             | develop | private    |     |       |       
    |   | resource1                     | xml              |                  |                   |             |          |       |        |     |       |             | develop | private    |     |       |       
    |                                   |                  |                  |                   |             |          |       |        |     |       |             | develop | private    |     |       |       
    |   |   |   | Administraciniai      |                  |                  | /ADMINISTRACINIAI |             |          |       |        |     |       |             | develop | private    |     |       |       
    |   |   |   |   | administracinis[] | backref          | Administracinis  | ADMINISTRACINIS   |             | expand() |       |        |     |       |             | develop | private    |     |       |       
    |                                   |                  |                  |                   |             |          |       |        |     |       |             | develop | private    |     |       |       
    |   |   |   | Administracinis/:part |                  |                  |                   |             |          |       |        |     |       |             | develop | private    |     |       |       
    |   |   |   |   | adm_id            | integer required |                  | ADM_ID/text()     |             |          |       |        |     |       |             | develop | private    |     |       |       
    |   |   |   |   | adm_kodas         | integer required |                  | ADM_KODAS/text()  |             |          |       |        |     |       |             | develop | private    |     |       |       
    |   |   |   |   | administraciniai  | ref              | Administraciniai |                   |             |          |       |        |     |       |             | develop | private    |     |       |       
    |                                   |                  |                  |                   |             |          |       |        |     |       |             | develop | private    |     |       |       
    |   |   |   | Gyvenviete/:part      |                  |                  |                   |             |          |       |        |     |       |             | develop | private    |     |       |       
    |   |   |   |   | gyv_id            | integer required |                  | GYV_ID/text()     |             |          |       |        |     |       |             | develop | private    |     |       |       
    |   |   |   |   | gyv_kodas         | integer required |                  | GYV_KODAS/text()  |             |          |       |        |     |       |             | develop | private    |     |       |       
    |   |   |   |   | gyvenvietes       | ref              | Gyvenvietes      |                   |             |          |       |        |     |       |             | develop | private    |     |       |       
    |                                   |                  |                  |                   |             |          |       |        |     |       |             | develop | private    |     |       |       
    |   |   |   | Gyvenvietes           |                  |                  | /GYVENVIETES      |             |          |       |        |     |       |             | develop | private    |     |       |       
    |   |   |   |   | gyvenviete[]      | backref          | Gyvenviete       | GYVENVIETE        |             | expand() |       |        |     |       |             | develop | private    |     |       |       

"""
    path = tmp_path / 'manifest.xsd'
    path_xsd2 = f"xsd2+file://{path}"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path_xsd2)
    assert manifest == table


def test_xsd_backref(rc: RawConfig, tmp_path: Path):
    xsd = """

<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
<xs:element name="asmenys">
  <xs:complexType>
    <xs:sequence>
      <xs:element ref="asmuo" maxOccurs="unbounded" />
    </xs:sequence>
    <xs:attribute name="puslapis" type="xs:long" />
  </xs:complexType>
</xs:element>

<xs:element name="asmuo">
  <xs:complexType>
      <xs:attribute name="id" type="xs:string" />
      <xs:attribute name="ak" type="xs:string" />
  </xs:complexType>
</xs:element>
</xs:schema>
    """

    table = """
 id | d | r | b | m | property    | type             | ref     | source    | prepare  | level | access | uri | title | description  | status  | visibility | eli | count | origin |
    | manifest                    |                  |         |           |          |       |        |     |       |              | develop | private    |     |       |        |
    |   | resource1               | xml              |         |           |          |       |        |     |       |              | develop | private    |     |       |        |
    |                             |                  |         |           |          |       |        |     |       |              | develop | private    |     |       |        |
    |   |   |   | Asmenys         |                  |         | /asmenys  |          |       |        |     |       |              | develop | private    |     |       |        |
    |   |   |   |   | asmuo[]     | backref required | Asmuo   | asmuo     | expand() |       |        |     |       |              | develop | private    |     |       |        |
    |   |   |   |   | puslapis    | integer          |         | @puslapis |          |       |        |     |       |              | develop | private    |     |       |        |
    |                             |                  |         |           |          |       |        |     |       |              | develop | private    |     |       |        |
    |   |   |   | Asmuo/:part     |                  |         |           |          |       |        |     |       |              | develop | private    |     |       |        |
    |   |   |   |   | ak          | string           |         | @ak       |          |       |        |     |       |              | develop | private    |     |       |        |
    |   |   |   |   | asmenys     | ref              | Asmenys |           |          |       |        |     |       |              | develop | private    |     |       |        |
    |   |   |   |   | id          | string           |         | @id       |          |       |        |     |       |              | develop | private    |     |       |        |
"""

    path = tmp_path / 'manifest.xsd'
    path_xsd2 = f"xsd2+file://{path}"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path_xsd2)
    print(manifest)
    assert manifest == table


def test_xsd_ref(rc: RawConfig, tmp_path: Path):
    xsd = """

<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
<xs:element name="asmenys">
  <xs:complexType mixed="true">
    <xs:sequence>
      <xs:element ref="asmuo" minOccurs="0" />
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
 id | d | r | b | m | property    | type             | ref   | source    | prepare  | level | access | uri | title | description                | status  | visibility | eli | count | origin |
    | manifest                    |                  |       |           |          |       |        |     |       |                            | develop | private    |     |       |        |
    |   | resource1               | xml              |       |           |          |       |        |     |       |                            | develop | private    |     |       |        |
    |                             |                  |       |           |          |       |        |     |       |                            | develop | private    |     |       |        |
    |   |   |   | Asmenys         |                  |       | /asmenys  |          |       |        |     |       |                            | develop | private    |     |       |        |
    |   |   |   |   | asmuo       | ref              | Asmuo | asmuo     | expand() |       |        |     |       |                            | develop | private    |     |       |        |
    |   |   |   |   | puslapis    | integer required |       | @puslapis |          |       |        |     |       | rezultatu puslapio numeris | develop | private    |     |       |        |
    |   |   |   |   | text        | string           |       | text()    |          |       |        |     |       |                            | develop | private    |     |       |        |
    |                             |                  |       |           |          |       |        |     |       |                            | develop | private    |     |       |        |
    |   |   |   | Asmuo/:part     |                  |       |           |          |       |        |     |       |                            | develop | private    |     |       |        |
    |   |   |   |   | ak          | string required  |       | @ak       |          |       |        |     |       |                            | develop | private    |     |       |        |
    |   |   |   |   | id          | string required  |       | @id       |          |       |        |     |       |                            | develop | private    |     |       |        |
    |   |   |   |   | text        | string           |       | text()    |          |       |        |     |       |                            | develop | private    |     |       |        |
"""

    path = tmp_path / 'manifest.xsd'
    path_xsd2 = f"xsd2+file://{path}"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path_xsd2)
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
 id | d | r | b | m | property   | type             | ref | source        | prepare | level | access | uri                                           | title | description                         | status  | visibility | eli | count | origin |
    | manifest                   |                  |     |               |         |       |        |                                               |       |                                     | develop | private    |     |       |        |
    |   | resource1              | xml              |     |               |         |       |        |                                               |       |                                     | develop | private    |     |       |        |
    |                            |                  |     |               |         |       |        |                                               |       |                                     | develop | private    |     |       |        |
    |   |   |   | Asmenys        |                  |     | /asmenys      |         |       |        |                                               |       |                                     | develop | private    |     |       |        |
    |   |   |   |   | puslapis   | integer required |     | @puslapis     |         |       |        |                                               |       | rezultatu puslapio numeris          | develop | private    |     |       |        |
    |   |   |   |   | text       | string           |     | text()        |         |       |        |                                               |       |                                     | develop | private    |     |       |        |
    |                            |                  |     |               |         |       |        |                                               |       |                                     | develop | private    |     |       |        |
    |   |   |   | Resource       |                  |     | /             |         |       |        | http://www.w3.org/2000/01/rdf-schema#Resource |       | Įvairūs duomenys                    | develop | private    |     |       |        |
    |   |   |   |   | klaida     | string required  |     | klaida/text() |         |       |        |                                               |       | Klaidos atveju - klaidos pranešimas | develop | private    |     |       |        |
"""

    path = tmp_path / 'manifest.xsd'
    path_xsd2 = f"xsd2+file://{path}"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path_xsd2)
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
id | d | r | b | m | property            | type            | ref              | source                         | prepare | level | access | uri | title | description                 | status  | visibility | eli | count | origin |
   | manifest                            |                 |                  |                                |         |       |        |     |       |                             | develop | private    |     |       |        |
   |   | resource1                       | xml             |                  |                                |         |       |        |     |       |                             | develop | private    |     |       |        |
   |                                     |                 |                  |                                |         |       |        |     |       |                             | develop | private    |     |       |        |
   |   |   |   | SkiepasEu               |                 |                  | /SKIEPAS_EU                    |         |       |        |     |       |                             | develop | private    |     |       |        |
   |   |   |   |   | paciento_ak         | string          |                  | PACIENTO_AK/text()             |         |       |        |     |       | Paciento asmens kodas (LTU) | develop | private    |     |       |        |
   |   |   |   |   | skiepijimo_data     | string required |                  | SKIEPIJIMO_DATA/text()         |         |       |        |     |       | Skiepijimo data - Data      | develop | private    |     |       |        |
"""

    path = tmp_path / 'manifest.xsd'                                                                                                                                                  
    path_xsd2 = f"xsd2+file://{path}"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path_xsd2)
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
 id | d | r | b | m | property             | type             | ref | source                      | prepare | level | access | uri | title | description                          | status  | visibility | eli | count | origin | 
    | manifest                             |                  |     |                             |         |       |        |     |       |                                      | develop | private    |     |       |        | 
    |   | resource1                        | xml              |     |                             |         |       |        |     |       |                                      | develop | private    |     |       |        | 
    |                                      |                  |     |                             |         |       |        |     |       |                                      | develop | private    |     |       |        | 
    |   |   |   | Parcel                   |                  |     | /parcel                     |         |       |        |     |       | Žemės sklypo pasikeitimo informacija | develop | private    |     |       |        | 
    |   |   |   |   | parcel_unique_number | integer required |     | parcel_unique_number/text() |         |       |        |     |       | Žemės sklypo unikalus numeris        | develop | private    |     |       |        | 
    |   |   |   |   | text                 | string           |     | text()                      |         |       |        |     |       |                                      | develop | private    |     |       |        | 
    |                                      |                  |     |                             |         |       |        |     |       |                                      | develop | private    |     |       |        |
    |   |   |   | Parcel1                  |                  |     | /parcel                     |         |       |        |     |       | Žemės sklypo pasikeitimo informacija | develop | private    |     |       |        |
    |   |   |   |   | sign_of_change       | integer required |     | sign_of_change/text()       |         |       |        |     |       | Žemės sklypo pasikeitimo požymis     | develop | private    |     |       |        |
    |                                      | enum             |     | 1                           |         |       |        |     |       |                                      | develop | private    |     |       |        |
    |                                      |                  |     | 2                           |         |       |        |     |       |                                      | develop | private    |     |       |        |
    |   |   |   |   | text                 | string           |     | text()                      |         |       |        |     |       |                                      | develop | private    |     |       |        |                                                                                                                                                     
"""

    path = tmp_path / 'manifest.xsd'
    path_xsd2 = f"xsd2+file://{path}"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path_xsd2)
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
 id | d | r | b | m | property               | type             | ref | source                      | prepare | level | access | uri | title | description                          | status  | visibility | eli | count | origin |
    | manifest                               |                  |     |                             |         |       |        |     |       |                                      | develop | private    |     |       |        |
    |   | resource1                          | xml              |     |                             |         |       |        |     |       |                                      | develop | private    |     |       |        |
    |                                        |                  |     |                             |         |       |        |     |       |                                      | develop | private    |     |       |        |
    |   |   |   | Parcel                     |                  |     | /parcel                     |         |       |        |     |       | Žemės sklypo pasikeitimo informacija | develop | private    |     |       |        |
    |   |   |   |   | parcel_unique_number[] | integer required |     | parcel_unique_number/text() |         |       |        |     |       | Žemės sklypo unikalus numeris        | develop | private    |     |       |        |
    |   |   |   |   | sign_of_change[]       | integer required |     | sign_of_change/text()       |         |       |        |     |       | Žemės sklypo pasikeitimo požymis     | develop | private    |     |       |        |
    |                                        | enum             |     | 1                           |         |       |        |     |       |                                      | develop | private    |     |       |        |
    |                                        |                  |     | 2                           |         |       |        |     |       |                                      | develop | private    |     |       |        |
    |   |   |   |   | text                   | string           |     | text()                      |         |       |        |     |       |                                      | develop | private    |     |       |        |
"""

    path = tmp_path / 'manifest.xsd'
    path_xsd2 = f"xsd2+file://{path}"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path_xsd2)
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
 id | d | r | b | m | property             | type            | ref     | source             | prepare | level | access | uri | title | description | status  | visibility | eli | count | origin |
    | manifest                             |                 |         |                    |         |       |        |     |       |             | develop | private    |     |       |        |
    |   | resource1                        | xml             |         |                    |         |       |        |     |       |             | develop | private    |     |       |        |
    |                                      |                 |         |                    |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   | Salyga                   |                 |         | /SALYGA            |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | kodas                | string          |         | @kodas             |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | pavadinimas          | string          |         | @pavadinimas       |         |       |        |     |       |             | develop | private    |     |       |        |
"""

    path = tmp_path / 'manifest.xsd'
    path_xsd2 = f"xsd2+file://{path}"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path_xsd2)
    print(manifest)
    assert manifest == table


def test_xsd_model_one_property(rc: RawConfig, tmp_path: Path):
    xsd = """
    <xs:schema xmlns="http://eTaarPlat.ServiceContracts/2007/08/Messages" xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified" targetNamespace="http://eTaarPlat.ServiceContracts/2007/08/Messages">

        <xs:element name="Response">
            <xs:complexType>
                <xs:sequence>
                    <xs:element minOccurs="0" maxOccurs="1" name="searchParameters" type="xs:string" />
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
 id | d | r | b | m | property          | type   | ref    | source                  | prepare  | level | access | uri | title | description | status  | visibility | eli | count | origin |
    | manifest                          |        |        |                         |          |       |        |     |       |             | develop | private    |     |       |        |
    |   | resource1                     | xml    |        |                         |          |       |        |     |       |             | develop | private    |     |       |        |
    |                                   |        |        |                         |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   | Klaida/:part          |        |        |                         |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | aprasymas         | string |        | Aprasymas/text()        |          |       |        |     |       |             | develop | private    |     |       |        |
    |                                   |        |        |                         |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   | Response              |        |        | /Response               |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | klaida            | ref    | Klaida | klaida                  | expand() |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | search_parameters | string |        | searchParameters/text() |          |       |        |     |       |             | develop | private    |     |       |        |
"""
    path = tmp_path / 'manifest.xsd'
    path_xsd2 = f"xsd2+file://{path}"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path_xsd2)
    assert manifest == table


@pytest.mark.skip("waiting for #941")
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
 id | d | r | b | m | property                | type   | ref | source                         | prepare | level | access | uri | title | description                               | status  | visibility | eli | count | origin |
    | manifest                                |        |     |                                |         |       |        |     |       |                                           | develop | private    |     |       |        |
    |   | resource1                           | xml    |     |                                |         |       |        |     |       |                                           | develop | private    |     |       |        |
    |                                         |        |     |                                |         |       |        |     |       |                                           | develop | private    |     |       |        |
    |   |   |   | Tyrimas                     |        |     | /TYRIMAS                       |         |       |        |     |       |                                           | develop | private    |     |       |        |
    |   |   |   |   | ct_e200ats_duom_sukurti | string |     | CT_E200ATS_DUOM_SUKURTI/text() |         |       |        |     |       | E200-ats duomenų sukūrimo data ir laikas  | develop | private    |     |       |        |
    |   |   |   |   | ct_paciento_spi         | string |     | CT_PACIENTO_SPI/text()         |         |       |        |     |       | Paciento prisirašymo įstaigos pavadinimas | develop | private    |     |       |        |
"""
    path = tmp_path / 'manifest.xsd'
    path_xsd2 = f"xsd2+file://{path}"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path_xsd2)
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
 id | d | r | b | m | property      | type   | ref | source              | prepare | level | access | uri | title | description | status  | visibility | eli | count | origin |
    | manifest                      |        |     |                     |         |       |        |     |       |             | develop | private    |     |       |        |
    |   | resource1                 | xml    |     |                     |         |       |        |     |       |             | develop | private    |     |       |        |
    |                               |        |     |                     |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   | Person            |        |     | /person             |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | first_name    | string |     | firstName/text()    |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | iltu_code     | string |     | iltu_code/text()    |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | last_name     | string |     | lastName/text()     |         |       |        |     |       |             | develop | private    |     |       |        |
    |                               |        |     |                     |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   | Person1           |        |     | /person             |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | business_name | string |     | businessName/text() |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | iltu_code     | string |     | iltu_code/text()    |         |       |        |     |       |             | develop | private    |     |       |        |
"""
    path = tmp_path / 'manifest.xsd'
    path_xsd2 = f"xsd2+file://{path}"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path_xsd2)
    assert manifest == table


@pytest.mark.skip(reason='waiting for #942')
def test_xsd_complex_content(rc: RawConfig, tmp_path: Path):
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
 id | d | r | b | m | property    | type   | ref | source             | prepare | level | access | uri | title | description | status  | visibility | eli | count | origin |
    | manifest                    |        |     |                    |         |       |        |     |       |             | develop | private    |     |       |        |
    |   | resource1               | xml    |     |                    |         |       |        |     |       |             | develop | private    |     |       |        |
    |                             |        |     |                    |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   | BeFull          |        |     | /BE_FULL           |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | printeddate | string |     | printeddate/text() |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | title1      | string |     | title1/text()      |         |       |        |     |       |             | develop | private    |     |       |        |
"""
    path = tmp_path / 'manifest.xsd'
    path_xsd2 = f"xsd2+file://{path}"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path_xsd2)
    assert manifest == table


def test_xsd_recursion(rc: RawConfig, tmp_path: Path):
    # recursion in XSD
    xsd = """
<s:schema xmlns:s="http://www.w3.org/2001/XMLSchema" xmlns:tns="http://rc/ireg/1.0/" targetNamespace="http://rc/ireg/1.0/" elementFormDefault="qualified" attributeFormDefault="unqualified">

        <s:element name="responseData">
            <s:complexType>
                <s:sequence>
                    <s:element minOccurs="0" maxOccurs="unbounded" name="action" type="tns:action" />
                </s:sequence>
            </s:complexType>
        </s:element>

        <s:complexType name="children">
            <s:sequence>
                <s:element minOccurs="0" maxOccurs="unbounded" name="action" type="tns:action"/>
            </s:sequence>
        </s:complexType>

        <s:complexType name="action">
            <s:sequence>
                <s:element minOccurs="1" maxOccurs="1" name="code" type="s:string" />
                <s:element minOccurs="1" maxOccurs="unbounded" name="children" type="tns:children" />
            </s:sequence>
        </s:complexType>
</s:schema>
"""

    table = """
 id | d | r | b | m | property       | type             | ref          | source        | prepare  | level | access | uri | title | description | status  | visibility | eli | count | origin |
    | manifest                       |                  |              |               |          |       |        |     |       |             | develop | private    |     |       |        |
    |   | resource1                  | xml              |              |               |          |       |        |     |       |             | develop | private    |     |       |        |
    |                                |                  |              |               |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   | Action/:part       |                  |              |               |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | children[]     | backref required | Children     | children      | expand() |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | children1      | ref              | Children     |               |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | code           | string required  |              | code/text()   |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | response_data  | ref              | ResponseData |               |          |       |        |     |       |             | develop | private    |     |       |        |
    |                                |                  |              |               |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   | Children/:part     |                  |              |               |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | action[]       | backref          | Action       | action        | expand() |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | action1        | ref              | Action       |               |          |       |        |     |       |             | develop | private    |     |       |        |
    |                                |                  |              |               |          |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   | ResponseData       |                  |              | /responseData |          |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | action[]       | backref          | Action       | action        | expand() |       |        |     |       |             | develop | private    |     |       |        |   
"""
    path = tmp_path / 'manifest.xsd'
    path_xsd2 = f"xsd2+file://{path}"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path_xsd2)
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
                <s:element name="who_may_constitute" minOccurs="1" maxOccurs="1">
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
 id | d | r | b | m | property           | type            | ref    | source                    | prepare  | level | access | uri | title | description | status  | visibility | eli | count | origin |
    | manifest                           |                 |        |                           |          |       |        |     |       |             | develop | private    |     |       |        |
    |   | resource1                      | xml             |        |                           |          |       |        |     |       |             | develop | private    |     |       |        |
    |                                    |                 |        |                           |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   | Action/:part           |                 |        |                           |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | who_may_constitute | string required |        | who_may_constitute/text() |          |       |        |     |       |             | develop | private    |     |       |        |
    |                                    | enum            |        | fiz                       |          |       |        |     |       |             | develop | private    |     |       |        |
    |                                    |                 |        | fiz-notarial              |          |       |        |     |       |             | develop | private    |     |       |        |
    |                                    |                 |        |                           |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   | Data                   |                 |        | /data                     |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | response_data      | ref             | Action | responseData              | expand() |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | response_message   | string          |        | responseMessage/text()    |          |       |        |     |       |             | develop | private    |     |       |        |
"""
    path = tmp_path / 'manifest.xsd'
    path_xsd2 = f"xsd2+file://{path}"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path_xsd2)
    assert manifest == table


@pytest.mark.skip(reason='waiting for 942')
def test_duplicate_removal(rc: RawConfig, tmp_path: Path):
    xsd = """
<xs:schema xmlns="http://countriesCities.ServiceContracts/2024/11/Messages" elementFormDefault="qualified" targetNamespace="http://countriesCities.ServiceContracts/2024/11/Messages" xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="getCitiesByCountry">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="queryParameters" />
                <xs:choice>
                    <xs:element minOccurs="0" name="error" type="Error" />
                    <xs:element minOccurs="0" name="cityList" type="CityList" />
                </xs:choice>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    
    <xs:complexType name="Error">
        <xs:sequence>
            <xs:element minOccurs="0" maxOccurs="1" name="description" type="xs:string" />
        </xs:sequence>
    </xs:complexType>
    
    <xs:complexType name="CityList">
        <xs:sequence>
            <xs:element name="retrievalTime" type="xs:dateTime" />
            <xs:element name="lastUpdateTime" type="xs:dateTime" />
        </xs:sequence>
    </xs:complexType>
</xs:schema>

"""

    table = """
 id | d | r | b | m | property                         | type              | ref              | source                                        | prepare  | level | access | uri | title | description | status  | visibility | eli | count | origin |
    | manifest                                         |                   |                  |                                               |          |       |        |     |       |             | develop | private    |     |       |        |
    |   | resource1                                    | xml               |                  |                                               |          |       |        |     |       |             | develop | private    |     |       |        |
    |                                                  |                   |                  |                                               |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   | Extract/:part                        |                   |                  |                                               |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | extract_preparation_time         | datetime required |                  | extractPreparationTime/text()                 |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | last_update_time                 | datetime required |                  | lastUpdateTime/text()                         |          |       |        |     |       |             | develop | private    |     |       |        |
    |                                                  |                   |                  |                                               |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   | GetDocumentsByWagonResponse1         |                   |                  | /getDocumentsByWagonResponse                  |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | aprasymas                        | string            |                  | klaida/Aprasymas/text()                       |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | search_parameters                | ref               | SearchParameters | searchParameters                              | expand() |       |        |     |       |             | develop | private    |     |       |        |
    |                                                  |                   |                  |                                               |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   | GetDocumentsByWagonResponse2         |                   |                  | /getDocumentsByWagonResponse                  |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | extract                          | ref               | Extract          | extract                                       |          |       |        |     |       |             | develop | private    |     |       |        |   
    |                                                  |                   |                  |                                               |          |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   | SearchParameters                     |                   |                  |                                               |          |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | code                             | string            |                  | code/text()                                   |          |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | search_type                      | string            |                  | searchType/text()                             |          |       |        |     |       |             | develop | private    |     |       |        |   
  """

    path = tmp_path / 'manifest.xsd'
    path_xsd2 = f"xsd2+file://{path}"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path_xsd2)
    assert manifest == table


@pytest.mark.skip(reason='waiting for #942')
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
 id | d | r | b | m | property                         | type              | ref              | source                                        | prepare | level | access | uri | title | description | status  | visibility | eli | count | origin |
    | manifest                                         |                   |                  |                                               |         |       |        |     |       |             | develop | private    |     |       |        |
    |   | resource1                                    | xml               |                  |                                               |         |       |        |     |       |             | develop | private    |     |       |        |
    |                                                  |                   |                  |                                               |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   | Extract                              |                   |                  |                                               |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | extract_preparation_time         | datetime required |                  | extractPreparationTime/text()                 |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | last_update_time                 | datetime required |                  | lastUpdateTime/text()                         |         |       |        |     |       |             | develop | private    |     |       |        |
    |                                                  |                   |                  |                                               |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   | GetDocumentsByWagonResponse1         |                   |                  | /getDocumentsByWagonResponse                  |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | aprasymas                        | string            |                  | klaida/Aprasymas/text()                       |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | search_parameters[]              | backref           | SearchParameters | searchParameters                              |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | search_parameters[].code         | string            |                  | code/text()                                   |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | search_parameters[].search_type  | string            |                  | searchType/text()                             |         |       |        |     |       |             | develop | private    |     |       |        |
    |                                                  |                   |                  |                                               |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   | GetDocumentsByWagonResponse2         |                   |                  | /getDocumentsByWagonResponse                  |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | extract                          | ref               | Extract          | extract                                       |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | extract.extract_preparation_time | datetime required |                  | extract/extractPreparationTime/text()         |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | extract.last_update_time         | datetime required |                  | extract/lastUpdateTime/text()                 |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | search_parameters[]              | backref           | SearchParameters | searchParameters                              |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | search_parameters[].code         | string            |                  | code/text()                                   |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | search_parameters[].search_type  | string            |                  | searchType/text()                             |         |       |        |     |       |             | develop | private    |     |       |        |   
    |                                                  |                   |                  |                                               |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   | SearchParameters                     |                   |                  |                                               |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | code                             | string            |                  | code/text()                                   |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | get_documents_by_wagon_response1 | ref               | GetDocumentsByWagonResponse1 |                                               |         |       |        |     |       | | develop | private    |     |       |        |   
    |   |   |   |   | get_documents_by_wagon_response2 | ref               | GetDocumentsByWagonResponse2 |                                               |         |       |        |     |       | | develop | private    |     |       |        |   
    |   |   |   |   | search_type                      | string            |                  | searchType/text()                             |         |       |        |     |       |             | develop | private    |     |       |        |                                                                                                                                                                                              | develop | private    |     |       |        |   
  """

    path = tmp_path / 'manifest.xsd'
    path_xsd2 = f"xsd2+file://{path}"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path_xsd2)
    assert manifest == table


@pytest.mark.skip(reason='waiting for 942')
def test_duplicate_removal_different_models(rc: RawConfig, tmp_path: Path):
    """
    in this situation, "Extract" model has to be only once
    """
    xsd = """
    <xs:schema xmlns="http://eTaarPlat.ServiceContracts/2007/08/Messages" elementFormDefault="qualified" targetNamespace="http://eTaarPlat.ServiceContracts/2007/08/Messages" xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <xs:element name="Wagon" type="Wagon" />
        <xs:complexType name="Wagon">
            <xs:sequence>
                <xs:element name="searchParameters" type="xs:string" />
                <xs:choice>
                    <xs:element minOccurs="0" name="klaida" type="Klaida" />
                    <xs:element minOccurs="0" name="extract" type="Extract" />
                </xs:choice>
            </xs:sequence>
        </xs:complexType>
        <xs:element name="AirCraft" type="AirCraft" />
        <xs:complexType name="AirCraft">
            <xs:sequence>
                <xs:element name="searchParameters" type="xs:string" />
                <xs:choice>
                    <xs:element minOccurs="0" name="klaida" type="Klaida" />
                    <xs:element minOccurs="0" name="extract" type="Extract" />
                </xs:choice>
            </xs:sequence>
        </xs:complexType>
        <xs:complexType name="Klaida">
                    <xs:sequence>
                        <xs:element name="Aprasymas" type="xs:string" />
                    </xs:sequence>
        </xs:complexType>
        <xs:complexType name="Extract">
                    <xs:sequence>
                        <xs:element name="extractPreparationTime" type="xs:dateTime" />
                        <xs:element name="lastUpdateTime" type="xs:dateTime" />
                    </xs:sequence>
        </xs:complexType>
    </xs:schema>
"""

    table = """
 id | d | r | b | m | property                         | type              | ref              | source                                                            | prepare | level | access | uri | title | description | status  | visibility | eli | count | origin |
    | manifest                                         |                   |                  |                                                                   |         |       |        |     |       |             | develop | private    |     |       |        |
    |   | resource1                                    | xml               |                  |                                                                   |         |       |        |     |       |             | develop | private    |     |       |        |
    |                                                  |                   |                  |                                                                   |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   | Extract                              |                   |                  |                                                                   |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | extract_preparation_time         | datetime required |                  | extractPreparationTime/text()                                     |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | last_update_time                 | datetime required |                  | lastUpdateTime/text()                                             |         |       |        |     |       |             | develop | private    |     |       |        |
    |                                                  |                   |                  |                                                                   |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   | GetDocumentsByAirCraftResponse1      |                   |                  | /getDocumentsByAirCraftResponse                                   |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | aprasymas                        | string            |                  | klaida/Aprasymas/text()                                           |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | search_parameters                | ref               | SearchParameters | searchParameters                                                  |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | search_parameters.code           | string            |                  | getDocumentsByWagonResponse/searchParameters/code/text()          |         |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | search_parameters.search_type    | string            |                  | getDocumentsByWagonResponse/searchParameters/searchType/text()    |         |       |        |     |       |             | develop | private    |     |       |        |
    |                                                  |                   |                  |                                                                   |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   | GetDocumentsByAirCraftResponse2      |                   |                  | /getDocumentsByAirCraftResponse                                   |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | extract                          | ref               | Extract          | extract                                                           |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | extract.extract_preparation_time | datetime required |                  | getDocumentsByWagonResponse/extract/extractPreparationTime/text() |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | extract.last_update_time         | datetime required |                  | getDocumentsByWagonResponse/extract/lastUpdateTime/text()         |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | search_parameters                | ref               | SearchParameters | searchParameters                                                  |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | search_parameters.code           | string            |                  | getDocumentsByWagonResponse/searchParameters/code/text()          |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | search_parameters.search_type    | string            |                  | getDocumentsByWagonResponse/searchParameters/searchType/text()    |         |       |        |     |       |             | develop | private    |     |       |        |   
    |                                                  |                   |                  |                                                                   |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   | GetDocumentsByWagonResponse1         |                   |                  | /getDocumentsByWagonResponse                                      |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | aprasymas                        | string            |                  | klaida/Aprasymas/text()                                           |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | search_parameters                | ref               | SearchParameters | searchParameters                                                  |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | search_parameters.code           | string            |                  | searchParameters/code/text()                                      |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | search_parameters.search_type    | string            |                  | searchParameters/searchType/text()                                |         |       |        |     |       |             | develop | private    |     |       |        |   
    |                                                  |                   |                  |                                                                   |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   | GetDocumentsByWagonResponse2         |                   |                  | /getDocumentsByWagonResponse                                      |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | extract                          | ref               | Extract          | extract                                                           |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | extract.extract_preparation_time | datetime required |                  | extract/extractPreparationTime/text()                             |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | extract.last_update_time         | datetime required |                  | extract/lastUpdateTime/text()                                     |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | search_parameters                | ref               | SearchParameters | searchParameters                                                  |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | search_parameters.code           | string            |                  | searchParameters/code/text()                                      |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | search_parameters.search_type    | string            |                  | searchParameters/searchType/text()                                |         |       |        |     |       |             | develop | private    |     |       |        |   
    |                                                  |                   |                  |                                                                   |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   | SearchParameters                     |                   |                  |                                                                   |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | code                             | string            |                  | code/text()                                                       |         |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | search_type                      | string            |                  | searchType/text()                                                 |         |       |        |     |       |             | develop | private    |     |       |        |   
  """

    path = tmp_path / 'manifest.xsd'
    path_xsd2 = f"xsd2+file://{path}"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path_xsd2)
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
 id | d | r | b | m | property                 | type            | ref           | source                              | prepare  | level | access | uri | title | description | status  | visibility | eli | count | origin |
    | manifest                                 |                 |               |                                     |          |       |        |     |       |             | develop | private    |     |       |        |
    |   | resource1                            | xml             |               |                                     |          |       |        |     |       |             | develop | private    |     |       |        |
    |                                          |                 |               |                                     |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   | Car                          |                 |               | /Car                                |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | colour                   | string required |               | colour/text()                       |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | documentation            | ref required    | Documentation | Documentation                       | expand() |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | make                     | string required |               | make/text()                         |          |       |        |     |       |             | develop | private    |     |       |        |
    |                                          |                 |               |                                     |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   | Documentation/:part          |                 |               |                                     |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | country                  | string required |               | country/text()                      |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   |   | maker                    | ref required    | Maker         | Maker                               | expand() |       |        |     |       |             | develop | private    |     |       |        |
    |                                          |                 |               |                                     |          |       |        |     |       |             | develop | private    |     |       |        |
    |   |   |   | Maker/:part                  |                 |               |                                     |          |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | code                     | string required |               | code/text()                         |          |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | name                     | string required |               | name/text()                         |          |       |        |     |       |             | develop | private    |     |       |        |   
    |                                          |                 |               |                                     |          |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   | Ship                         |                 |               | /Ship                               |          |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | colour                   | string required |               | colour/text()                       |          |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | documentation            | ref required    | Documentation | Documentation                       | expand() |       |        |     |       |             | develop | private    |     |       |        |   
    |   |   |   |   | make                     | string required |               | make/text()                         |          |       |        |     |       |             | develop | private    |     |       |        |   

  """

    path = tmp_path / 'manifest.xsd'
    path_xsd2 = f"xsd2+file://{path}"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path_xsd2)
    assert manifest == table


def test_xsd_resource_model_only(rc: RawConfig, tmp_path: Path):
    xsd = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
<xs:element name="name" type="xs:string"/>
<xs:element name="population" type="xs:int"/>
</xs:schema>
"""

    table = """
id | d | r | b | m | property   | type             | ref | source            | prepare | level | access | uri                                           | title | description      | status  | visibility | eli | count | origin |
   | manifest                   |                  |     |                   |         |       |        |                                               |       |                  | develop | private    |     |       |        |
   |   | resource1              | xml              |     |                   |         |       |        |                                               |       |                  | develop | private    |     |       |        |
   |                            |                  |     |                   |         |       |        |                                               |       |                  | develop | private    |     |       |        |
   |   |   |   | Resource       |                  |     | /                 |         |       |        | http://www.w3.org/2000/01/rdf-schema#Resource |       | Įvairūs duomenys | develop | private    |     |       |        |
   |   |   |   |   | name       | string required  |     | name/text()       |         |       |        |                                               |       |                  | develop | private    |     |       |        |
   |   |   |   |   | population | integer required |     | population/text() |         |       |        |                                               |       |                  | develop | private    |     |       |        |
"""
    path = tmp_path / 'manifest.xsd'
    path_xsd2 = f"xsd2+file://{path}"
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path_xsd2)
    assert manifest == table
