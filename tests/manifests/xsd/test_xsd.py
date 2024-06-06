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
              <xs:element type="xs:string" name="TIPAS"/>
              <xs:element type="xs:string" name="TIPO_SANTRUMPA"/>
              <xs:element type="xs:string" name="VARDAS_K"/>
              <xs:element type="xs:string" name="VARDAS_K_LOT"/>
              <xs:element type="xs:long" name="PRIKLAUSO_KODAS"/>
              <xs:element type="xs:long" name="GYV_KODAS"/>
              <xs:element type="xs:date" name="NUO"/>
              <xs:element type="xs:date" name="IKI"/>
              <xs:element type="xs:date" name="ADM_NUO"/>
              <xs:element type="xs:date" name="ADM_IKI"/>
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
              <xs:element type="xs:string" name="TIPAS"/>
              <xs:element type="xs:string" name="TIPO_SANTRUMPA"/>
              <xs:element type="xs:string" name="VARDAS_V"/>
              <xs:element type="xs:string" name="VARDAS_V_LOT"/>
              <xs:element type="xs:string" name="VARDAS_K"/>
              <xs:element type="xs:string" name="VARDAS_K_LOT"/>
              <xs:element type="xs:long" name="ADM_KODAS"/>
              <xs:element type="xs:date" name="NUO"/>
              <xs:element type="xs:date" name="IKI"/>
              <xs:element type="xs:date" name="GYV_NUO"/>
              <xs:element type="xs:date" name="GYV_IKI"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
  <xs:element name="GATVES">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="GATVE" maxOccurs="unbounded" minOccurs="0">
          <xs:complexType>
            <xs:sequence>
              <xs:element type="xs:long" name="GAT_KODAS"/>
              <xs:element type="xs:long" name="GAT_ID"/>
              <xs:element type="xs:string" name="TIPAS"/>
              <xs:element type="xs:string" name="TIPO_SANTRUMPA"/>
              <xs:element type="xs:string" name="VARDAS_K"/>
              <xs:element type="xs:string" name="VARDAS_K_LOT"/>
              <xs:element type="xs:long" name="GYV_KODAS"/>
              <xs:element type="xs:date" name="NUO"/>
              <xs:element type="xs:date" name="IKI"/>
              <xs:element type="xs:date" name="GAT_NUO"/>
              <xs:element type="xs:date" name="GAT_IKI"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
  <xs:element name="ADRESAI">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="ADRESAS" maxOccurs="unbounded" minOccurs="0">
          <xs:complexType>
            <xs:sequence>
              <xs:element type="xs:long" name="AOB_KODAS"/>
              <xs:element type="xs:long" name="AOB_ID"/>
              <xs:element type="xs:long" name="GYV_KODAS"/>
              <xs:element type="xs:long" name="GAT_KODAS"/>
              <xs:element type="xs:string" name="NR"/>
              <xs:element type="xs:string" name="KORPUSO_NR"/>
              <xs:element type="xs:string" name="PASTO_KODAS"/>
              <xs:element type="xs:date" name="NUO"/>
              <xs:element type="xs:date" name="IKI"/>
              <xs:element type="xs:date" name="AOB_NUO"/>
              <xs:element type="xs:date" name="AOB_IKI"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
  <xs:element name="PATALPOS">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="PATALPA" maxOccurs="unbounded" minOccurs="0">
          <xs:complexType>
            <xs:sequence>
              <xs:element type="xs:long" name="PAT_KODAS"/>
              <xs:element type="xs:long" name="PAT_ID"/>
              <xs:element type="xs:long" name="AOB_KODAS"/>
              <xs:element type="xs:string" name="PATALPOS_NR"/>
              <xs:element type="xs:date" name="NUO"/>
              <xs:element type="xs:date" name="IKI"/>
              <xs:element type="xs:date" name="PAT_NUO"/>
              <xs:element type="xs:date" name="PAT_IKI"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
  <xs:element name="KODAI">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="KODAS" maxOccurs="unbounded" minOccurs="0">
          <xs:complexType>
            <xs:sequence>
              <xs:element type="xs:string" name="PASTO_KODAS"/>
              <xs:element type="xs:string" name="PASTO_VIET_PAV"/>
              <xs:element type="xs:date" name="NUO"/>
              <xs:element type="xs:date" name="IKI"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>
    """

    table = """
d | r | b | m | property        | type    | ref | source                            | prepare | level | access | uri | title | description
manifest                        |         |     |                                   |         |       |        |     |       |
  | resource1                   | xml     |     |                                   |         |       |        |     |       |
                                |         |     |                                   |         |       |        |     |       |
  |   |   | Administracinis     |         |     | /ADMINISTRACINIAI/ADMINISTRACINIS |         |       |        |     |       |
  |   |   |   | adm_kodas       | integer required |     | ADM_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | adm_id          | integer required |     | ADM_ID/text()                     |         |       |        |     |       |
  |   |   |   | tipas           | string required |     | TIPAS/text()                      |         |       |        |     |       |
  |   |   |   | tipo_santrumpa  | string required |     | TIPO_SANTRUMPA/text()             |         |       |        |     |       |
  |   |   |   | vardas_k        | string required |     | VARDAS_K/text()                   |         |       |        |     |       |
  |   |   |   | vardas_k_lot    | string required |     | VARDAS_K_LOT/text()               |         |       |        |     |       |
  |   |   |   | priklauso_kodas | integer required |     | PRIKLAUSO_KODAS/text()            |         |       |        |     |       |
  |   |   |   | gyv_kodas       | integer required |     | GYV_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | nuo             | date required |     | NUO/text()                        |         |       |        |     |       |
  |   |   |   | iki             | date required |     | IKI/text()                        |         |       |        |     |       |
  |   |   |   | adm_nuo         | date required |     | ADM_NUO/text()                    |         |       |        |     |       |
  |   |   |   | adm_iki         | date required |     | ADM_IKI/text()                    |         |       |        |     |       |
                                |         |     |                                   |         |       |        |     |       |
  |   |   | Gyvenviete          |         |     | /GYVENVIETES/GYVENVIETE           |         |       |        |     |       |
  |   |   |   | gyv_kodas       | integer required |     | GYV_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | gyv_id          | integer required |     | GYV_ID/text()                     |         |       |        |     |       |
  |   |   |   | tipas           | string required |     | TIPAS/text()                      |         |       |        |     |       |
  |   |   |   | tipo_santrumpa  | string required |     | TIPO_SANTRUMPA/text()             |         |       |        |     |       |
  |   |   |   | vardas_v        | string required |     | VARDAS_V/text()                   |         |       |        |     |       |
  |   |   |   | vardas_v_lot    | string required |     | VARDAS_V_LOT/text()               |         |       |        |     |       |
  |   |   |   | vardas_k        | string required |     | VARDAS_K/text()                   |         |       |        |     |       |
  |   |   |   | vardas_k_lot    | string required |     | VARDAS_K_LOT/text()               |         |       |        |     |       |
  |   |   |   | adm_kodas       | integer required |     | ADM_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | nuo             | date required |     | NUO/text()                        |         |       |        |     |       |
  |   |   |   | iki             | date required |     | IKI/text()                        |         |       |        |     |       |
  |   |   |   | gyv_nuo         | date required |     | GYV_NUO/text()                    |         |       |        |     |       |
  |   |   |   | gyv_iki         | date required |     | GYV_IKI/text()                    |         |       |        |     |       |
                                |         |     |                                   |         |       |        |     |       |
  |   |   | Gatve               |         |     | /GATVES/GATVE                     |         |       |        |     |       |
  |   |   |   | gat_kodas       | integer required |     | GAT_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | gat_id          | integer required |     | GAT_ID/text()                     |         |       |        |     |       |
  |   |   |   | tipas           | string required |     | TIPAS/text()                      |         |       |        |     |       |
  |   |   |   | tipo_santrumpa  | string required |     | TIPO_SANTRUMPA/text()             |         |       |        |     |       |
  |   |   |   | vardas_k        | string required |     | VARDAS_K/text()                   |         |       |        |     |       |
  |   |   |   | vardas_k_lot    | string required |     | VARDAS_K_LOT/text()               |         |       |        |     |       |
  |   |   |   | gyv_kodas       | integer required |     | GYV_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | nuo             | date required |     | NUO/text()                        |         |       |        |     |       |
  |   |   |   | iki             | date required |     | IKI/text()                        |         |       |        |     |       |
  |   |   |   | gat_nuo         | date required |     | GAT_NUO/text()                    |         |       |        |     |       |
  |   |   |   | gat_iki         | date required |     | GAT_IKI/text()                    |         |       |        |     |       |
                                |         |     |                                   |         |       |        |     |       |
  |   |   | Adresas             |         |     | /ADRESAI/ADRESAS                  |         |       |        |     |       |
  |   |   |   | aob_kodas       | integer required |     | AOB_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | aob_id          | integer required |     | AOB_ID/text()                     |         |       |        |     |       |
  |   |   |   | gyv_kodas       | integer required |     | GYV_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | gat_kodas       | integer required |     | GAT_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | nr              | string required |     | NR/text()                         |         |       |        |     |       |
  |   |   |   | korpuso_nr      | string required |     | KORPUSO_NR/text()                 |         |       |        |     |       |
  |   |   |   | pasto_kodas     | string required |     | PASTO_KODAS/text()                |         |       |        |     |       |
  |   |   |   | nuo             | date required |     | NUO/text()                        |         |       |        |     |       |
  |   |   |   | iki             | date required |     | IKI/text()                        |         |       |        |     |       |
  |   |   |   | aob_nuo         | date required |     | AOB_NUO/text()                    |         |       |        |     |       |
  |   |   |   | aob_iki         | date required |     | AOB_IKI/text()                    |         |       |        |     |       |
                                |         |     |                                   |         |       |        |     |       |
  |   |   | Patalpa             |         |     | /PATALPOS/PATALPA                 |         |       |        |     |       |
  |   |   |   | pat_kodas       | integer required |     | PAT_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | pat_id          | integer required |     | PAT_ID/text()                     |         |       |        |     |       |
  |   |   |   | aob_kodas       | integer required |     | AOB_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | patalpos_nr     | string required |     | PATALPOS_NR/text()                |         |       |        |     |       |
  |   |   |   | nuo             | date required |     | NUO/text()                        |         |       |        |     |       |
  |   |   |   | iki             | date required |     | IKI/text()                        |         |       |        |     |       |
  |   |   |   | pat_nuo         | date required |     | PAT_NUO/text()                    |         |       |        |     |       |
  |   |   |   | pat_iki         | date required |     | PAT_IKI/text()                    |         |       |        |     |       |
                                |         |     |                                   |         |       |        |     |       |
  |   |   | Kodas               |         |     | /KODAI/KODAS                      |         |       |        |     |       |
  |   |   |   | pasto_kodas     | string required |     | PASTO_KODAS/text()                |         |       |        |     |       |
  |   |   |   | pasto_viet_pav  | string required |     | PASTO_VIET_PAV/text()             |         |       |        |     |       |
  |   |   |   | nuo             | date required |     | NUO/text()                        |         |       |        |     |       |
  |   |   |   | iki             | date required |     | IKI/text()                        |         |       |        |     |       |
  """
    path = tmp_path / 'manifest.xsd'
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table


def test_xsd_ref(rc: RawConfig, tmp_path: Path):

    xsd = """

<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">


<xs:element name="klientu_saraso_rezultatas">
  <xs:complexType mixed="true">
    <xs:sequence>
      <xs:element ref="asmenys"               minOccurs="0" maxOccurs="1" />
    </xs:sequence>
  </xs:complexType>
</xs:element>


<xs:element name="asmenys">
  <xs:complexType mixed="true">
    <xs:sequence>
      <xs:element ref="asmuo"                 minOccurs="0" maxOccurs="unbounded" />
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
 id | d | r | b | m | property                  | type            | ref                     | source                           | prepare | level | access | uri | title | description
    | manifest                                  |                 |                         |                                  |         |       |        |     |       |
    |   | resource1                             | xml             |                         |                                  |         |       |        |     |       |
    |                                           |                 |                         |                                  |         |       |        |     |       |
    |   |   |   | Asmuo                         |                 |                         | /klientu_saraso_rezultatas/asmuo |         |       |        |     |       |
    |   |   |   |   | klientu_saraso_rezultatas | ref             | KlientuSarasoRezultatas |                                  |         |       |        |     |       |
    |   |   |   |   | id                        | string required |                         | @id                              |         |       |        |     |       |
    |   |   |   |   | ak                        | string required |                         | @ak                              |         |       |        |     |       |
    |   |   |   |   | text                      | string          |                         | text()                           |         |       |        |     |       |
    |                                           |                 |                         |                                  |         |       |        |     |       |
    |   |   |   | KlientuSarasoRezultatas       |                 |                         | /klientu_saraso_rezultatas       |         |       |        |     |       |
    |   |   |   |   | text                      | string          |                         | text()                           |         |       |        |     |       |
    |   |   |   |   | asmenys[]                 | backref         | Asmuo                   |                                  |         |       |        |     |       |
    |   |   |   |   | asmuo[].id                | string required |                         | asmuo/@id                        |         |       |        |     |       |
    |   |   |   |   | asmuo[].ak                | string required |                         | asmuo/@ak                        |         |       |        |     |       |
    |   |   |   |   | asmuo[].text              | string          |                         | asmuo/text()                     |         |       |        |     |       |
"""

    path = tmp_path / 'manifest.xsd'
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    print(manifest)
    assert manifest == table


def test_xsd_resource_model(rc: RawConfig, tmp_path: Path):

    xsd = """

<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">

<xs:element name="klientu_saraso_rezultatas">
  <xs:complexType mixed="true">
    <xs:sequence>
      <xs:element ref="asmenys"               minOccurs="0" maxOccurs="1" />
    </xs:sequence>
  </xs:complexType>
</xs:element>

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
id | d | r | b | m | property                | type             | ref      | source                                   | prepare | level | access | uri | title | description
   | manifest                                |                  |          |                                          |         |       |        |     |       |
   |   | resource1                           | xml              |          |                                          |         |       |        |     |       |
   |                                         |                  |          |                                          |         |       |        |     |       |
   |   |   |   | Resource                    |                  |          | /                                        |         |       |        | http://www.w3.org/2000/01/rdf-schema#Resource |       | Įvairūs duomenys
   |   |   |   |   | klaida                  | string           |          | klaida/text()                            |         |       |        |     |       | Klaidos atveju - klaidos pranešimas
   |                                         |                  |          |                                          |         |       |        |     |       |
   |   |   |   | Asmenys                     |                  |          | /klientu_saraso_rezultatas/asmenys       |         |       |        |     |       |
   |   |   |   |   | puslapis                | integer required |          | @puslapis                                |         |       |        |     |       | rezultatu puslapio numeris
   |   |   |   |   | text                    | string           |          | text()                                   |         |       |        |     |       |
   |                                         |                  |          |                                          |         |       |        |     |       |
   |   |   |   | KlientuSarasoRezultatas     |                  |          | /klientu_saraso_rezultatas               |         |       |        |     |       |
   |   |   |   |   | text                    | string           |          | text()                                   |         |       |        |     |       |
   |   |   |   |   | asmenys                 | ref              | Asmenys  |                                          |         |       |        |     |       |
   |   |   |   |   | asmenys.puslapis        | integer required |          | asmenys/@puslapis                        |         |       |        |     |       | rezultatu puslapio numeris
   |   |   |   |   | asmenys.text            | string           |          | asmenys/text()                           |         |       |        |     |       |

"""

    path = tmp_path / 'manifest.xsd'
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
   |   | resource1                       | xml             |                  |                                |         |       |        |     |       |
   |                                     |                 |                  |                                |         |       |        |     |       |
   |   |   |   | SkiepasEu               |                 |                  | /SKIEPAS_EU                    |         |       |        |     |       |
   |   |   |   |   | paciento_ak         | string          |                  | PACIENTO_AK/text()             |         |       |        |     |       | Paciento asmens kodas (LTU)
   |   |   |   |   | skiepijimo_data     | string required |                  | SKIEPIJIMO_DATA/text()         |         |       |        |     |       | Skiepijimo data
"""

    path = tmp_path / 'manifest.xsd'
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    print(manifest)
    assert manifest == table


def test_xsd_choice(rc: RawConfig, tmp_path: Path):

    xsd = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
	<xs:element name="parcels">
		<xs:annotation>
			<xs:documentation>Pasikeitusių žemės sklypų sąrašas</xs:documentation>
		</xs:annotation>
		<xs:complexType mixed="true">
			<xs:sequence>
				<xs:element ref="parcel" minOccurs="0" maxOccurs="unbounded"/>
			</xs:sequence>
		</xs:complexType>
	</xs:element>
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
							<xs:enumeration value="3"/> <!-- anuliuoti sklypai -->
						</xs:restriction>
					</xs:simpleType>
				</xs:element>
			</xs:choice>
		</xs:complexType>
	</xs:element>
</xs:schema>
    """

    table = """
id | d | r | b | m | property             | type             | ref     | source                      | prepare | level | access | uri | title | description
   | manifest                             |                  |         |                             |         |       |        |     |       |
   |   | resource1                        | xml              |         |                             |         |       |        |     |       |
   |                                      |                  |         |                             |         |       |        |     |       |
   |   |   |   | Parcel1                  |                  |         | /parcels/parcel             |         |       |        |     |       | Žemės sklypo pasikeitimo informacija
   |   |   |   |   | parcels              | ref              | Parcels |                             |         |       |        |     |       |
   |   |   |   |   | text                 | string           |         | text()                      |         |       |        |     |       |
   |   |   |   |   | parcel_unique_number | integer required |         | parcel_unique_number/text() |         |       |        |     |       | Žemės sklypo unikalus numeris
   |                                      |                  |         |                             |         |       |        |     |       |
   |   |   |   | Parcel2                  |                  |         | /parcels/parcel             |         |       |        |     |       | Žemės sklypo pasikeitimo informacija
   |   |   |   |   | parcels              | ref              | Parcels |                             |         |       |        |     |       |
   |   |   |   |   | text                 | string           |         | text()                      |         |       |        |     |       |
   |   |   |   |   | sign_of_change       | integer required |         | sign_of_change/text()       |         |       |        |     |       | Žemės sklypo pasikeitimo požymis
   |                                      | enum             |         | 1                           |         |       |        |     |       |
   |                                      |                  |         | 2                           |         |       |        |     |       |
   |                                      |                  |         | 3                           |         |       |        |     |       |
   |                                      |                  |         |                             |         |       |        |     |       |
   |   |   |   | Parcels                  |                  |         | /parcels                    |         |       |        |     |       | Pasikeitusių žemės sklypų sąrašas
   |   |   |   |   | text                 | string           |         | text()                      |         |       |        |     |       |
   |   |   |   |   | parcel[]             | backref          | Parcel1 |                             |         |       |        |     |       |
   |   |   |   |   | parcel1[]            | backref          | Parcel2 |                             |         |       |        |     |       |
"""

    path = tmp_path / 'manifest.xsd'
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    print(manifest)
    assert manifest == table


def test_xsd_choice_max_occurs_unbound(rc: RawConfig, tmp_path: Path):

    xsd = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
	<xs:element name="parcels">
		<xs:annotation>
			<xs:documentation>Pasikeitusių žemės sklypų sąrašas</xs:documentation>
		</xs:annotation>
		<xs:complexType mixed="true">
			<xs:sequence>
				<xs:element ref="parcel" minOccurs="0" maxOccurs="unbounded"/>
			</xs:sequence>
		</xs:complexType>
	</xs:element>
	<xs:element name="parcel">
		<xs:annotation>
			<xs:documentation>Žemės sklypo pasikeitimo informacija</xs:documentation>
		</xs:annotation>
		<xs:complexType mixed="true">
			<xs:choice maxOccurs="unbound">
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
							<xs:enumeration value="3"/> <!-- anuliuoti sklypai -->
						</xs:restriction>
					</xs:simpleType>
				</xs:element>
			</xs:choice>
		</xs:complexType>
	</xs:element>
</xs:schema>
    """

    table = """
 id | d | r | b | m | property             | type             | ref     | source                      | prepare | level | access | uri | title | description
    | manifest                             |                  |         |                             |         |       |        |     |       |
    |   | resource1                        | xml              |         |                             |         |       |        |     |       |
    |                                      |                  |         |                             |         |       |        |     |       |
    |   |   |   | Parcel1                  |                  |         | /parcels/parcel             |         |       |        |     |       | Žemės sklypo pasikeitimo informacija
    |   |   |   |   | parcels              | ref              | Parcels |                             |         |       |        |     |       |
    |   |   |   |   | text                 | string           |         | text()                      |         |       |        |     |       |
    |   |   |   |   | parcel_unique_number | integer required |         | parcel_unique_number/text() |         |       |        |     |       | Žemės sklypo unikalus numeris
    |                                      |                  |         |                             |         |       |        |     |       |
    |   |   |   | Parcel2                  |                  |         | /parcels/parcel             |         |       |        |     |       | Žemės sklypo pasikeitimo informacija
    |   |   |   |   | parcels              | ref              | Parcels |                             |         |       |        |     |       |
    |   |   |   |   | text                 | string           |         | text()                      |         |       |        |     |       |
    |   |   |   |   | sign_of_change       | integer required |         | sign_of_change/text()       |         |       |        |     |       | Žemės sklypo pasikeitimo požymis
    |                                      | enum             |         | 1                           |         |       |        |     |       |
    |                                      |                  |         | 2                           |         |       |        |     |       |
    |                                      |                  |         | 3                           |         |       |        |     |       |
    |                                      |                  |         |                             |         |       |        |     |       |
    |   |   |   | Parcels                  |                  |         | /parcels                    |         |       |        |     |       | Pasikeitusių žemės sklypų sąrašas
    |   |   |   |   | text                 | string           |         | text()                      |         |       |        |     |       |
    |   |   |   |   | parcel[]             | backref          | Parcel1 |                             |         |       |        |     |       |
    |   |   |   |   | parcel1[]            | backref          | Parcel2 |                             |         |       |        |     |       |
"""

    path = tmp_path / 'manifest.xsd'
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    print(manifest)
    assert manifest == table


def test_xsd_attributes(rc: RawConfig, tmp_path: Path):
    xsd = """

<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">

<xs:element name="SALYGOS">
  <xs:complexType mixed="true">
    <xs:choice minOccurs="0" maxOccurs="unbounded">
      <xs:element ref="SALYGA" />
    </xs:choice>
  </xs:complexType>
</xs:element>

<xs:element name="SALYGA">
  <xs:complexType mixed="true">
    <xs:all>
      <xs:element name="REIKSME"     type="xs:string" />
      <xs:element name="PAVADINIMAS" type="xs:string" minOccurs="0" maxOccurs="1" />
      <xs:element name="APRASYMAS"   type="xs:string" minOccurs="0" maxOccurs="1" />
    </xs:all>
    <xs:attribute name="kodas"    type="xs:string"  use="optional" />
    <xs:attribute name="nr"       type="xs:integer" use="optional" />
  </xs:complexType>
</xs:element>

</xs:schema>
    """

    table = """
 id | d | r | b | m | property    | type            | ref    | source             | prepare | level | access | uri | title | description
    | manifest                    |                 |        |                    |         |       |        |     |       |
    |   | resource1               | xml             |        |                    |         |       |        |     |       |
    |                             |                 |        |                    |         |       |        |     |       |
    |   |   |   | Salyga          |                 |        | /SALYGOS/SALYGA    |         |       |        |     |       |
    |   |   |   |   | kodas       | string          |        | @kodas             |         |       |        |     |       |
    |   |   |   |   | nr          | integer         |        | @nr                |         |       |        |     |       |
    |   |   |   |   | text        | string          |        | text()             |         |       |        |     |       |
    |   |   |   |   | reiksme     | string required |        | REIKSME/text()     |         |       |        |     |       |
    |   |   |   |   | pavadinimas | string          |        | PAVADINIMAS/text() |         |       |        |     |       |
    |   |   |   |   | aprasymas   | string          |        | APRASYMAS/text()   |         |       |        |     |       |
    |                             |                 |        |                    |         |       |        |     |       |
    |   |   |   | Salygos         |                 |        | /SALYGOS           |         |       |        |     |       |
    |   |   |   |   | text        | string          |        | text()             |         |       |        |     |       |
    |   |   |   |   | salyga      | ref required    | Salyga |                    |         |       |        |     |       |
"""

    path = tmp_path / 'manifest.xsd'
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    print(manifest)
    assert manifest == table


def test_xsd_model_one_property(rc: RawConfig, tmp_path: Path):

    xsd = """
    <xs:schema xmlns="http://eTaarPlat.ServiceContracts/2007/08/Messages" xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified" targetNamespace="http://eTaarPlat.ServiceContracts/2007/08/Messages">

        <xs:complexType name="getTzByTRAResponse">
            <xs:sequence>
                <xs:element minOccurs="0" maxOccurs="1" name="searchParameters" type="SearchParametersTRA"/>
                <xs:element minOccurs="0" name="extracttz">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element minOccurs="0" name="extractPreparationTime" type="xs:dateTime" />
                            <xs:element minOccurs="0" name="phipoteka" type="xs:unsignedByte" />
                        </xs:sequence>
                    </xs:complexType>
                </xs:element>
                <xs:element minOccurs="0" maxOccurs="1" name="klaida">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element minOccurs="0" name="Aprasymas" type="xs:string" />
                        </xs:sequence>
                    </xs:complexType>
                </xs:element>  
            </xs:sequence>
        </xs:complexType>
        
        <xs:element name="getTzByTRAResponse" nillable="true" type="getTzByTRAResponse"/>

    </xs:schema>
    """

    table = """
 id | d | r | b | m | property                 | type     | ref | source                        | prepare | level | access | uri | title | description
    | manifest                                 |          |     |                               |         |       |        |     |       |
    |   | resource1                            | xml      |     |                               |         |       |        |     |       |
    |                                          |          |     |                               |         |       |        |     |       |
    |   |   |   | Extracttz                    |          |     | /getTzByTRAResponse/extracttz |         |       |        |     |       |
    |   |   |   |   | extract_preparation_time | datetime |     | extractPreparationTime/text() |         |       |        |     |       |
    |   |   |   |   | phipoteka                | integer  |     | phipoteka/text()              |         |       |        |     |       |
    |                                          |          |     |                               |         |       |        |     |       |
    |   |   |   | Klaida                       |          |     | /getTzByTRAResponse/klaida    |         |       |        |     |       |
    |   |   |   |   | aprasymas                | string   |     | Aprasymas/text()              |         |       |        |     |       |
    |                                          |          |     |                               |         |       |        |     |       |
    |   |   |   | GetTzByTRAResponse           |          |     | /getTzByTRAResponse           |         |       |        |     |       |
    |   |   |   |   | search_parameters        | string   |     | searchParameters/text()       |         |       |        |     |       |
"""
    path = tmp_path / 'manifest.xsd'
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
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_CTD_EMINYS_GAUTAS" />
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

<xs:element name="CT_CTD_EMINYS_GAUTAS" type="data_laikas">
  <xs:annotation><xs:documentation>Ėminio gavimo data</xs:documentation></xs:annotation>
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
 id | d | r | b | m | property                | type   | ref | source                         | prepare | level | access | uri                                           | title | description
    | manifest                                |        |     |                                |         |       |        |                                               |       |
    |   | resource1                           | xml    |     |                                |         |       |        |                                               |       |
    |                                         |        |     |                                |         |       |        |                                               |       |
    |   |   |   | Tyrimas                     |        |     | /TYRIMAS                       |         |       |        |                                               |       |
    |   |   |   |   | ct_e200ats_duom_sukurti | string |     | CT_E200ATS_DUOM_SUKURTI/text() |         |       |        |                                               |       | E200-ats duomenų sukūrimo data ir laikas
    |   |   |   |   | ct_paciento_spi         | string |     | CT_PACIENTO_SPI/text()         |         |       |        |                                               |       | Paciento prisirašymo įstaigos pavadinimas
    |   |   |   |   | ct_ctd_eminys_gautas    | string |     | CT_CTD_EMINYS_GAUTAS/text()    |         |       |        |                                               |       | Ėminio gavimo data

"""
    path = tmp_path / 'manifest.xsd'
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table


def test_xsd_sequence_choice_sequence(rc: RawConfig, tmp_path: Path):
    # choice in a sequence with a sequence inside
    xsd = """
<s:schema xmlns:s="http://www.w3.org/2001/XMLSchema" xmlns:tns="http://rc/ireg/1.0/" targetNamespace="http://rc/ireg/1.0/" elementFormDefault="qualified" attributeFormDefault="unqualified">
    <s:element name="data">
        <s:complexType>
            <s:sequence>
                <s:element minOccurs="0" maxOccurs="1" name="responseData" type="tns:summary" />
                <s:element minOccurs="0" maxOccurs="1" name="responseMessage" type="s:string" />
            </s:sequence>
        </s:complexType>
    </s:element>

    <s:complexType name="summary">

        <s:sequence>
            <s:element minOccurs="1" maxOccurs="1" name="statementId" />
            <s:element minOccurs="1" maxOccurs="1" name="title" />
            <s:element minOccurs="1" maxOccurs="1" name="documents" type="tns:person"></s:element>
        </s:sequence>

    </s:complexType>

    <s:complexType name="person">
        <s:sequence>
            <s:element minOccurs="0" maxOccurs="1" name="code" type="s:string" />
            <s:element minOccurs="0" maxOccurs="1" name="iltu_code" nillable="true" type="s:string" />
            <s:choice>
                <s:sequence>
                    <s:element minOccurs="0" maxOccurs="1" name="firstName" />
                    <s:element minOccurs="0" maxOccurs="1" name="lastName" />
                    <s:element minOccurs="0" maxOccurs="1" name="birthDate" />
                </s:sequence>
                <s:sequence>
                    <s:element minOccurs="0" maxOccurs="1" name="businessName" />
                </s:sequence>
            </s:choice>
        </s:sequence>
    </s:complexType>
</s:schema>
"""

    table = """
 id | d | r | b | m | property         | type            | ref | source                       | prepare | level | access | uri | title | description
    | manifest                         |                 |     |                              |         |       |        |     |       |
    |   | resource1                    | xml             |     |                              |         |       |        |     |       |
    |                                  |                 |     |                              |         |       |        |     |       |
    |   |   |   | Documents1           |                 |     | /data/responseData/documents |         |       |        |     |       |
    |   |   |   |   | birth_date       | string          |     | birthDate/text()             |         |       |        |     |       |
    |   |   |   |   | last_name        | string          |     | lastName/text()              |         |       |        |     |       |
    |   |   |   |   | first_name       | string          |     | firstName/text()             |         |       |        |     |       |
    |   |   |   |   | code             | string          |     | code/text()                  |         |       |        |     |       |
    |   |   |   |   | iltu_code        | string          |     | iltu_code/text()             |         |       |        |     |       |
    |                                  |                 |     |                              |         |       |        |     |       |
    |   |   |   | Documents2           |                 |     | /data/responseData/documents |         |       |        |     |       |
    |   |   |   |   | business_name    | string          |     | businessName/text()          |         |       |        |     |       |
    |   |   |   |   | code             | string          |     | code/text()                  |         |       |        |     |       |
    |   |   |   |   | iltu_code        | string          |     | iltu_code/text()             |         |       |        |     |       |
    |                                  |                 |     |                              |         |       |        |     |       |
    |   |   |   | ResponseData         |                 |     | /data/responseData           |         |       |        |     |       |
    |   |   |   |   | statement_id     | string required |     | statementId/text()           |         |       |        |     |       |
    |   |   |   |   | title            | string required |     | title/text()                 |         |       |        |     |       |
    |                                  |                 |     |                              |         |       |        |     |       |
    |   |   |   | Data                 |                 |     | /data                        |         |       |        |     |       |
    |   |   |   |   | response_message | string          |     | responseMessage/text()       |         |       |        |     |       |
"""
    path = tmp_path / 'manifest.xsd'
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
                        <xs:element minOccurs="0" maxOccurs="1" name="title2" type="xs:string"/>
                        <xs:element minOccurs="0" maxOccurs="1" name="printeddate" type="xs:string"/>
                        <xs:element minOccurs="0" maxOccurs="1" name="searchparameter1" type="xs:string"/>
                        <xs:element minOccurs="0" maxOccurs="1" name="searchparameter2" type="xs:string"/>
                        <xs:element minOccurs="0" maxOccurs="1" name="searchparameter3" type="xs:string"/>
                        <xs:element minOccurs="0" maxOccurs="1" name="statusas" type="xs:string"/>
                        <xs:element minOccurs="0" maxOccurs="1" name="DocList" type="ArrayOfBE_FULL_doclist"/>
                        <xs:element minOccurs="0" maxOccurs="1" name="PreorderStatusas" type="xs:string"/>
                        <xs:element minOccurs="0" maxOccurs="1" name="PreorderList" type="ArrayOfBE_FULL_preorder"/>
                        <xs:element minOccurs="0" maxOccurs="1" name="ContractStatusas" type="xs:string"/>
                        <xs:element minOccurs="0" maxOccurs="1" name="ContractList" type="ArrayOfBE_FULL_contract"/>
                    </xs:sequence>
                </xs:extension>
            </xs:complexContent>
        </xs:complexType>
        <xs:complexType name="BusinessEntityOfBE_FULL" abstract="true"/>
    </xs:schema>
"""

    table = """
 id | d | r | b | m | property          | type   | ref | source                  | prepare | level | access | uri | title | description
    | manifest                          |        |     |                         |         |       |        |     |       |
    |   | resource1                     | xml    |     |                         |         |       |        |     |       |
    |                                   |        |     |                         |         |       |        |     |       |
    |   |   |   | BeFull                |        |     | /BE_FULL                |         |       |        |     |       |
    |   |   |   |   | title1            | string |     | title1/text()           |         |       |        |     |       |
    |   |   |   |   | title2            | string |     | title2/text()           |         |       |        |     |       |
    |   |   |   |   | printeddate       | string |     | printeddate/text()      |         |       |        |     |       |
    |   |   |   |   | searchparameter1  | string |     | searchparameter1/text() |         |       |        |     |       |
    |   |   |   |   | searchparameter2  | string |     | searchparameter2/text() |         |       |        |     |       |
    |   |   |   |   | searchparameter3  | string |     | searchparameter3/text() |         |       |        |     |       |
    |   |   |   |   | statusas          | string |     | statusas/text()         |         |       |        |     |       |
    |   |   |   |   | doc_list          | string |     | DocList/text()          |         |       |        |     |       |
    |   |   |   |   | preorder_statusas | string |     | PreorderStatusas/text() |         |       |        |     |       |
    |   |   |   |   | preorder_list     | string |     | PreorderList/text()     |         |       |        |     |       |
    |   |   |   |   | contract_statusas | string |     | ContractStatusas/text() |         |       |        |     |       |
    |   |   |   |   | contract_list     | string |     | ContractList/text()     |         |       |        |     |       |

"""
    path = tmp_path / 'manifest.xsd'
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
 id | d | r | b | m | property         | type            | ref | source                            | prepare | level | access | uri | title | description
    | manifest                         |                 |     |                                   |         |       |        |     |       |
    |   | resource1                    | xml             |     |                                   |         |       |        |     |       |
    |                                  |                 |     |                                   |         |       |        |     |       |
    |   |   |   | Action               |                 |     | /data/responseData/actions/action |         |       |        |     |       |
    |   |   |   |   | code             | string required |     | code/text()                       |         |       |        |     |       | Paslaugos kodas (RC kodas)
    |                                  |                 |     |                                   |         |       |        |     |       |
    |   |   |   | Data                 |                 |     | /data                             |         |       |        |     |       |
    |   |   |   |   | response_message | string          |     | responseMessage/text()            |         |       |        |     |       |

"""
    path = tmp_path / 'manifest.xsd'
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
                        <s:annotation>
                            <s:documentation>Įgaliojimą gali sudaryti.</s:documentation>
                        </s:annotation>
                        <s:restriction base="s:string">
                            <s:enumeration value="fiz"/>
                            <s:enumeration value="fiz-notarial"/>
                            <s:enumeration value="jur"/>
                            <s:enumeration value="jur-notarial"/>
                            <s:enumeration value="fiz-jur"/>
                            <s:enumeration value="fiz-notarial-jur-notarial"/>
                            <s:enumeration value="fiz-notarial-jur"/>
                            <s:enumeration value="fiz-jur-notarial"/>
                        </s:restriction>
                    </s:simpleType>
                </s:element>
    
                <s:element name="default_description_editable" minOccurs="1" maxOccurs="1">
                    <s:simpleType>
                        <s:annotation>
                            <s:documentation>Ar numatytasis aprašymas gali būti redaguojamas? 0 - NE, 1 - TAIP</s:documentation>
                        </s:annotation>
                        <s:restriction base="s:string">
                            <s:enumeration value="0"/>
                            <s:enumeration value="1"/>
                        </s:restriction>
                    </s:simpleType>
                </s:element>
    
                <s:element name="digital_service" minOccurs="1" maxOccurs="1">
                    <s:simpleType>
                        <s:annotation>
                            <s:documentation>El. paslauga. Reikšmės: digital - Tik elektroninė paslauga, analog - Tik neelektroninė paslauga, digital-or-analog - Elektroninė arba neelektroninė paslauga</s:documentation>
                        </s:annotation>
                        <s:restriction base="s:string">
                            <s:enumeration value="digital"/>
                            <s:enumeration value="analog"/>
                            <s:enumeration value="digital-or-analog"/>
                        </s:restriction>
                    </s:simpleType>
                </s:element>
                
            </s:sequence>
        </s:complexType>
    </s:schema>
"""

    table = """
 id | d | r | b | m | property                     | type            | ref | source                              | prepare | level | access | uri | title | description
    | manifest                                     |                 |     |                                     |         |       |        |     |       |
    |   | resource1                                | xml             |     |                                     |         |       |        |     |       |
    |                                              |                 |     |                                     |         |       |        |     |       |
    |   |   |   | ResponseData                     |                 |     | /data/responseData                  |         |       |        |     |       |
    |   |   |   |   | who_may_consitute            | string required |     | who_may_consitute/text()            |         |       |        |     |       | Įgaliojimą gali sudaryti.
    |                                              | enum            |     | fiz                                 |         |       |        |     |       |
    |                                              |                 |     | fiz-notarial                        |         |       |        |     |       |
    |                                              |                 |     | jur                                 |         |       |        |     |       |
    |                                              |                 |     | jur-notarial                        |         |       |        |     |       |
    |                                              |                 |     | fiz-jur                             |         |       |        |     |       |
    |                                              |                 |     | fiz-notarial-jur-notarial           |         |       |        |     |       |
    |                                              |                 |     | fiz-notarial-jur                    |         |       |        |     |       |
    |                                              |                 |     | fiz-jur-notarial                    |         |       |        |     |       |
    |   |   |   |   | default_description_editable | string required |     | default_description_editable/text() |         |       |        |     |       | Ar numatytasis aprašymas gali būti redaguojamas? 0 - NE, 1 - TAIP
    |                                              | enum            |     | 0                                   |         |       |        |     |       |
    |                                              |                 |     | 1                                   |         |       |        |     |       |
    |   |   |   |   | digital_service              | string required |     | digital_service/text()              |         |       |        |     |       | El. paslauga. Reikšmės: digital - Tik elektroninė paslauga, analog - Tik neelektroninė paslauga, digital-or-analog - Elektroninė arba neelektroninė paslauga
    |                                              | enum            |     | digital                             |         |       |        |     |       |
    |                                              |                 |     | analog                              |         |       |        |     |       |
    |                                              |                 |     | digital-or-analog                   |         |       |        |     |       |
    |                                              |                 |     |                                     |         |       |        |     |       |
    |   |   |   | Data                             |                 |     | /data                               |         |       |        |     |       |
    |   |   |   |   | response_message             | string          |     | responseMessage/text()              |         |       |        |     |       |

"""
    path = tmp_path / 'manifest.xsd'
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table
