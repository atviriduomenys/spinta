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
 id | d | r | b | m | property        | type             | ref | source                            | prepare | level | access | uri | title | description
    | manifest                        |                  |     |                                   |         |       |        |     |       |
    |   | resource1                   | xml              |     |                                   |         |       |        |     |       |
    |                                 |                  |     |                                   |         |       |        |     |       |
    |   |   |   | Administracinis     |                  |     | /ADMINISTRACINIAI/ADMINISTRACINIS |         |       |        |     |       |
    |   |   |   |   | adm_id          | integer required |     | ADM_ID/text()                     |         |       |        |     |       |
    |   |   |   |   | adm_iki         | date required    |     | ADM_IKI/text()                    |         |       |        |     |       |
    |   |   |   |   | adm_kodas       | integer required |     | ADM_KODAS/text()                  |         |       |        |     |       |
    |   |   |   |   | adm_nuo         | date required    |     | ADM_NUO/text()                    |         |       |        |     |       |
    |   |   |   |   | gyv_kodas       | integer required |     | GYV_KODAS/text()                  |         |       |        |     |       |
    |   |   |   |   | iki             | date required    |     | IKI/text()                        |         |       |        |     |       |
    |   |   |   |   | nuo             | date required    |     | NUO/text()                        |         |       |        |     |       |
    |   |   |   |   | priklauso_kodas | integer required |     | PRIKLAUSO_KODAS/text()            |         |       |        |     |       |
    |   |   |   |   | tipas           | string required  |     | TIPAS/text()                      |         |       |        |     |       |
    |   |   |   |   | tipo_santrumpa  | string required  |     | TIPO_SANTRUMPA/text()             |         |       |        |     |       |
    |   |   |   |   | vardas_k        | string required  |     | VARDAS_K/text()                   |         |       |        |     |       |
    |   |   |   |   | vardas_k_lot    | string required  |     | VARDAS_K_LOT/text()               |         |       |        |     |       |
    |                                 |                  |     |                                   |         |       |        |     |       |
    |   |   |   | Adresas             |                  |     | /ADRESAI/ADRESAS                  |         |       |        |     |       |
    |   |   |   |   | aob_id          | integer required |     | AOB_ID/text()                     |         |       |        |     |       |
    |   |   |   |   | aob_iki         | date required    |     | AOB_IKI/text()                    |         |       |        |     |       |
    |   |   |   |   | aob_kodas       | integer required |     | AOB_KODAS/text()                  |         |       |        |     |       |
    |   |   |   |   | aob_nuo         | date required    |     | AOB_NUO/text()                    |         |       |        |     |       |
    |   |   |   |   | gat_kodas       | integer required |     | GAT_KODAS/text()                  |         |       |        |     |       |
    |   |   |   |   | gyv_kodas       | integer required |     | GYV_KODAS/text()                  |         |       |        |     |       |
    |   |   |   |   | iki             | date required    |     | IKI/text()                        |         |       |        |     |       |
    |   |   |   |   | korpuso_nr      | string required  |     | KORPUSO_NR/text()                 |         |       |        |     |       |
    |   |   |   |   | nr              | string required  |     | NR/text()                         |         |       |        |     |       |
    |   |   |   |   | nuo             | date required    |     | NUO/text()                        |         |       |        |     |       |
    |   |   |   |   | pasto_kodas     | string required  |     | PASTO_KODAS/text()                |         |       |        |     |       |
    |                                 |                  |     |                                   |         |       |        |     |       |
    |   |   |   | Gatve               |                  |     | /GATVES/GATVE                     |         |       |        |     |       |
    |   |   |   |   | gat_id          | integer required |     | GAT_ID/text()                     |         |       |        |     |       |
    |   |   |   |   | gat_iki         | date required    |     | GAT_IKI/text()                    |         |       |        |     |       |
    |   |   |   |   | gat_kodas       | integer required |     | GAT_KODAS/text()                  |         |       |        |     |       |
    |   |   |   |   | gat_nuo         | date required    |     | GAT_NUO/text()                    |         |       |        |     |       |
    |   |   |   |   | gyv_kodas       | integer required |     | GYV_KODAS/text()                  |         |       |        |     |       |
    |   |   |   |   | iki             | date required    |     | IKI/text()                        |         |       |        |     |       |
    |   |   |   |   | nuo             | date required    |     | NUO/text()                        |         |       |        |     |       |
    |   |   |   |   | tipas           | string required  |     | TIPAS/text()                      |         |       |        |     |       |
    |   |   |   |   | tipo_santrumpa  | string required  |     | TIPO_SANTRUMPA/text()             |         |       |        |     |       |
    |   |   |   |   | vardas_k        | string required  |     | VARDAS_K/text()                   |         |       |        |     |       |
    |   |   |   |   | vardas_k_lot    | string required  |     | VARDAS_K_LOT/text()               |         |       |        |     |       |
    |                                 |                  |     |                                   |         |       |        |     |       |
    |   |   |   | Gyvenviete          |                  |     | /GYVENVIETES/GYVENVIETE           |         |       |        |     |       |
    |   |   |   |   | adm_kodas       | integer required |     | ADM_KODAS/text()                  |         |       |        |     |       |
    |   |   |   |   | gyv_id          | integer required |     | GYV_ID/text()                     |         |       |        |     |       |
    |   |   |   |   | gyv_iki         | date required    |     | GYV_IKI/text()                    |         |       |        |     |       |
    |   |   |   |   | gyv_kodas       | integer required |     | GYV_KODAS/text()                  |         |       |        |     |       |
    |   |   |   |   | gyv_nuo         | date required    |     | GYV_NUO/text()                    |         |       |        |     |       |
    |   |   |   |   | iki             | date required    |     | IKI/text()                        |         |       |        |     |       |
    |   |   |   |   | nuo             | date required    |     | NUO/text()                        |         |       |        |     |       |
    |   |   |   |   | tipas           | string required  |     | TIPAS/text()                      |         |       |        |     |       |
    |   |   |   |   | tipo_santrumpa  | string required  |     | TIPO_SANTRUMPA/text()             |         |       |        |     |       |
    |   |   |   |   | vardas_k        | string required  |     | VARDAS_K/text()                   |         |       |        |     |       |
    |   |   |   |   | vardas_k_lot    | string required  |     | VARDAS_K_LOT/text()               |         |       |        |     |       |
    |   |   |   |   | vardas_v        | string required  |     | VARDAS_V/text()                   |         |       |        |     |       |
    |   |   |   |   | vardas_v_lot    | string required  |     | VARDAS_V_LOT/text()               |         |       |        |     |       |
    |                                 |                  |     |                                   |         |       |        |     |       |
    |   |   |   | Kodas               |                  |     | /KODAI/KODAS                      |         |       |        |     |       |
    |   |   |   |   | iki             | date required    |     | IKI/text()                        |         |       |        |     |       |
    |   |   |   |   | nuo             | date required    |     | NUO/text()                        |         |       |        |     |       |
    |   |   |   |   | pasto_kodas     | string required  |     | PASTO_KODAS/text()                |         |       |        |     |       |
    |   |   |   |   | pasto_viet_pav  | string required  |     | PASTO_VIET_PAV/text()             |         |       |        |     |       |
    |                                 |                  |     |                                   |         |       |        |     |       |
    |   |   |   | Patalpa             |                  |     | /PATALPOS/PATALPA                 |         |       |        |     |       |
    |   |   |   |   | aob_kodas       | integer required |     | AOB_KODAS/text()                  |         |       |        |     |       |
    |   |   |   |   | iki             | date required    |     | IKI/text()                        |         |       |        |     |       |
    |   |   |   |   | nuo             | date required    |     | NUO/text()                        |         |       |        |     |       |
    |   |   |   |   | pat_id          | integer required |     | PAT_ID/text()                     |         |       |        |     |       |
    |   |   |   |   | pat_iki         | date required    |     | PAT_IKI/text()                    |         |       |        |     |       |
    |   |   |   |   | pat_kodas       | integer required |     | PAT_KODAS/text()                  |         |       |        |     |       |
    |   |   |   |   | pat_nuo         | date required    |     | PAT_NUO/text()                    |         |       |        |     |       |
    |   |   |   |   | patalpos_nr     | string required  |     | PATALPOS_NR/text()                |         |       |        |     |       |
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
      <xs:element ref="asmenys" minOccurs="0" maxOccurs="1" />
    </xs:sequence>
  </xs:complexType>
</xs:element>


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
 id | d | r | b | m | property                  | type             | ref                     | source                                   | prepare | level | access | uri | title | description
    | manifest                                  |                  |                         |                                          |         |       |        |     |       |
    |   | resource1                             | xml              |                         |                                          |         |       |        |     |       |
    |                                           |                  |                         |                                          |         |       |        |     |       |
    |   |   |   | Asmenys                       |                  |                         | /klientu_saraso_rezultatas/asmenys       |         |       |        |     |       |
    |   |   |   |   | asmuo[]                   | backref          | Asmuo                   | asmuo                                    |         |       |        |     |       |
    |   |   |   |   | puslapis                  | integer required |                         | @puslapis                                |         |       |        |     |       | rezultatu puslapio numeris
    |   |   |   |   | text                      | string           |                         | text()                                   |         |       |        |     |       |
    |                                           |                  |                         |                                          |         |       |        |     |       |
    |   |   |   | Asmuo                         |                  |                         | /klientu_saraso_rezultatas/asmenys/asmuo |         |       |        |     |       |
    |   |   |   |   | ak                        | string required  |                         | @ak                                      |         |       |        |     |       |
    |   |   |   |   | asmenys                   | ref              | Asmenys                 |                                          |         |       |        |     |       |
    |   |   |   |   | id                        | string required  |                         | @id                                      |         |       |        |     |       |
    |   |   |   |   | klientu_saraso_rezultatas | ref              | KlientuSarasoRezultatas |                                          |         |       |        |     |       |
    |   |   |   |   | text                      | string           |                         | text()                                   |         |       |        |     |       |
    |                                           |                  |                         |                                          |         |       |        |     |       |
    |   |   |   | KlientuSarasoRezultatas       |                  |                         | /klientu_saraso_rezultatas               |         |       |        |     |       |
    |   |   |   |   | asmenys                   | ref              | Asmenys                 | asmenys                                  |         |       |        |     |       |
    |   |   |   |   | asmenys.asmuo[]           | backref          | Asmuo                   | asmenys/asmuo                            |         |       |        |     |       |
    |   |   |   |   | asmenys.asmuo[].ak        | string required  |                         | @ak                                      |         |       |        |     |       |
    |   |   |   |   | asmenys.asmuo[].id        | string required  |                         | @id                                      |         |       |        |     |       |
    |   |   |   |   | asmenys.asmuo[].text      | string           |                         | text()                                   |         |       |        |     |       |
    |   |   |   |   | asmenys.puslapis          | integer required |                         | asmenys/@puslapis                        |         |       |        |     |       | rezultatu puslapio numeris
    |   |   |   |   | asmenys.text              | string           |                         | asmenys/text()                           |         |       |        |     |       |
    |   |   |   |   | text                      | string           |                         | text()                                   |         |       |        |     |       |
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
 id | d | r | b | m | property                | type             | ref     | source                             | prepare | level | access | uri                                           | title | description
    | manifest                                |                  |         |                                    |         |       |        |                                               |       |
    |   | resource1                           | xml              |         |                                    |         |       |        |                                               |       |
    |                                         |                  |         |                                    |         |       |        |                                               |       |
    |   |   |   | Asmenys                     |                  |         | /klientu_saraso_rezultatas/asmenys |         |       |        |                                               |       |
    |   |   |   |   | puslapis                | integer required |         | @puslapis                          |         |       |        |                                               |       | rezultatu puslapio numeris
    |   |   |   |   | text                    | string           |         | text()                             |         |       |        |                                               |       |
    |                                         |                  |         |                                    |         |       |        |                                               |       |
    |   |   |   | KlientuSarasoRezultatas     |                  |         | /klientu_saraso_rezultatas         |         |       |        |                                               |       |
    |   |   |   |   | asmenys                 | ref              | Asmenys | asmenys                            |         |       |        |                                               |       |
    |   |   |   |   | asmenys.puslapis        | integer required |         | asmenys/@puslapis                  |         |       |        |                                               |       | rezultatu puslapio numeris
    |   |   |   |   | asmenys.text            | string           |         | asmenys/text()                     |         |       |        |                                               |       |
    |   |   |   |   | text                    | string           |         | text()                             |         |       |        |                                               |       |
    |                                         |                  |         |                                    |         |       |        |                                               |       |
    |   |   |   | Resource                    |                  |         | /                                  |         |       |        | http://www.w3.org/2000/01/rdf-schema#Resource |       | Įvairūs duomenys
    |   |   |   |   | klaida                  | string           |         | klaida/text()                      |         |       |        |                                               |       | Klaidos atveju - klaidos pranešimas
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
 id | d | r | b | m | property                      | type             | ref     | source                      | prepare | level | access | uri | title | description
    | manifest                                      |                  |         |                             |         |       |        |     |       |
    |   | resource1                                 | xml              |         |                             |         |       |        |     |       |
    |                                               |                  |         |                             |         |       |        |     |       |
    |   |   |   | Parcel1                           |                  |         | /parcels/parcel             |         |       |        |     |       | Žemės sklypo pasikeitimo informacija
    |   |   |   |   | parcel_unique_number          | integer required |         | parcel_unique_number/text() |         |       |        |     |       | Žemės sklypo unikalus numeris
    |   |   |   |   | parcels                       | ref              | Parcels |                             |         |       |        |     |       |
    |   |   |   |   | text                          | string           |         | text()                      |         |       |        |     |       |
    |                                               |                  |         |                             |         |       |        |     |       |
    |   |   |   | Parcel2                           |                  |         | /parcels/parcel             |         |       |        |     |       | Žemės sklypo pasikeitimo informacija
    |   |   |   |   | parcels                       | ref              | Parcels |                             |         |       |        |     |       |
    |   |   |   |   | sign_of_change                | integer required |         | sign_of_change/text()       |         |       |        |     |       | Žemės sklypo pasikeitimo požymis
    |                                               | enum             |         | 1                           |         |       |        |     |       |
    |                                               |                  |         | 2                           |         |       |        |     |       |
    |                                               |                  |         | 3                           |         |       |        |     |       |
    |   |   |   |   | text                          | string           |         | text()                      |         |       |        |     |       |
    |                                               |                  |         |                             |         |       |        |     |       |
    |   |   |   | Parcels                           |                  |         | /parcels                    |         |       |        |     |       | Pasikeitusių žemės sklypų sąrašas
    |   |   |   |   | parcel1[]                     | backref          | Parcel2 | parcel                      |         |       |        |     |       | Žemės sklypo pasikeitimo informacija
    |   |   |   |   | parcel1[].sign_of_change      | integer required |         | sign_of_change/text()       |         |       |        |     |       | Žemės sklypo pasikeitimo požymis
    |                                               | enum             |         | 1                           |         |       |        |     |       |
    |                                               |                  |         | 2                           |         |       |        |     |       |
    |                                               |                  |         | 3                           |         |       |        |     |       |
    |   |   |   |   | parcel1[].text                | string           |         | text()                      |         |       |        |     |       |
    |   |   |   |   | parcel[]                      | backref          | Parcel1 | parcel                      |         |       |        |     |       | Žemės sklypo pasikeitimo informacija
    |   |   |   |   | parcel[].parcel_unique_number | integer required |         | parcel_unique_number/text() |         |       |        |     |       | Žemės sklypo unikalus numeris
    |   |   |   |   | parcel[].text                 | string           |         | text()                      |         |       |        |     |       |
    |   |   |   |   | text                          | string           |         | text()                      |         |       |        |     |       |

"""

    path = tmp_path / 'manifest.xsd'
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    print(manifest)
    assert manifest == table


def test_xsd_choice_max_occurs_unbounded(rc: RawConfig, tmp_path: Path):
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
 id | d | r | b | m | property                      | type    | ref     | source                      | prepare | level | access | uri | title | description
    | manifest                                      |         |         |                             |         |       |        |     |       |
    |   | resource1                                 | xml     |         |                             |         |       |        |     |       |
    |                                               |         |         |                             |         |       |        |     |       |
    |   |   |   | Parcel                            |         |         | /parcels/parcel             |         |       |        |     |       | Žemės sklypo pasikeitimo informacija
    |   |   |   |   | parcel_unique_number          | integer |         | parcel_unique_number/text() |         |       |        |     |       | Žemės sklypo unikalus numeris
    |   |   |   |   | parcels                       | ref     | Parcels |                             |         |       |        |     |       |
    |   |   |   |   | sign_of_change                | integer |         | sign_of_change/text()       |         |       |        |     |       | Žemės sklypo pasikeitimo požymis
    |                                               | enum    |         | 1                           |         |       |        |     |       |
    |                                               |         |         | 2                           |         |       |        |     |       |
    |                                               |         |         | 3                           |         |       |        |     |       |
    |   |   |   |   | text                          | string  |         | text()                      |         |       |        |     |       |
    |                                               |         |         |                             |         |       |        |     |       |
    |   |   |   | Parcels                           |         |         | /parcels                    |         |       |        |     |       | Pasikeitusių žemės sklypų sąrašas
    |   |   |   |   | parcel[]                      | backref | Parcel  | parcel                      |         |       |        |     |       | Žemės sklypo pasikeitimo informacija
    |   |   |   |   | parcel[].parcel_unique_number | integer |         | parcel_unique_number/text() |         |       |        |     |       | Žemės sklypo unikalus numeris
    |   |   |   |   | parcel[].sign_of_change       | integer |         | sign_of_change/text()       |         |       |        |     |       | Žemės sklypo pasikeitimo požymis
    |                                               | enum    |         | 1                           |         |       |        |     |       |
    |                                               |         |         | 2                           |         |       |        |     |       |
    |                                               |         |         | 3                           |         |       |        |     |       |
    |   |   |   |   | parcel[].text                 | string  |         | text()                      |         |       |        |     |       |
    |   |   |   |   | text                          | string  |         | text()                      |         |       |        |     |       |
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
 id | d | r | b | m | property             | type            | ref     | source             | prepare | level | access | uri | title | description
    | manifest                             |                 |         |                    |         |       |        |     |       |
    |   | resource1                        | xml             |         |                    |         |       |        |     |       |
    |                                      |                 |         |                    |         |       |        |     |       |
    |   |   |   | Salyga                   |                 |         | /SALYGOS/SALYGA    |         |       |        |     |       |
    |   |   |   |   | aprasymas            | string          |         | APRASYMAS/text()   |         |       |        |     |       |
    |   |   |   |   | kodas                | string          |         | @kodas             |         |       |        |     |       |
    |   |   |   |   | nr                   | integer         |         | @nr                |         |       |        |     |       |
    |   |   |   |   | pavadinimas          | string          |         | PAVADINIMAS/text() |         |       |        |     |       |
    |   |   |   |   | reiksme              | string required |         | REIKSME/text()     |         |       |        |     |       |
    |   |   |   |   | salygos              | ref             | Salygos |                    |         |       |        |     |       |
    |   |   |   |   | text                 | string          |         | text()             |         |       |        |     |       |
    |                                      |                 |         |                    |         |       |        |     |       |
    |   |   |   | Salygos                  |                 |         | /SALYGOS           |         |       |        |     |       |
    |   |   |   |   | salyga[]             | backref         | Salyga  | SALYGA             |         |       |        |     |       |
    |   |   |   |   | salyga[].aprasymas   | string          |         | APRASYMAS/text()   |         |       |        |     |       |
    |   |   |   |   | salyga[].kodas       | string          |         | @kodas             |         |       |        |     |       |
    |   |   |   |   | salyga[].nr          | integer         |         | @nr                |         |       |        |     |       |
    |   |   |   |   | salyga[].pavadinimas | string          |         | PAVADINIMAS/text() |         |       |        |     |       |
    |   |   |   |   | salyga[].reiksme     | string required |         | REIKSME/text()     |         |       |        |     |       |
    |   |   |   |   | salyga[].text        | string          |         | text()             |         |       |        |     |       |
    |   |   |   |   | text                 | string          |         | text()             |         |       |        |     |       |

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
 id | d | r | b | m | property                           | type     | ref       | source                                  | prepare | level | access | uri | title | description
    | manifest                                           |          |           |                                         |         |       |        |     |       |
    |   | resource1                                      | xml      |           |                                         |         |       |        |     |       |
    |                                                    |          |           |                                         |         |       |        |     |       |
    |   |   |   | Extracttz                              |          |           | /getTzByTRAResponse/extracttz           |         |       |        |     |       |
    |   |   |   |   | extract_preparation_time           | datetime |           | extractPreparationTime/text()           |         |       |        |     |       |
    |   |   |   |   | phipoteka                          | integer  |           | phipoteka/text()                        |         |       |        |     |       |
    |                                                    |          |           |                                         |         |       |        |     |       |
    |   |   |   | GetTzByTRAResponse                     |          |           | /getTzByTRAResponse                     |         |       |        |     |       |
    |   |   |   |   | aprasymas                          | string   |           | klaida/Aprasymas/text()                 |         |       |        |     |       |
    |   |   |   |   | extracttz                          | ref      | Extracttz | extracttz                               |         |       |        |     |       |
    |   |   |   |   | extracttz.extract_preparation_time | datetime |           | extracttz/extractPreparationTime/text() |         |       |        |     |       |
    |   |   |   |   | extracttz.phipoteka                | integer  |           | extracttz/phipoteka/text()              |         |       |        |     |       |
    |   |   |   |   | search_parameters                  | string   |           | searchParameters/text()                 |         |       |        |     |       |

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
 id | d | r | b | m | property                | type   | ref | source                         | prepare | level | access | uri | title | description
    | manifest                                |        |     |                                |         |       |        |     |       |
    |   | resource1                           | xml    |     |                                |         |       |        |     |       |
    |                                         |        |     |                                |         |       |        |     |       |
    |   |   |   | Tyrimas                     |        |     | /TYRIMAS                       |         |       |        |     |       |
    |   |   |   |   | ct_ctd_eminys_gautas    | string |     | CT_CTD_EMINYS_GAUTAS/text()    |         |       |        |     |       | Ėminio gavimo data
    |   |   |   |   | ct_e200ats_duom_sukurti | string |     | CT_E200ATS_DUOM_SUKURTI/text() |         |       |        |     |       | E200-ats duomenų sukūrimo data ir laikas
    |   |   |   |   | ct_paciento_spi         | string |     | CT_PACIENTO_SPI/text()         |         |       |        |     |       | Paciento prisirašymo įstaigos pavadinimas
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
 id | d | r | b | m | property                               | type            | ref          | source                                     | prepare | level | access | uri | title | description
    | manifest                                               |                 |              |                                            |         |       |        |     |       |
    |   | resource1                                          | xml             |              |                                            |         |       |        |     |       |
    |                                                        |                 |              |                                            |         |       |        |     |       |
    |   |   |   | Data                                       |                 |              | /data                                      |         |       |        |     |       |
    |   |   |   |   | response_data                          | ref             | ResponseData | responseData                               |         |       |        |     |       |
    |   |   |   |   | response_data.documents                | ref required    | Documents1   | responseData/documents                     |         |       |        |     |       |
    |   |   |   |   | response_data.documents.birth_date     | string          |              | responseData/documents/birthDate/text()    |         |       |        |     |       |
    |   |   |   |   | response_data.documents.code           | string          |              | responseData/documents/code/text()         |         |       |        |     |       |
    |   |   |   |   | response_data.documents.first_name     | string          |              | responseData/documents/firstName/text()    |         |       |        |     |       |
    |   |   |   |   | response_data.documents.iltu_code      | string          |              | responseData/documents/iltu_code/text()    |         |       |        |     |       |
    |   |   |   |   | response_data.documents.last_name      | string          |              | responseData/documents/lastName/text()     |         |       |        |     |       |
    |   |   |   |   | response_data.documents1               | ref required    | Documents2   | responseData/documents                     |         |       |        |     |       |
    |   |   |   |   | response_data.documents1.business_name | string          |              | responseData/documents/businessName/text() |         |       |        |     |       |
    |   |   |   |   | response_data.documents1.code          | string          |              | responseData/documents/code/text()         |         |       |        |     |       |
    |   |   |   |   | response_data.documents1.iltu_code     | string          |              | responseData/documents/iltu_code/text()    |         |       |        |     |       |
    |   |   |   |   | response_data.statement_id             | string required |              | responseData/statementId/text()            |         |       |        |     |       |
    |   |   |   |   | response_data.title                    | string required |              | responseData/title/text()                  |         |       |        |     |       |
    |   |   |   |   | response_message                       | string          |              | responseMessage/text()                     |         |       |        |     |       |
    |                                                        |                 |              |                                            |         |       |        |     |       |
    |   |   |   | Documents1                                 |                 |              | /data/responseData/documents               |         |       |        |     |       |
    |   |   |   |   | birth_date                             | string          |              | birthDate/text()                           |         |       |        |     |       |
    |   |   |   |   | code                                   | string          |              | code/text()                                |         |       |        |     |       |
    |   |   |   |   | first_name                             | string          |              | firstName/text()                           |         |       |        |     |       |
    |   |   |   |   | iltu_code                              | string          |              | iltu_code/text()                           |         |       |        |     |       |
    |   |   |   |   | last_name                              | string          |              | lastName/text()                            |         |       |        |     |       |
    |                                                        |                 |              |                                            |         |       |        |     |       |
    |   |   |   | Documents2                                 |                 |              | /data/responseData/documents               |         |       |        |     |       |
    |   |   |   |   | business_name                          | string          |              | businessName/text()                        |         |       |        |     |       |
    |   |   |   |   | code                                   | string          |              | code/text()                                |         |       |        |     |       |
    |   |   |   |   | iltu_code                              | string          |              | iltu_code/text()                           |         |       |        |     |       |
    |                                                        |                 |              |                                            |         |       |        |     |       |
    |   |   |   | ResponseData                               |                 |              | /data/responseData                         |         |       |        |     |       |
    |   |   |   |   | documents                              | ref required    | Documents1   | documents                                  |         |       |        |     |       |
    |   |   |   |   | documents1                             | ref required    | Documents2   | documents                                  |         |       |        |     |       |
    |   |   |   |   | statement_id                           | string required |              | statementId/text()                         |         |       |        |     |       |
    |   |   |   |   | title                                  | string required |              | title/text()                               |         |       |        |     |       |

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
    |   |   |   |   | contract_list     | string |     | ContractList/text()     |         |       |        |     |       |
    |   |   |   |   | contract_statusas | string |     | ContractStatusas/text() |         |       |        |     |       |
    |   |   |   |   | doc_list          | string |     | DocList/text()          |         |       |        |     |       |
    |   |   |   |   | preorder_list     | string |     | PreorderList/text()     |         |       |        |     |       |
    |   |   |   |   | preorder_statusas | string |     | PreorderStatusas/text() |         |       |        |     |       |
    |   |   |   |   | printeddate       | string |     | printeddate/text()      |         |       |        |     |       |
    |   |   |   |   | searchparameter1  | string |     | searchparameter1/text() |         |       |        |     |       |
    |   |   |   |   | searchparameter2  | string |     | searchparameter2/text() |         |       |        |     |       |
    |   |   |   |   | searchparameter3  | string |     | searchparameter3/text() |         |       |        |     |       |
    |   |   |   |   | statusas          | string |     | statusas/text()         |         |       |        |     |       |
    |   |   |   |   | title1            | string |     | title1/text()           |         |       |        |     |       |
    |   |   |   |   | title2            | string |     | title2/text()           |         |       |        |     |       |
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
 id | d | r | b | m | property               | type            | ref     | source                     | prepare | level | access | uri | title | description
    | manifest                               |                 |         |                            |         |       |        |     |       |
    |   | resource1                          | xml             |         |                            |         |       |        |     |       |
    |                                        |                 |         |                            |         |       |        |     |       |
    |   |   |   | Actions                    |                 |         | /data/responseData/actions |         |       |        |     |       |
    |   |   |   |   | code[]                 | string required |         | action/code/text()         |         |       |        |     |       | Paslaugos kodas (RC kodas)
    |   |   |   |   | data                   | ref             | Data    |                            |         |       |        |     |       |
    |                                        |                 |         |                            |         |       |        |     |       |
    |   |   |   | Data                       |                 |         | /data                      |         |       |        |     |       |
    |   |   |   |   | response_data[]        | backref         | Actions | responseData/actions       |         |       |        |     |       |
    |   |   |   |   | response_data[].code[] | string required |         | action/code/text()         |         |       |        |     |       | Paslaugos kodas (RC kodas)
    |   |   |   |   | response_message       | string          |         | responseMessage/text()     |         |       |        |     |       |

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
 id | d | r | b | m | property                                   | type            | ref          | source                                           | prepare | level | access | uri | title | description
    | manifest                                                   |                 |              |                                                  |         |       |        |     |       |
    |   | resource1                                              | xml             |              |                                                  |         |       |        |     |       |
    |                                                            |                 |              |                                                  |         |       |        |     |       |
    |   |   |   | Data                                           |                 |              | /data                                            |         |       |        |     |       |
    |   |   |   |   | response_data                              | ref             | ResponseData | responseData                                     |         |       |        |     |       |
    |   |   |   |   | response_data.default_description_editable | string required |              | responseData/default_description_editable/text() |         |       |        |     |       | Ar numatytasis aprašymas gali būti redaguojamas? 0 - NE, 1 - TAIP
    |                                                            | enum            |              | 0                                                |         |       |        |     |       |
    |                                                            |                 |              | 1                                                |         |       |        |     |       |
    |   |   |   |   | response_data.digital_service              | string required |              | responseData/digital_service/text()              |         |       |        |     |       | El. paslauga. Reikšmės: digital - Tik elektroninė paslauga, analog - Tik neelektroninė paslauga, digital-or-analog - Elektroninė arba neelektroninė paslauga
    |                                                            | enum            |              | digital                                          |         |       |        |     |       |
    |                                                            |                 |              | analog                                           |         |       |        |     |       |
    |                                                            |                 |              | digital-or-analog                                |         |       |        |     |       |
    |   |   |   |   | response_data.who_may_consitute            | string required |              | responseData/who_may_consitute/text()            |         |       |        |     |       | Įgaliojimą gali sudaryti.
    |                                                            | enum            |              | fiz                                              |         |       |        |     |       |
    |                                                            |                 |              | fiz-notarial                                     |         |       |        |     |       |
    |                                                            |                 |              | jur                                              |         |       |        |     |       |
    |                                                            |                 |              | jur-notarial                                     |         |       |        |     |       |
    |                                                            |                 |              | fiz-jur                                          |         |       |        |     |       |
    |                                                            |                 |              | fiz-notarial-jur-notarial                        |         |       |        |     |       |
    |                                                            |                 |              | fiz-notarial-jur                                 |         |       |        |     |       |
    |                                                            |                 |              | fiz-jur-notarial                                 |         |       |        |     |       |
    |   |   |   |   | response_message                           | string          |              | responseMessage/text()                           |         |       |        |     |       |
    |                                                            |                 |              |                                                  |         |       |        |     |       |
    |   |   |   | ResponseData                                   |                 |              | /data/responseData                               |         |       |        |     |       |
    |   |   |   |   | default_description_editable               | string required |              | default_description_editable/text()              |         |       |        |     |       | Ar numatytasis aprašymas gali būti redaguojamas? 0 - NE, 1 - TAIP
    |                                                            | enum            |              | 0                                                |         |       |        |     |       |
    |                                                            |                 |              | 1                                                |         |       |        |     |       |
    |   |   |   |   | digital_service                            | string required |              | digital_service/text()                           |         |       |        |     |       | El. paslauga. Reikšmės: digital - Tik elektroninė paslauga, analog - Tik neelektroninė paslauga, digital-or-analog - Elektroninė arba neelektroninė paslauga
    |                                                            | enum            |              | digital                                          |         |       |        |     |       |
    |                                                            |                 |              | analog                                           |         |       |        |     |       |
    |                                                            |                 |              | digital-or-analog                                |         |       |        |     |       |
    |   |   |   |   | who_may_consitute                          | string required |              | who_may_consitute/text()                         |         |       |        |     |       | Įgaliojimą gali sudaryti.
    |                                                            | enum            |              | fiz                                              |         |       |        |     |       |
    |                                                            |                 |              | fiz-notarial                                     |         |       |        |     |       |
    |                                                            |                 |              | jur                                              |         |       |        |     |       |
    |                                                            |                 |              | jur-notarial                                     |         |       |        |     |       |
    |                                                            |                 |              | fiz-jur                                          |         |       |        |     |       |
    |                                                            |                 |              | fiz-notarial-jur-notarial                        |         |       |        |     |       |
    |                                                            |                 |              | fiz-notarial-jur                                 |         |       |        |     |       |
    |                                                            |                 |              | fiz-jur-notarial                                 |         |       |        |     |       |

"""
    path = tmp_path / 'manifest.xsd'
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
                <xs:element minOccurs="0" maxOccurs="1" name="RegDateFrom" type="xs:string" />
                <xs:element minOccurs="0" maxOccurs="1" name="RegDateTo" type="xs:string" />
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
    |   | resource1                                    | xml               |                  |                                               |         |       |        |     |       |
    |                                                  |                   |                  |                                               |         |       |        |     |       |
    |   |   |   | Extract                              |                   |                  | /getDocumentsByWagonResponse/extract          |         |       |        |     |       |
    |   |   |   |   | extract_preparation_time         | datetime required |                  | extractPreparationTime/text()                 |         |       |        |     |       |
    |   |   |   |   | last_update_time                 | datetime required |                  | lastUpdateTime/text()                         |         |       |        |     |       |
    |                                                  |                   |                  |                                               |         |       |        |     |       |
    |   |   |   | GetDocumentsByWagonResponse1         |                   |                  | /getDocumentsByWagonResponse                  |         |       |        |     |       |
    |   |   |   |   | aprasymas                        | string            |                  | klaida/Aprasymas/text()                       |         |       |        |     |       |
    |   |   |   |   | search_parameters                | ref               | SearchParameters | searchParameters                              |         |       |        |     |       |
    |   |   |   |   | search_parameters.code           | string            |                  | searchParameters/code/text()                  |         |       |        |     |       |
    |   |   |   |   | search_parameters.reg_date_from  | string            |                  | searchParameters/RegDateFrom/text()           |         |       |        |     |       |
    |   |   |   |   | search_parameters.reg_date_to    | string            |                  | searchParameters/RegDateTo/text()             |         |       |        |     |       |
    |   |   |   |   | search_parameters.search_type    | string            |                  | searchParameters/searchType/text()            |         |       |        |     |       |
    |                                                  |                   |                  |                                               |         |       |        |     |       |
    |   |   |   | GetDocumentsByWagonResponse2         |                   |                  | /getDocumentsByWagonResponse                  |         |       |        |     |       |
    |   |   |   |   | extract                          | ref               | Extract          | extract                                       |         |       |        |     |       |
    |   |   |   |   | extract.extract_preparation_time | datetime required |                  | extract/extractPreparationTime/text()         |         |       |        |     |       |
    |   |   |   |   | extract.last_update_time         | datetime required |                  | extract/lastUpdateTime/text()                 |         |       |        |     |       |
    |   |   |   |   | search_parameters                | ref               | SearchParameters | searchParameters                              |         |       |        |     |       |
    |   |   |   |   | search_parameters.code           | string            |                  | searchParameters/code/text()                  |         |       |        |     |       |
    |   |   |   |   | search_parameters.reg_date_from  | string            |                  | searchParameters/RegDateFrom/text()           |         |       |        |     |       |
    |   |   |   |   | search_parameters.reg_date_to    | string            |                  | searchParameters/RegDateTo/text()             |         |       |        |     |       |
    |   |   |   |   | search_parameters.search_type    | string            |                  | searchParameters/searchType/text()            |         |       |        |     |       |
    |                                                  |                   |                  |                                               |         |       |        |     |       |
    |   |   |   | SearchParameters                     |                   |                  | /getDocumentsByWagonResponse/searchParameters |         |       |        |     |       |
    |   |   |   |   | code                             | string            |                  | code/text()                                   |         |       |        |     |       |
    |   |   |   |   | reg_date_from                    | string            |                  | RegDateFrom/text()                            |         |       |        |     |       |
    |   |   |   |   | reg_date_to                      | string            |                  | RegDateTo/text()                              |         |       |        |     |       |
    |   |   |   |   | search_type                      | string            |                  | searchType/text()                             |         |       |        |     |       |
    """

    path = tmp_path / 'manifest.xsd'
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table
