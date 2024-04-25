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


def test_xsd_rc729(rc: RawConfig, tmp_path: Path):

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
    <xs:sequence>
      <xs:element ref="asmuo"                 minOccurs="0" maxOccurs="unbounded" />
    </xs:sequence>

    <xs:attribute name="puslapis" type="xs:long" use="required">
      <xs:annotation><xs:documentation>rezultatu puslapio numeris</xs:documentation></xs:annotation>
    </xs:attribute>

    <xs:attribute name="viso_puslapiu" type="xs:long" use="required">
      <xs:annotation><xs:documentation>rezultatu puslapiu skaicius</xs:documentation></xs:annotation>
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
id | d | r | b | m | property                | type             | ref      | source                                   | prepare | level | access | uri | title | description
   | manifest                                |                  |          |                                          |         |       |        |     |       |
   |   | resource1                           | xml              |          |                                          |         |       |        |     |       |
   |                                         |                  |          |                                          |         |       |        |     |       |
   |   |   |   | Resource                    |                  |          | /                                        |         |       |        | http://www.w3.org/2000/01/rdf-schema#Resource |       | Įvairūs duomenys
   |   |   |   |   | klaida                  | string           |          | klaida/text()                            |         |       |        |     |       | Klaidos atveju - klaidos pranešimas
   |                                         |                  |          |                                          |         |       |        |     |       |
   |   |   |   | Asmuo                       |                  |          | /klientu_saraso_rezultatas/asmenys/asmuo |         |       |        |     |       |
   |   |   |   |   | asmenys                 | ref              | Asmenys  |                                          |         |       |        |     |       |
   |   |   |   |   | id                      | string required  |          | @id                                      |         |       |        |     |       |
   |   |   |   |   | ak                      | string required  |          | @ak                                      |         |       |        |     |       |
   |   |   |   |   | text                    | string           |          | text()                                   |         |       |        |     |       |
   |                                         |                  |          |                                          |         |       |        |     |       |
   |   |   |   | Asmenys                     |                  |          | /klientu_saraso_rezultatas/asmenys       |         |       |        |     |       |
   |   |   |   |   | puslapis                | integer required |          | @puslapis                                |         |       |        |     |       | rezultatu puslapio numeris
   |   |   |   |   | viso_puslapiu           | integer required |          | @viso_puslapiu                           |         |       |        |     |       | rezultatu puslapiu skaicius
   |   |   |   |   | text                    | string           |          | text()                                   |         |       |        |     |       |
   |   |   |   |   | asmuo[]                 | backref          | Asmuo    |                                          |         |       |        |     |       |
   |                                         |                  |          |                                          |         |       |        |     |       |
   |   |   |   | KlientuSarasoRezultatas     |                  |          | /klientu_saraso_rezultatas               |         |       |        |     |       |
   |   |   |   |   | text                    | string           |          | text()                                   |         |       |        |     |       |
   |   |   |   |   | asmenys                 | ref              | Asmenys  |                                          |         |       |        |     |       |
   |                                         |                  |          |                                          |         |       |        |     |       |
   |   |   |   | Asmuo1                      |                  |          | /asmenys/asmuo                           |         |       |        |     |       |
   |   |   |   |   | asmenys1                | ref              | Asmenys1 |                                          |         |       |        |     |       |
   |   |   |   |   | id                      | string required  |          | @id                                      |         |       |        |     |       |
   |   |   |   |   | ak                      | string required  |          | @ak                                      |         |       |        |     |       |
   |   |   |   |   | text                    | string           |          | text()                                   |         |       |        |     |       |
   |                                         |                  |          |                                          |         |       |        |     |       |
   |   |   |   | Asmenys1                    |                  |          | /asmenys                                 |         |       |        |     |       |
   |   |   |   |   | puslapis                | integer required |          | @puslapis                                |         |       |        |     |       | rezultatu puslapio numeris
   |   |   |   |   | viso_puslapiu           | integer required |          | @viso_puslapiu                           |         |       |        |     |       | rezultatu puslapiu skaicius
   |   |   |   |   | text                    | string           |          | text()                                   |         |       |        |     |       |
   |   |   |   |   | asmuo[]                 | backref          | Asmuo1   |                                          |         |       |        |     |       |
   |                                         |                  |          |                                          |         |       |        |     |       |
   |   |   |   | Asmuo2                      |                  |          | /asmuo                                   |         |       |        |     |       |
   |   |   |   |   | id                      | string required  |          | @id                                      |         |       |        |     |       |
   |   |   |   |   | ak                      | string required  |          | @ak                                      |         |       |        |     |       |
   |   |   |   |   | text                    | string           |          | text()                                   |         |       |        |     |       |
  """

    path = tmp_path / 'manifest.xsd'
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    print(manifest)
    assert manifest == table


def test_xsd_rc729_variant(rc: RawConfig, tmp_path: Path):

    xsd = """

<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">


<xs:element name="klientu_saraso_rezultatas">
  <xs:complexType mixed="true">
    <xs:sequence>
      <xs:element name="asmenys" minOccurs="0" maxOccurs="1">
        <xs:complexType mixed="true">
          <xs:sequence>
            <xs:element name="asmuo" minOccurs="0" maxOccurs="unbounded">
  <xs:complexType mixed="true">

      <xs:attribute name="id"     type="xs:string" use="required">
      </xs:attribute>

      <xs:attribute name="ak"  type="xs:string" use="required">
      </xs:attribute>

  </xs:complexType>
</xs:element>
          </xs:sequence>

          <xs:attribute name="puslapis" type="xs:long" use="required">
            <xs:annotation><xs:documentation>rezultatu puslapio numeris</xs:documentation></xs:annotation>
          </xs:attribute>

          <xs:attribute name="viso_puslapiu" type="xs:long" use="required">
            <xs:annotation><xs:documentation>rezultatu puslapiu skaicius</xs:documentation></xs:annotation>
          </xs:attribute>

        </xs:complexType>
     </xs:element>
    </xs:sequence>
  </xs:complexType>
</xs:element>


<xs:element name="klaida" type="xs:string">
  <xs:annotation><xs:documentation>Klaidos atveju - klaidos pranešimas</xs:documentation></xs:annotation>
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
   |   |   |   | Asmuo                       |                  |          | /klientu_saraso_rezultatas/asmenys/asmuo |         |       |        |     |       |
   |   |   |   |   | id                      | string required  |          | @id                                      |         |       |        |     |       |
   |   |   |   |   | ak                      | string required  |          | @ak                                      |         |       |        |     |       |
   |   |   |   |   | text                    | string           |          | text()                                   |         |       |        |     |       |
   |                                         |                  |          |                                          |         |       |        |     |       |
   |   |   |   | Asmenys                     |                  |          | /klientu_saraso_rezultatas/asmenys       |         |       |        |     |       |
   |   |   |   |   | puslapis                | integer required |          | @puslapis                                |         |       |        |     |       | rezultatu puslapio numeris
   |   |   |   |   | viso_puslapiu           | integer required |          | @viso_puslapiu                           |         |       |        |     |       | rezultatu puslapiu skaicius
   |   |   |   |   | text                    | string           |          | text()                                   |         |       |        |     |       |
   |                                         |                  |          |                                          |         |       |        |     |       |
   |   |   |   | KlientuSarasoRezultatas     |                  |          | /klientu_saraso_rezultatas               |         |       |        |     |       |
   |   |   |   |   | text                    | string           |          | text()                                   |         |       |        |     |       |
  """

    path = tmp_path / 'manifest.xsd'
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    print(manifest)
    assert manifest == table


def test_xsd_rc742_variant(rc: RawConfig, tmp_path: Path):

    xsd = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">


<xs:element name="COVID19_SKIEPAI_EU">
  <xs:annotation><xs:documentation>COVID19 skiepai EU</xs:documentation></xs:annotation>
  <xs:complexType>
    <xs:sequence>
      <xs:element ref="SKIEPAS_EU" minOccurs="0" maxOccurs="unbounded"/>
    </xs:sequence>
  </xs:complexType>
</xs:element>


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

      <xs:element minOccurs="0" maxOccurs="1" name="PACIENTO_VARDAS">
        <xs:annotation><xs:documentation>Paciento vardas</xs:documentation></xs:annotation>
        <xs:simpleType>
          <xs:restriction base="xs:string"><xs:maxLength value="1024"/></xs:restriction>
        </xs:simpleType>
      </xs:element>

      <xs:element minOccurs="0" maxOccurs="1" name="PACIENTO_PAVARDE">
        <xs:annotation><xs:documentation>Paciento pavarde</xs:documentation></xs:annotation>
        <xs:simpleType>
          <xs:restriction base="xs:string"><xs:maxLength value="1024"/></xs:restriction>
        </xs:simpleType>
      </xs:element>

      <xs:element minOccurs="1" maxOccurs="1" name="PACIENTO_GIM_DATA" type="t_data">
        <xs:annotation><xs:documentation>Paciento gimimo data</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element minOccurs="0" maxOccurs="1" name="PACIENTO_ESINR">
        <xs:annotation><xs:documentation>Paciento ESI numeris</xs:documentation></xs:annotation>
        <xs:simpleType>
          <xs:restriction base="xs:string"><xs:maxLength value="1024"/></xs:restriction>
        </xs:simpleType>
      </xs:element>

      <xs:element minOccurs="0" maxOccurs="1" name="VAKCINOS_PAV">
        <xs:annotation><xs:documentation>Vakcinos pavadinimas</xs:documentation></xs:annotation>
        <xs:simpleType>
          <xs:restriction base="xs:string"><xs:maxLength value="100"/></xs:restriction>
        </xs:simpleType>
      </xs:element>

      <xs:element minOccurs="0" maxOccurs="1" name="DOZES_EILNR">
        <xs:annotation><xs:documentation>Dozės eilės numeris</xs:documentation></xs:annotation>
        <xs:simpleType>
          <xs:restriction base="xs:int"></xs:restriction>
        </xs:simpleType>
      </xs:element>

      <xs:element minOccurs="0" maxOccurs="1" name="DOZIU_VISO">
        <xs:annotation><xs:documentation>Viso reikalinga dozių iki pilno paskiepijimo kurso</xs:documentation></xs:annotation>
        <xs:simpleType>
          <xs:restriction base="xs:int"></xs:restriction>
        </xs:simpleType>
      </xs:element>

      <xs:element minOccurs="1" maxOccurs="1" name="SKIEPIJIMO_DATA" type="t_data">
        <xs:annotation><xs:documentation>Skiepijimo data</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element minOccurs="0" maxOccurs="1" name="SKIEPIJIMO_VALSTYBE">
        <xs:annotation><xs:documentation>Valstybė, kurioje buvo paskiepyta</xs:documentation></xs:annotation>
        <xs:simpleType>
          <xs:restriction base="xs:string"><xs:maxLength value="300"/></xs:restriction>
        </xs:simpleType>
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
   |   |   |   | SkiepasEu               |                 |                  | /COVID19_SKIEPAI_EU/SKIEPAS_EU |         |       |        |     |       |
   |   |   |   |   | covid19_skiepai_eu  | ref             | Covid19SkiepaiEu |                                |         |       |        |     |       |
   |   |   |   |   | paciento_ak         | string          |                  | PACIENTO_AK/text()             |         |       |        |     |       | Paciento asmens kodas (LTU)
   |   |   |   |   | paciento_vardas     | string          |                  | PACIENTO_VARDAS/text()         |         |       |        |     |       | Paciento vardas
   |   |   |   |   | paciento_pavarde    | string          |                  | PACIENTO_PAVARDE/text()        |         |       |        |     |       | Paciento pavarde
   |   |   |   |   | paciento_gim_data   | string required |                  | PACIENTO_GIM_DATA/text()       |         |       |        |     |       | Paciento gimimo data
   |   |   |   |   | paciento_esinr      | string          |                  | PACIENTO_ESINR/text()          |         |       |        |     |       | Paciento ESI numeris
   |   |   |   |   | vakcinos_pav        | string          |                  | VAKCINOS_PAV/text()            |         |       |        |     |       | Vakcinos pavadinimas
   |   |   |   |   | dozes_eilnr         | integer         |                  | DOZES_EILNR/text()             |         |       |        |     |       | Dozės eilės numeris
   |   |   |   |   | doziu_viso          | integer         |                  | DOZIU_VISO/text()              |         |       |        |     |       | Viso reikalinga dozių iki pilno paskiepijimo kurso
   |   |   |   |   | skiepijimo_data     | string required |                  | SKIEPIJIMO_DATA/text()         |         |       |        |     |       | Skiepijimo data
   |   |   |   |   | skiepijimo_valstybe | string          |                  | SKIEPIJIMO_VALSTYBE/text()     |         |       |        |     |       | Valstybė, kurioje buvo paskiepyta
   |                                     |                 |                  |                                |         |       |        |     |       |
   |   |   |   | Covid19SkiepaiEu        |                 |                  | /COVID19_SKIEPAI_EU            |         |       |        |     |       | COVID19 skiepai EU
   |   |   |   |   | skiepas_eu[]        | backref         | SkiepasEu        |                                |         |       |        |     |       |
   |                                     |                 |                  |                                |         |       |        |     |       |
   |   |   |   | SkiepasEu1              |                 |                  | /SKIEPAS_EU                    |         |       |        |     |       |
   |   |   |   |   | paciento_ak         | string          |                  | PACIENTO_AK/text()             |         |       |        |     |       | Paciento asmens kodas (LTU)
   |   |   |   |   | paciento_vardas     | string          |                  | PACIENTO_VARDAS/text()         |         |       |        |     |       | Paciento vardas
   |   |   |   |   | paciento_pavarde    | string          |                  | PACIENTO_PAVARDE/text()        |         |       |        |     |       | Paciento pavarde
   |   |   |   |   | paciento_gim_data   | string required |                  | PACIENTO_GIM_DATA/text()       |         |       |        |     |       | Paciento gimimo data
   |   |   |   |   | paciento_esinr      | string          |                  | PACIENTO_ESINR/text()          |         |       |        |     |       | Paciento ESI numeris
   |   |   |   |   | vakcinos_pav        | string          |                  | VAKCINOS_PAV/text()            |         |       |        |     |       | Vakcinos pavadinimas
   |   |   |   |   | dozes_eilnr         | integer         |                  | DOZES_EILNR/text()             |         |       |        |     |       | Dozės eilės numeris
   |   |   |   |   | doziu_viso          | integer         |                  | DOZIU_VISO/text()              |         |       |        |     |       | Viso reikalinga dozių iki pilno paskiepijimo kurso
   |   |   |   |   | skiepijimo_data     | string required |                  | SKIEPIJIMO_DATA/text()         |         |       |        |     |       | Skiepijimo data
   |   |   |   |   | skiepijimo_valstybe | string          |                  | SKIEPIJIMO_VALSTYBE/text()     |         |       |        |     |       | Valstybė, kurioje buvo paskiepyta
    """

    path = tmp_path / 'manifest.xsd'
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    print(manifest)
    assert manifest == table


def test_xsd_rc765(rc: RawConfig, tmp_path: Path):

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
   |                                      |                  |         |                             |         |       |        |     |       |
   |   |   |   | Parcel4                  |                  |         | /parcel                     |         |       |        |     |       | Žemės sklypo pasikeitimo informacija
   |   |   |   |   | text                 | string           |         | text()                      |         |       |        |     |       |
   |   |   |   |   | parcel_unique_number | integer required |         | parcel_unique_number/text() |         |       |        |     |       | Žemės sklypo unikalus numeris
   |                                      |                  |         |                             |         |       |        |     |       |
   |   |   |   | Parcel5                  |                  |         | /parcel                     |         |       |        |     |       | Žemės sklypo pasikeitimo informacija
   |   |   |   |   | text                 | string           |         | text()                      |         |       |        |     |       |
   |   |   |   |   | sign_of_change       | integer required |         | sign_of_change/text()       |         |       |        |     |       | Žemės sklypo pasikeitimo požymis
   |                                      | enum             |         | 1                           |         |       |        |     |       |
   |                                      |                  |         | 2                           |         |       |        |     |       |
   |                                      |                  |         | 3                           |         |       |        |     |       |
    """

    path = tmp_path / 'manifest.xsd'
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    print(manifest)
    assert manifest == table


def test_xsd_rc137(rc: RawConfig, tmp_path: Path):
    # attributes
    xsd = """

<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">

<xs:element name="REGISTRO_E_DOKUMENTAI">
  <xs:complexType>
    <xs:all>
      <xs:element minOccurs="0"                       ref="DEBUG" />
      <xs:element minOccurs="0" maxOccurs="1"         ref="REGISTRAS" />
      <xs:element minOccurs="0"                       ref="E_DOKUMENTAI" />	
      <xs:element minOccurs="0" maxOccurs="1"         ref="SISTEMOS_INFORMACIJA"/>
    </xs:all>
  </xs:complexType>
</xs:element>

<xs:element name="DEBUG">
  <xs:annotation><xs:documentation>pagalbinė techninė informacija</xs:documentation></xs:annotation>
  <xs:complexType mixed="true">

    <xs:attribute name="id"  type="xs:string" use="optional">
      <xs:annotation><xs:documentation>patikslinanti informacija</xs:documentation></xs:annotation>
    </xs:attribute>

  </xs:complexType>
</xs:element>

<xs:element name="REGISTRAS">
  <xs:annotation><xs:documentation>NT registro duomenys</xs:documentation></xs:annotation>
  <xs:complexType mixed="true">
    <xs:choice minOccurs="0" maxOccurs="unbounded">

      <xs:element name="REG_TARN_NR"    minOccurs="1" maxOccurs="1" type="xs:long"> 
        <xs:annotation><xs:documentation>[ntr311.registrai.reg_tarn_nr]</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element name="REG_NR"         minOccurs="1" maxOccurs="1" type="xs:long">
        <xs:annotation><xs:documentation>[ntr311.registrai.reg_nr]</xs:documentation></xs:annotation>
      </xs:element>

    </xs:choice>
  </xs:complexType>
</xs:element>

<xs:element name="E_DOKUMENTAI">
  <xs:complexType mixed="true">
    <xs:choice minOccurs="0" maxOccurs="unbounded">
      <xs:element ref="E_DOKUMENTAS"      minOccurs="1" maxOccurs="unbounded">
        <xs:annotation><xs:documentation>Infomacija apie el. dokumentą</xs:documentation></xs:annotation>
      </xs:element>
    </xs:choice>
  </xs:complexType>
</xs:element>

<xs:element name="E_DOKUMENTAS">
  <xs:annotation><xs:documentation>El. dokumento duomenys</xs:documentation></xs:annotation>
  <xs:complexType mixed="true">
    <xs:all>

      <xs:element name="DOK_ID" minOccurs="1" maxOccurs="1" type="xs:long">
        <xs:annotation><xs:documentation>dokumento id [ntr311.dokumentai.dok_id]</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element ref="DOKU_TIPAS" minOccurs="1" maxOccurs="1">
        <xs:annotation><xs:documentation>dokumento tipo id [dokumentai.dokumentu_tipai.doku_tipas]</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element ref="D_DOKU_TIPAS" minOccurs="0" maxOccurs="1">
        <xs:annotation><xs:documentation>[]</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element name="PAVAD" minOccurs="0" maxOccurs="1">
        <xs:annotation><xs:documentation>[]</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element name="EDOK_ID" minOccurs="1" maxOccurs="1" type="xs:long">
        <xs:annotation><xs:documentation>el. dokumento identifikatorius el. dokumentų archyve</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element name="DOK_NR" minOccurs="0" maxOccurs="1" type="xs:string">
        <xs:annotation><xs:documentation>dokumento numeris</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element name="DOK_DATA" minOccurs="0" maxOccurs="1" type="t_data">
        <xs:annotation><xs:documentation>dokumento data</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element name="IND_DATA" minOccurs="0" maxOccurs="1" type="t_data">
        <xs:annotation><xs:documentation>dokumento indeksavimo data</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element name="PUSL_SK" minOccurs="0" maxOccurs="1" type="xs:long">
        <xs:annotation><xs:documentation>dokumento puslapių skaičius</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element name="PUSL_DYDIS" minOccurs="0" maxOccurs="1" type="xs:string">
        <xs:annotation><xs:documentation>puslapio dydis, pvz., "A4"</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element ref="EDOK_RUSIS" minOccurs="0" maxOccurs="1">
        <xs:annotation><xs:documentation>[]</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element name="SALTINIS" minOccurs="1" maxOccurs="1" type="xs:string"> 
        <xs:annotation><xs:documentation>saltinis (el. dokumentų archyvo raidinis kodas)</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element name="FORMATAS" minOccurs="0" maxOccurs="1" type="xs:string">
        <xs:annotation><xs:documentation>bylos formatas (pvz., "PDF")</xs:documentation></xs:annotation>
      </xs:element>

    </xs:all>

      <xs:attribute name="nr" use="optional" type="xs:integer">
        <xs:annotation><xs:documentation>dokumento eilės numeris sąraše</xs:documentation></xs:annotation>
      </xs:attribute>

  </xs:complexType>
</xs:element>

<xs:element name="DOKU_TIPAS">
  <xs:annotation><xs:documentation>dokumento tipo pavadinimas</xs:documentation></xs:annotation>
  <xs:complexType mixed="true">
      <xs:attribute name="doku_tipas" use="required" type="xs:long">
        <xs:annotation><xs:documentation>dokumento tipo kodas</xs:documentation></xs:annotation>
      </xs:attribute>
  </xs:complexType>
</xs:element>

<xs:element name="D_DOKU_TIPAS">
  <xs:annotation><xs:documentation>dokumento tipo pavadinimas 2</xs:documentation></xs:annotation>
  <xs:complexType mixed="true">
      <xs:attribute name="doku_tipas" use="required" type="xs:long">
        <xs:annotation><xs:documentation>dokumento tipo kodas</xs:documentation></xs:annotation>
      </xs:attribute>
  </xs:complexType>
</xs:element>

<xs:element name="EDOK_RUSIS">
  <xs:annotation><xs:documentation>dokumento rūšies pavadinimas</xs:documentation></xs:annotation>
  <xs:complexType mixed="true">
      <xs:attribute name="edok_rusis" use="required" type="xs:long">
        <xs:annotation><xs:documentation>dokumento rūšies kodas</xs:documentation></xs:annotation>
      </xs:attribute>
  </xs:complexType>
</xs:element>

<xs:element name="SISTEMOS_INFORMACIJA">
  <xs:complexType mixed="true">
    <xs:sequence>
      <xs:element name="VEI_ID"      minOccurs="1" maxOccurs="1">
        <xs:annotation><xs:documentation>veiksmo id RC audito sistemoje</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element name="DATA"  type="xs:string" minOccurs="1" maxOccurs="1">
        <xs:annotation><xs:documentation>rezultato (atsakymo) suformavimo data</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element name="TRUKME" type="xs:string" minOccurs="1" maxOccurs="1">
        <xs:annotation><xs:documentation>rezultato (atsakymo) suformavimui sugaištas laikas, sek.</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element name="DB"               minOccurs="1" maxOccurs="1">
        <xs:annotation><xs:documentation>RC aplinkos kodas</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element name="SSID"               minOccurs="0" maxOccurs="1">
        <xs:annotation><xs:documentation>sesijos identifikatorius</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element ref="VARTOTOJAS"               minOccurs="1" maxOccurs="1">
        <xs:annotation><xs:documentation>vartotojo duomenys</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element ref="SALYGOS"               minOccurs="1" maxOccurs="1">
        <xs:annotation><xs:documentation>paieškos (užklausos) sąlygos</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element ref="IRASAI"               minOccurs="1" maxOccurs="1">
        <xs:annotation><xs:documentation>informacija apie paieškos rezultatų kiekį</xs:documentation></xs:annotation>
      </xs:element>

    </xs:sequence>
  </xs:complexType>
</xs:element>


<xs:element name="VARTOTOJAS">
  <xs:complexType mixed="true">
    <xs:choice minOccurs="0" maxOccurs="unbounded">

      <xs:element name="DB"               minOccurs="1" maxOccurs="1">
        <xs:annotation><xs:documentation>RC aplinkos kodas</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element name="VAR_PAV"               minOccurs="1" maxOccurs="1">
        <xs:annotation><xs:documentation>vartotojo vardo pirma raidė ir pavardė</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element name="VAR_PAVARDE"               minOccurs="1" maxOccurs="1">
        <xs:annotation><xs:documentation>vartotojo pavardė</xs:documentation></xs:annotation>
      </xs:element>

      <xs:element name="VAR_VARDAS"               minOccurs="1" maxOccurs="1">
        <xs:annotation><xs:documentation>vartotojo vardas</xs:documentation></xs:annotation>
      </xs:element>


    </xs:choice>
  </xs:complexType>
</xs:element>


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


<xs:element name="IRASAI">
  <xs:complexType>
    <xs:simpleContent>
      <xs:extension base="xs:unsignedByte">
        <xs:attribute name="sk"         type="xs:unsignedByte"  use="required" />
        <xs:attribute name="nuo"        type="xs:unsignedByte"  use="optional" />
        <xs:attribute name="iki"        type="xs:unsignedByte"  use="optional" />
        <xs:attribute name="viso"       type="xs:unsignedByte"  use="required" />
        <xs:attribute name="limitas"    type="xs:unsignedShort" use="required" />
        <xs:attribute name="pusl_nr"    type="xs:unsignedByte"  use="required" />
        <xs:attribute name="pusl_ilgis" type="xs:unsignedShort" use="required" />
      </xs:extension>
    </xs:simpleContent>
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
id | d | r | b | m | property             | type             | ref                 | source                                                        | prepare | level | access | uri | title | description
   | manifest                             |                  |                     |                                                               |         |       |        |     |       |
   |   | resource1                        | xml              |                     |                                                               |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Debug                    |                  |                     | /REGISTRO_E_DOKUMENTAI/DEBUG                                  |         |       |        |     |       | pagalbinė techninė informacija
   |   |   |   |   | id                   | string           |                     | @id                                                           |         |       |        |     |       | patikslinanti informacija
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Registras1               |                  |                     | /REGISTRO_E_DOKUMENTAI/REGISTRAS                              |         |       |        |     |       | NT registro duomenys
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | reg_tarn_nr          | integer required |                     | REG_TARN_NR/text()                                            |         |       |        |     |       | [ntr311.registrai.reg_tarn_nr]
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Registras2               |                  |                     | /REGISTRO_E_DOKUMENTAI/REGISTRAS                              |         |       |        |     |       | NT registro duomenys
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | reg_nr               | integer required |                     | REG_NR/text()                                                 |         |       |        |     |       | [ntr311.registrai.reg_nr]
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | DokuTipas                |                  |                     | /REGISTRO_E_DOKUMENTAI/E_DOKUMENTAI/E_DOKUMENTAS/DOKU_TIPAS   |         |       |        |     |       | dokumento tipo pavadinimas
   |   |   |   |   | doku_tipas           | integer required |                     | @doku_tipas                                                   |         |       |        |     |       | dokumento tipo kodas
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | DDokuTipas               |                  |                     | /REGISTRO_E_DOKUMENTAI/E_DOKUMENTAI/E_DOKUMENTAS/D_DOKU_TIPAS |         |       |        |     |       | dokumento tipo pavadinimas 2
   |   |   |   |   | doku_tipas           | integer required |                     | @doku_tipas                                                   |         |       |        |     |       | dokumento tipo kodas
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | EdokRusis                |                  |                     | /REGISTRO_E_DOKUMENTAI/E_DOKUMENTAI/E_DOKUMENTAS/EDOK_RUSIS   |         |       |        |     |       | dokumento rūšies pavadinimas
   |   |   |   |   | edok_rusis           | integer required |                     | @edok_rusis                                                   |         |       |        |     |       | dokumento rūšies kodas
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | EDokumentas              |                  |                     | /REGISTRO_E_DOKUMENTAI/E_DOKUMENTAI/E_DOKUMENTAS              |         |       |        |     |       | El. dokumento duomenys
   |   |   |   |   | e_dokumentai1        | ref              | EDokumentai1        |                                                               |         |       |        |     |       |
   |   |   |   |   | nr                   | integer          |                     | @nr                                                           |         |       |        |     |       | dokumento eilės numeris sąraše
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | dok_id               | integer required |                     | DOK_ID/text()                                                 |         |       |        |     |       | dokumento id [ntr311.dokumentai.dok_id]
   |   |   |   |   | pavad                | string           |                     | PAVAD/text()                                                  |         |       |        |     |       | []
   |   |   |   |   | edok_id              | integer required |                     | EDOK_ID/text()                                                |         |       |        |     |       | el. dokumento identifikatorius el. dokumentų archyve
   |   |   |   |   | dok_nr               | string           |                     | DOK_NR/text()                                                 |         |       |        |     |       | dokumento numeris
   |   |   |   |   | dok_data             | string           |                     | DOK_DATA/text()                                               |         |       |        |     |       | dokumento data
   |   |   |   |   | ind_data             | string           |                     | IND_DATA/text()                                               |         |       |        |     |       | dokumento indeksavimo data
   |   |   |   |   | pusl_sk              | integer          |                     | PUSL_SK/text()                                                |         |       |        |     |       | dokumento puslapių skaičius
   |   |   |   |   | pusl_dydis           | string           |                     | PUSL_DYDIS/text()                                             |         |       |        |     |       | puslapio dydis, pvz., "A4"
   |   |   |   |   | saltinis             | string required  |                     | SALTINIS/text()                                               |         |       |        |     |       | saltinis (el. dokumentų archyvo raidinis kodas)
   |   |   |   |   | formatas             | string           |                     | FORMATAS/text()                                               |         |       |        |     |       | bylos formatas (pvz., "PDF")
   |   |   |   |   | doku_tipas           | ref required     | DokuTipas           |                                                               |         |       |        |     |       | dokumento tipo id [dokumentai.dokumentu_tipai.doku_tipas]
   |   |   |   |   | d_doku_tipas         | ref              | DDokuTipas          |                                                               |         |       |        |     |       | []
   |   |   |   |   | edok_rusis           | ref              | EdokRusis           |                                                               |         |       |        |     |       | []
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | EDokumentai1             |                  |                     | /REGISTRO_E_DOKUMENTAI/E_DOKUMENTAI                           |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | e_dokumentas[]       | backref required | EDokumentas         |                                                               |         |       |        |     |       | Infomacija apie el. dokumentą
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Vartotojas1              |                  |                     | /REGISTRO_E_DOKUMENTAI/SISTEMOS_INFORMACIJA/VARTOTOJAS        |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | db                   | string required  |                     | DB/text()                                                     |         |       |        |     |       | RC aplinkos kodas
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Vartotojas2              |                  |                     | /REGISTRO_E_DOKUMENTAI/SISTEMOS_INFORMACIJA/VARTOTOJAS        |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | var_pav              | string required  |                     | VAR_PAV/text()                                                |         |       |        |     |       | vartotojo vardo pirma raidė ir pavardė
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Vartotojas3              |                  |                     | /REGISTRO_E_DOKUMENTAI/SISTEMOS_INFORMACIJA/VARTOTOJAS        |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | var_pavarde          | string required  |                     | VAR_PAVARDE/text()                                            |         |       |        |     |       | vartotojo pavardė
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Vartotojas4              |                  |                     | /REGISTRO_E_DOKUMENTAI/SISTEMOS_INFORMACIJA/VARTOTOJAS        |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | var_vardas           | string required  |                     | VAR_VARDAS/text()                                             |         |       |        |     |       | vartotojo vardas
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Salyga                   |                  |                     | /REGISTRO_E_DOKUMENTAI/SISTEMOS_INFORMACIJA/SALYGOS/SALYGA    |         |       |        |     |       |
   |   |   |   |   | kodas                | string           |                     | @kodas                                                        |         |       |        |     |       |
   |   |   |   |   | nr                   | integer          |                     | @nr                                                           |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | reiksme              | string required  |                     | REIKSME/text()                                                |         |       |        |     |       |
   |   |   |   |   | pavadinimas          | string           |                     | PAVADINIMAS/text()                                            |         |       |        |     |       |
   |   |   |   |   | aprasymas            | string           |                     | APRASYMAS/text()                                              |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Salygos1                 |                  |                     | /REGISTRO_E_DOKUMENTAI/SISTEMOS_INFORMACIJA/SALYGOS           |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | salyga               | ref required     | Salyga              |                                                               |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | SistemosInformacija      |                  |                     | /REGISTRO_E_DOKUMENTAI/SISTEMOS_INFORMACIJA                   |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | vei_id               | string required  |                     | VEI_ID/text()                                                 |         |       |        |     |       | veiksmo id RC audito sistemoje
   |   |   |   |   | data                 | string required  |                     | DATA/text()                                                   |         |       |        |     |       | rezultato (atsakymo) suformavimo data
   |   |   |   |   | trukme               | string required  |                     | TRUKME/text()                                                 |         |       |        |     |       | rezultato (atsakymo) suformavimui sugaištas laikas, sek.
   |   |   |   |   | db                   | string required  |                     | DB/text()                                                     |         |       |        |     |       | RC aplinkos kodas
   |   |   |   |   | ssid                 | string           |                     | SSID/text()                                                   |         |       |        |     |       | sesijos identifikatorius
   |   |   |   |   | vartotojas           | ref required     | Vartotojas1         |                                                               |         |       |        |     |       | vartotojo duomenys
   |   |   |   |   | vartotojas1          | ref required     | Vartotojas2         |                                                               |         |       |        |     |       | vartotojo duomenys
   |   |   |   |   | vartotojas2          | ref required     | Vartotojas3         |                                                               |         |       |        |     |       | vartotojo duomenys
   |   |   |   |   | vartotojas3          | ref required     | Vartotojas4         |                                                               |         |       |        |     |       | vartotojo duomenys
   |   |   |   |   | salygos              | ref required     | Salygos1            |                                                               |         |       |        |     |       | paieškos (užklausos) sąlygos
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | RegistroEDokumentai      |                  |                     | /REGISTRO_E_DOKUMENTAI                                        |         |       |        |     |       |
   |   |   |   |   | debug                | ref              | Debug               |                                                               |         |       |        |     |       |
   |   |   |   |   | registras            | ref              | Registras1          |                                                               |         |       |        |     |       |
   |   |   |   |   | registras1           | ref              | Registras2          |                                                               |         |       |        |     |       |
   |   |   |   |   | e_dokumentai         | ref              | EDokumentai1        |                                                               |         |       |        |     |       |
   |   |   |   |   | sistemos_informacija | ref              | SistemosInformacija |                                                               |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Debug1                   |                  |                     | /DEBUG                                                        |         |       |        |     |       | pagalbinė techninė informacija
   |   |   |   |   | id                   | string           |                     | @id                                                           |         |       |        |     |       | patikslinanti informacija
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Registras4               |                  |                     | /REGISTRAS                                                    |         |       |        |     |       | NT registro duomenys
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | reg_tarn_nr          | integer required |                     | REG_TARN_NR/text()                                            |         |       |        |     |       | [ntr311.registrai.reg_tarn_nr]
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Registras5               |                  |                     | /REGISTRAS                                                    |         |       |        |     |       | NT registro duomenys
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | reg_nr               | integer required |                     | REG_NR/text()                                                 |         |       |        |     |       | [ntr311.registrai.reg_nr]
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | DokuTipas1               |                  |                     | /E_DOKUMENTAI/E_DOKUMENTAS/DOKU_TIPAS                         |         |       |        |     |       | dokumento tipo pavadinimas
   |   |   |   |   | doku_tipas           | integer required |                     | @doku_tipas                                                   |         |       |        |     |       | dokumento tipo kodas
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | DDokuTipas1              |                  |                     | /E_DOKUMENTAI/E_DOKUMENTAS/D_DOKU_TIPAS                       |         |       |        |     |       | dokumento tipo pavadinimas 2
   |   |   |   |   | doku_tipas           | integer required |                     | @doku_tipas                                                   |         |       |        |     |       | dokumento tipo kodas
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | EdokRusis1               |                  |                     | /E_DOKUMENTAI/E_DOKUMENTAS/EDOK_RUSIS                         |         |       |        |     |       | dokumento rūšies pavadinimas
   |   |   |   |   | edok_rusis           | integer required |                     | @edok_rusis                                                   |         |       |        |     |       | dokumento rūšies kodas
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | EDokumentas1             |                  |                     | /E_DOKUMENTAI/E_DOKUMENTAS                                    |         |       |        |     |       | El. dokumento duomenys
   |   |   |   |   | e_dokumentai3        | ref              | EDokumentai3        |                                                               |         |       |        |     |       |
   |   |   |   |   | nr                   | integer          |                     | @nr                                                           |         |       |        |     |       | dokumento eilės numeris sąraše
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | dok_id               | integer required |                     | DOK_ID/text()                                                 |         |       |        |     |       | dokumento id [ntr311.dokumentai.dok_id]
   |   |   |   |   | pavad                | string           |                     | PAVAD/text()                                                  |         |       |        |     |       | []
   |   |   |   |   | edok_id              | integer required |                     | EDOK_ID/text()                                                |         |       |        |     |       | el. dokumento identifikatorius el. dokumentų archyve
   |   |   |   |   | dok_nr               | string           |                     | DOK_NR/text()                                                 |         |       |        |     |       | dokumento numeris
   |   |   |   |   | dok_data             | string           |                     | DOK_DATA/text()                                               |         |       |        |     |       | dokumento data
   |   |   |   |   | ind_data             | string           |                     | IND_DATA/text()                                               |         |       |        |     |       | dokumento indeksavimo data
   |   |   |   |   | pusl_sk              | integer          |                     | PUSL_SK/text()                                                |         |       |        |     |       | dokumento puslapių skaičius
   |   |   |   |   | pusl_dydis           | string           |                     | PUSL_DYDIS/text()                                             |         |       |        |     |       | puslapio dydis, pvz., "A4"
   |   |   |   |   | saltinis             | string required  |                     | SALTINIS/text()                                               |         |       |        |     |       | saltinis (el. dokumentų archyvo raidinis kodas)
   |   |   |   |   | formatas             | string           |                     | FORMATAS/text()                                               |         |       |        |     |       | bylos formatas (pvz., "PDF")
   |   |   |   |   | doku_tipas           | ref required     | DokuTipas1          |                                                               |         |       |        |     |       | dokumento tipo id [dokumentai.dokumentu_tipai.doku_tipas]
   |   |   |   |   | d_doku_tipas         | ref              | DDokuTipas1         |                                                               |         |       |        |     |       | []
   |   |   |   |   | edok_rusis           | ref              | EdokRusis1          |                                                               |         |       |        |     |       | []
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | EDokumentai3             |                  |                     | /E_DOKUMENTAI                                                 |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | e_dokumentas[]       | backref required | EDokumentas1        |                                                               |         |       |        |     |       | Infomacija apie el. dokumentą
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | DokuTipas2               |                  |                     | /E_DOKUMENTAS/DOKU_TIPAS                                      |         |       |        |     |       | dokumento tipo pavadinimas
   |   |   |   |   | doku_tipas           | integer required |                     | @doku_tipas                                                   |         |       |        |     |       | dokumento tipo kodas
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | DDokuTipas2              |                  |                     | /E_DOKUMENTAS/D_DOKU_TIPAS                                    |         |       |        |     |       | dokumento tipo pavadinimas 2
   |   |   |   |   | doku_tipas           | integer required |                     | @doku_tipas                                                   |         |       |        |     |       | dokumento tipo kodas
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | EdokRusis2               |                  |                     | /E_DOKUMENTAS/EDOK_RUSIS                                      |         |       |        |     |       | dokumento rūšies pavadinimas
   |   |   |   |   | edok_rusis           | integer required |                     | @edok_rusis                                                   |         |       |        |     |       | dokumento rūšies kodas
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | EDokumentas2             |                  |                     | /E_DOKUMENTAS                                                 |         |       |        |     |       | El. dokumento duomenys
   |   |   |   |   | nr                   | integer          |                     | @nr                                                           |         |       |        |     |       | dokumento eilės numeris sąraše
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | dok_id               | integer required |                     | DOK_ID/text()                                                 |         |       |        |     |       | dokumento id [ntr311.dokumentai.dok_id]
   |   |   |   |   | pavad                | string           |                     | PAVAD/text()                                                  |         |       |        |     |       | []
   |   |   |   |   | edok_id              | integer required |                     | EDOK_ID/text()                                                |         |       |        |     |       | el. dokumento identifikatorius el. dokumentų archyve
   |   |   |   |   | dok_nr               | string           |                     | DOK_NR/text()                                                 |         |       |        |     |       | dokumento numeris
   |   |   |   |   | dok_data             | string           |                     | DOK_DATA/text()                                               |         |       |        |     |       | dokumento data
   |   |   |   |   | ind_data             | string           |                     | IND_DATA/text()                                               |         |       |        |     |       | dokumento indeksavimo data
   |   |   |   |   | pusl_sk              | integer          |                     | PUSL_SK/text()                                                |         |       |        |     |       | dokumento puslapių skaičius
   |   |   |   |   | pusl_dydis           | string           |                     | PUSL_DYDIS/text()                                             |         |       |        |     |       | puslapio dydis, pvz., "A4"
   |   |   |   |   | saltinis             | string required  |                     | SALTINIS/text()                                               |         |       |        |     |       | saltinis (el. dokumentų archyvo raidinis kodas)
   |   |   |   |   | formatas             | string           |                     | FORMATAS/text()                                               |         |       |        |     |       | bylos formatas (pvz., "PDF")
   |   |   |   |   | doku_tipas           | ref required     | DokuTipas2          |                                                               |         |       |        |     |       | dokumento tipo id [dokumentai.dokumentu_tipai.doku_tipas]
   |   |   |   |   | d_doku_tipas         | ref              | DDokuTipas2         |                                                               |         |       |        |     |       | []
   |   |   |   |   | edok_rusis           | ref              | EdokRusis2          |                                                               |         |       |        |     |       | []
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | DokuTipas3               |                  |                     | /DOKU_TIPAS                                                   |         |       |        |     |       | dokumento tipo pavadinimas
   |   |   |   |   | doku_tipas           | integer required |                     | @doku_tipas                                                   |         |       |        |     |       | dokumento tipo kodas
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | DDokuTipas3              |                  |                     | /D_DOKU_TIPAS                                                 |         |       |        |     |       | dokumento tipo pavadinimas 2
   |   |   |   |   | doku_tipas           | integer required |                     | @doku_tipas                                                   |         |       |        |     |       | dokumento tipo kodas
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | EdokRusis3               |                  |                     | /EDOK_RUSIS                                                   |         |       |        |     |       | dokumento rūšies pavadinimas
   |   |   |   |   | edok_rusis           | integer required |                     | @edok_rusis                                                   |         |       |        |     |       | dokumento rūšies kodas
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Vartotojas6              |                  |                     | /SISTEMOS_INFORMACIJA/VARTOTOJAS                              |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | db                   | string required  |                     | DB/text()                                                     |         |       |        |     |       | RC aplinkos kodas
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Vartotojas7              |                  |                     | /SISTEMOS_INFORMACIJA/VARTOTOJAS                              |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | var_pav              | string required  |                     | VAR_PAV/text()                                                |         |       |        |     |       | vartotojo vardo pirma raidė ir pavardė
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Vartotojas8              |                  |                     | /SISTEMOS_INFORMACIJA/VARTOTOJAS                              |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | var_pavarde          | string required  |                     | VAR_PAVARDE/text()                                            |         |       |        |     |       | vartotojo pavardė
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Vartotojas9              |                  |                     | /SISTEMOS_INFORMACIJA/VARTOTOJAS                              |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | var_vardas           | string required  |                     | VAR_VARDAS/text()                                             |         |       |        |     |       | vartotojo vardas
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Salyga1                  |                  |                     | /SISTEMOS_INFORMACIJA/SALYGOS/SALYGA                          |         |       |        |     |       |
   |   |   |   |   | kodas                | string           |                     | @kodas                                                        |         |       |        |     |       |
   |   |   |   |   | nr                   | integer          |                     | @nr                                                           |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | reiksme              | string required  |                     | REIKSME/text()                                                |         |       |        |     |       |
   |   |   |   |   | pavadinimas          | string           |                     | PAVADINIMAS/text()                                            |         |       |        |     |       |
   |   |   |   |   | aprasymas            | string           |                     | APRASYMAS/text()                                              |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Salygos3                 |                  |                     | /SISTEMOS_INFORMACIJA/SALYGOS                                 |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | salyga               | ref required     | Salyga1             |                                                               |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | SistemosInformacija1     |                  |                     | /SISTEMOS_INFORMACIJA                                         |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | vei_id               | string required  |                     | VEI_ID/text()                                                 |         |       |        |     |       | veiksmo id RC audito sistemoje
   |   |   |   |   | data                 | string required  |                     | DATA/text()                                                   |         |       |        |     |       | rezultato (atsakymo) suformavimo data
   |   |   |   |   | trukme               | string required  |                     | TRUKME/text()                                                 |         |       |        |     |       | rezultato (atsakymo) suformavimui sugaištas laikas, sek.
   |   |   |   |   | db                   | string required  |                     | DB/text()                                                     |         |       |        |     |       | RC aplinkos kodas
   |   |   |   |   | ssid                 | string           |                     | SSID/text()                                                   |         |       |        |     |       | sesijos identifikatorius
   |   |   |   |   | vartotojas           | ref required     | Vartotojas6         |                                                               |         |       |        |     |       | vartotojo duomenys
   |   |   |   |   | vartotojas1          | ref required     | Vartotojas7         |                                                               |         |       |        |     |       | vartotojo duomenys
   |   |   |   |   | vartotojas2          | ref required     | Vartotojas8         |                                                               |         |       |        |     |       | vartotojo duomenys
   |   |   |   |   | vartotojas3          | ref required     | Vartotojas9         |                                                               |         |       |        |     |       | vartotojo duomenys
   |   |   |   |   | salygos              | ref required     | Salygos3            |                                                               |         |       |        |     |       | paieškos (užklausos) sąlygos
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Vartotojas11             |                  |                     | /VARTOTOJAS                                                   |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | db                   | string required  |                     | DB/text()                                                     |         |       |        |     |       | RC aplinkos kodas
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Vartotojas12             |                  |                     | /VARTOTOJAS                                                   |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | var_pav              | string required  |                     | VAR_PAV/text()                                                |         |       |        |     |       | vartotojo vardo pirma raidė ir pavardė
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Vartotojas13             |                  |                     | /VARTOTOJAS                                                   |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | var_pavarde          | string required  |                     | VAR_PAVARDE/text()                                            |         |       |        |     |       | vartotojo pavardė
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Vartotojas14             |                  |                     | /VARTOTOJAS                                                   |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | var_vardas           | string required  |                     | VAR_VARDAS/text()                                             |         |       |        |     |       | vartotojo vardas
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Salyga2                  |                  |                     | /SALYGOS/SALYGA                                               |         |       |        |     |       |
   |   |   |   |   | kodas                | string           |                     | @kodas                                                        |         |       |        |     |       |
   |   |   |   |   | nr                   | integer          |                     | @nr                                                           |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | reiksme              | string required  |                     | REIKSME/text()                                                |         |       |        |     |       |
   |   |   |   |   | pavadinimas          | string           |                     | PAVADINIMAS/text()                                            |         |       |        |     |       |
   |   |   |   |   | aprasymas            | string           |                     | APRASYMAS/text()                                              |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Salygos5                 |                  |                     | /SALYGOS                                                      |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | salyga               | ref required     | Salyga2             |                                                               |         |       |        |     |       |
   |                                      |                  |                     |                                                               |         |       |        |     |       |
   |   |   |   | Salyga3                  |                  |                     | /SALYGA                                                       |         |       |        |     |       |
   |   |   |   |   | kodas                | string           |                     | @kodas                                                        |         |       |        |     |       |
   |   |   |   |   | nr                   | integer          |                     | @nr                                                           |         |       |        |     |       |
   |   |   |   |   | text                 | string           |                     | text()                                                        |         |       |        |     |       |
   |   |   |   |   | reiksme              | string required  |                     | REIKSME/text()                                                |         |       |        |     |       |
   |   |   |   |   | pavadinimas          | string           |                     | PAVADINIMAS/text()                                            |         |       |        |     |       |
   |   |   |   |   | aprasymas            | string           |                     | APRASYMAS/text()                                              |         |       |        |     |       |
"""

    path = tmp_path / 'manifest.xsd'
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    print(manifest)
    assert manifest == table


def test_xsd_rc1579(rc: RawConfig, tmp_path: Path):

    xsd = """
    <xs:schema xmlns="http://eTaarPlat.ServiceContracts/2007/08/Messages" xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified" targetNamespace="http://eTaarPlat.ServiceContracts/2007/08/Messages">
	<xs:complexType name="getTzByWagonResponse">
		<xs:sequence>
			<xs:element minOccurs="0" maxOccurs="1" name="searchParameters" type="SearchParametersWagon"/>
			<xs:element minOccurs="0" name="extracttz">
				<xs:complexType>
					<xs:sequence>
						<xs:element minOccurs="0" name="extractPreparationTime" type="xs:dateTime" />
						<xs:element minOccurs="0" name="lastUpdateTime" type="xs:dateTime" />
						<xs:element minOccurs="0" name="ptaar" type="xs:unsignedByte" />
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
	<xs:complexType name="SearchParametersWagon">
		<xs:sequence>
			<xs:element minOccurs="1" name="code" type="xs:string" />
		</xs:sequence>
	</xs:complexType>
	<xs:complexType name="getTzByTRAResponse">
		<xs:sequence>
			<xs:element minOccurs="0" maxOccurs="1" name="searchParameters" type="SearchParametersTRA"/>
			<xs:element minOccurs="0" name="extracttz">
				<xs:complexType>
					<xs:sequence>
						<xs:element minOccurs="0" name="extractPreparationTime" type="xs:dateTime" />
						<xs:element minOccurs="0" name="lastUpdateTime" type="xs:dateTime" />
						<xs:element minOccurs="0" name="ptaar" type="xs:unsignedByte" />
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
	<xs:complexType name="SearchParametersTRA">
		<xs:sequence>
			<xs:element minOccurs="1" name="code" type="xs:string" />
			<xs:element minOccurs="0" name="location" type="xs:string" />
			<xs:element minOccurs="1" name="uniCode" type="xs:string" />
		</xs:sequence>
	</xs:complexType>
	<xs:complexType name="getTzByTPResponse">
		<xs:sequence>
			<xs:element minOccurs="0" maxOccurs="1" name="searchParameters" type="SearchParametersTP"/>
			<xs:element minOccurs="0" name="extracttz">
				<xs:complexType>
					<xs:sequence>
						<xs:element minOccurs="0" name="extractPreparationTime" type="xs:dateTime" />
						<xs:element minOccurs="0" name="lastUpdateTime" type="xs:dateTime" />
						<xs:element minOccurs="0" name="ptaar" type="xs:unsignedByte" />
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
	<xs:complexType name="SearchParametersTP">
		<xs:sequence>
			<xs:element minOccurs="1" name="code" type="xs:string" />
		</xs:sequence>
	</xs:complexType>
	<xs:complexType name="getTzByPCCODEResponse">
		<xs:sequence>
			<xs:element minOccurs="0" maxOccurs="1" name="searchParameters" type="SearchParametersTC"/>
			<xs:element minOccurs="0" name="extracttz">
				<xs:complexType>
					<xs:sequence>
						<xs:element minOccurs="0" name="extractPreparationTime" type="xs:dateTime" />
						<xs:element minOccurs="0" name="lastUpdateTime" type="xs:dateTime" />
						<xs:element minOccurs="0" name="ptaar" type="xs:unsignedByte" />
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
	<xs:complexType name="SearchParametersTC">
		<xs:sequence>
			<xs:element minOccurs="1" name="type" type="xs:string" />
			<xs:element minOccurs="1" name="code" type="xs:string" />
		</xs:sequence>
	</xs:complexType>
	<xs:complexType name="getTzByNTUResponse">
		<xs:sequence>
			<xs:element minOccurs="0" maxOccurs="1" name="searchParameters" type="SearchParametersTypeCode"/>
			<xs:element minOccurs="0" name="extracttz">
				<xs:complexType>
					<xs:sequence>
						<xs:element minOccurs="0" name="extractPreparationTime" type="xs:dateTime" />
						<xs:element minOccurs="0" name="lastUpdateTime" type="xs:dateTime" />
						<xs:element minOccurs="0" name="ptaar" type="xs:unsignedByte" />
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
	<xs:complexType name="SearchParametersTypeCode">
		<xs:sequence>
			<xs:element minOccurs="0" name="type" type="xs:string" />
			<xs:element minOccurs="1" name="code" type="xs:string" />
		</xs:sequence>
	</xs:complexType>
	<xs:complexType name="getTzByLogoResponse">
		<xs:sequence>
			<xs:element minOccurs="0" maxOccurs="1" name="searchParameters" type="SearchParametersCode"/>
			<xs:element minOccurs="0" name="extracttz">
				<xs:complexType>
					<xs:sequence>
						<xs:element minOccurs="0" name="extractPreparationTime" type="xs:dateTime" />
						<xs:element minOccurs="0" name="lastUpdateTime" type="xs:dateTime" />
						<xs:element minOccurs="0" name="ptaar" type="xs:unsignedByte" />
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
	<xs:complexType name="getTzByGNKResponse">
		<xs:sequence>
			<xs:element minOccurs="0" maxOccurs="1" name="searchParameters" type="SearchParametersCode"/>
			<xs:element minOccurs="0" name="extracttz">
				<xs:complexType>
					<xs:sequence>
						<xs:element minOccurs="0" name="extractPreparationTime" type="xs:dateTime" />
						<xs:element minOccurs="0" name="lastUpdateTime" type="xs:dateTime" />
						<xs:element minOccurs="0" name="ptaar" type="xs:unsignedByte" />
						<xs:element minOccurs="0" name="phipoteka" type="xs:unsignedByte" />
					</xs:sequence>
				</xs:complexType>
			</xs:element>
		</xs:sequence>
	</xs:complexType>
	<xs:complexType name="SearchParametersCode">
		<xs:sequence>
			<xs:element minOccurs="1" name="code" type="xs:string" />
		</xs:sequence>
	</xs:complexType>
	<xs:complexType name="getTzByBankAccResponse">
		<xs:sequence>
			<xs:element minOccurs="0" maxOccurs="1" name="searchParameters" type="SearchParameters"/>
			<xs:element minOccurs="0" name="extracttz">
				<xs:complexType>
					<xs:sequence>
						<xs:element minOccurs="0" name="extractPreparationTime" type="xs:dateTime" />
						<xs:element minOccurs="0" name="lastUpdateTime" type="xs:dateTime" />
						<xs:element minOccurs="0" name="ptaar" type="xs:unsignedByte" />
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
	<xs:complexType name="SearchParameters">
		<xs:sequence>
			<xs:element minOccurs="0" name="code1" type="xs:string" />
			<xs:element minOccurs="1" name="code2" type="xs:string" />
		</xs:sequence>
	</xs:complexType>  
	<xs:complexType name="getTzByAirCraftResponse">
		<xs:sequence>
			<xs:element minOccurs="0" maxOccurs="1" name="searchParameters" type="SearchParametersAirCraft"/>
			<xs:element minOccurs="0" name="extracttz">
				<xs:complexType>
					<xs:sequence>
						<xs:element minOccurs="0" name="extractPreparationTime" type="xs:dateTime" />
						<xs:element minOccurs="0" name="lastUpdateTime" type="xs:dateTime" />
						<xs:element minOccurs="0" name="ptaar" type="xs:unsignedByte" />
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
	<xs:complexType name="SearchParametersAirCraft">
		<xs:sequence>
			<xs:element minOccurs="0" name="code1" type="xs:string" />
			<xs:element minOccurs="0" name="code2" type="xs:string" />
		</xs:sequence>
	</xs:complexType>
	<xs:element name="getTzByAirCraftResponse" nillable="true" type="getTzByAirCraftResponse"/>
	<xs:element name="getTzByBankAccResponse" nillable="true" type="getTzByBankAccResponse"/>
	<xs:element name="getTzByGNKResponse" nillable="true" type="getTzByGNKResponse"/>
	<xs:element name="getTzByLogoResponse" nillable="true" type="getTzByLogoResponse"/>
	<xs:element name="getTzByNTUResponse" nillable="true" type="getTzByNTUResponse"/>
	<xs:element name="getTzByPCCODEResponse" nillable="true" type="getTzByPCCODEResponse"/>
	<xs:element name="getTzByTPResponse" nillable="true" type="getTzByTPResponse"/>
	<xs:element name="getTzByTRAResponse" nillable="true" type="getTzByTRAResponse"/>
	<xs:element name="getTzByWagonResponse" nillable="true" type="getTzByWagonResponse"/>
</xs:schema>
    """

    table = """
id | d | r | b | m | property                 | type            | ref | source                                    | prepare | level | access | uri | title | description
   | manifest                                 |                 |     |                                           |         |       |        |     |       |
   |   | resource1                            | xml             |     |                                           |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | SearchParameters             |                 |     | /getTzByAirCraftResponse/searchParameters |         |       |        |     |       |
   |   |   |   |   | code1                    | string          |     | code1/text()                              |         |       |        |     |       |
   |   |   |   |   | code2                    | string          |     | code2/text()                              |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | Extracttz                    |                 |     | /getTzByAirCraftResponse/extracttz        |         |       |        |     |       |
   |   |   |   |   | extract_preparation_time | datetime        |     | extractPreparationTime/text()             |         |       |        |     |       |
   |   |   |   |   | last_update_time         | datetime        |     | lastUpdateTime/text()                     |         |       |        |     |       |
   |   |   |   |   | ptaar                    | integer         |     | ptaar/text()                              |         |       |        |     |       |
   |   |   |   |   | phipoteka                | integer         |     | phipoteka/text()                          |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | Klaida                       |                 |     | /getTzByAirCraftResponse/klaida           |         |       |        |     |       |
   |   |   |   |   | aprasymas                | string          |     | Aprasymas/text()                          |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | SearchParameters1            |                 |     | /getTzByBankAccResponse/searchParameters  |         |       |        |     |       |
   |   |   |   |   | code1                    | string          |     | code1/text()                              |         |       |        |     |       |
   |   |   |   |   | code2                    | string required |     | code2/text()                              |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | Extracttz1                   |                 |     | /getTzByBankAccResponse/extracttz         |         |       |        |     |       |
   |   |   |   |   | extract_preparation_time | datetime        |     | extractPreparationTime/text()             |         |       |        |     |       |
   |   |   |   |   | last_update_time         | datetime        |     | lastUpdateTime/text()                     |         |       |        |     |       |
   |   |   |   |   | ptaar                    | integer         |     | ptaar/text()                              |         |       |        |     |       |
   |   |   |   |   | phipoteka                | integer         |     | phipoteka/text()                          |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | Klaida1                      |                 |     | /getTzByBankAccResponse/klaida            |         |       |        |     |       |
   |   |   |   |   | aprasymas                | string          |     | Aprasymas/text()                          |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | SearchParameters2            |                 |     | /getTzByGNKResponse/searchParameters      |         |       |        |     |       |
   |   |   |   |   | code                     | string required |     | code/text()                               |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | Extracttz2                   |                 |     | /getTzByGNKResponse/extracttz             |         |       |        |     |       |
   |   |   |   |   | extract_preparation_time | datetime        |     | extractPreparationTime/text()             |         |       |        |     |       |
   |   |   |   |   | last_update_time         | datetime        |     | lastUpdateTime/text()                     |         |       |        |     |       |
   |   |   |   |   | ptaar                    | integer         |     | ptaar/text()                              |         |       |        |     |       |
   |   |   |   |   | phipoteka                | integer         |     | phipoteka/text()                          |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | SearchParameters3            |                 |     | /getTzByLogoResponse/searchParameters     |         |       |        |     |       |
   |   |   |   |   | code                     | string required |     | code/text()                               |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | Extracttz3                   |                 |     | /getTzByLogoResponse/extracttz            |         |       |        |     |       |
   |   |   |   |   | extract_preparation_time | datetime        |     | extractPreparationTime/text()             |         |       |        |     |       |
   |   |   |   |   | last_update_time         | datetime        |     | lastUpdateTime/text()                     |         |       |        |     |       |
   |   |   |   |   | ptaar                    | integer         |     | ptaar/text()                              |         |       |        |     |       |
   |   |   |   |   | phipoteka                | integer         |     | phipoteka/text()                          |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | Klaida2                      |                 |     | /getTzByLogoResponse/klaida               |         |       |        |     |       |
   |   |   |   |   | aprasymas                | string          |     | Aprasymas/text()                          |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | SearchParameters4            |                 |     | /getTzByNTUResponse/searchParameters      |         |       |        |     |       |
   |   |   |   |   | type                     | string          |     | type/text()                               |         |       |        |     |       |
   |   |   |   |   | code                     | string required |     | code/text()                               |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | Extracttz4                   |                 |     | /getTzByNTUResponse/extracttz             |         |       |        |     |       |
   |   |   |   |   | extract_preparation_time | datetime        |     | extractPreparationTime/text()             |         |       |        |     |       |
   |   |   |   |   | last_update_time         | datetime        |     | lastUpdateTime/text()                     |         |       |        |     |       |
   |   |   |   |   | ptaar                    | integer         |     | ptaar/text()                              |         |       |        |     |       |
   |   |   |   |   | phipoteka                | integer         |     | phipoteka/text()                          |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | Klaida3                      |                 |     | /getTzByNTUResponse/klaida                |         |       |        |     |       |
   |   |   |   |   | aprasymas                | string          |     | Aprasymas/text()                          |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | SearchParameters5            |                 |     | /getTzByPCCODEResponse/searchParameters   |         |       |        |     |       |
   |   |   |   |   | type                     | string required |     | type/text()                               |         |       |        |     |       |
   |   |   |   |   | code                     | string required |     | code/text()                               |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | Extracttz5                   |                 |     | /getTzByPCCODEResponse/extracttz          |         |       |        |     |       |
   |   |   |   |   | extract_preparation_time | datetime        |     | extractPreparationTime/text()             |         |       |        |     |       |
   |   |   |   |   | last_update_time         | datetime        |     | lastUpdateTime/text()                     |         |       |        |     |       |
   |   |   |   |   | ptaar                    | integer         |     | ptaar/text()                              |         |       |        |     |       |
   |   |   |   |   | phipoteka                | integer         |     | phipoteka/text()                          |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | Klaida4                      |                 |     | /getTzByPCCODEResponse/klaida             |         |       |        |     |       |
   |   |   |   |   | aprasymas                | string          |     | Aprasymas/text()                          |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | SearchParameters6            |                 |     | /getTzByTPResponse/searchParameters       |         |       |        |     |       |
   |   |   |   |   | code                     | string required |     | code/text()                               |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | Extracttz6                   |                 |     | /getTzByTPResponse/extracttz              |         |       |        |     |       |
   |   |   |   |   | extract_preparation_time | datetime        |     | extractPreparationTime/text()             |         |       |        |     |       |
   |   |   |   |   | last_update_time         | datetime        |     | lastUpdateTime/text()                     |         |       |        |     |       |
   |   |   |   |   | ptaar                    | integer         |     | ptaar/text()                              |         |       |        |     |       |
   |   |   |   |   | phipoteka                | integer         |     | phipoteka/text()                          |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | Klaida5                      |                 |     | /getTzByTPResponse/klaida                 |         |       |        |     |       |
   |   |   |   |   | aprasymas                | string          |     | Aprasymas/text()                          |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | SearchParameters7            |                 |     | /getTzByTRAResponse/searchParameters      |         |       |        |     |       |
   |   |   |   |   | code                     | string required |     | code/text()                               |         |       |        |     |       |
   |   |   |   |   | location                 | string          |     | location/text()                           |         |       |        |     |       |
   |   |   |   |   | uni_code                 | string required |     | uniCode/text()                            |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | Extracttz7                   |                 |     | /getTzByTRAResponse/extracttz             |         |       |        |     |       |
   |   |   |   |   | extract_preparation_time | datetime        |     | extractPreparationTime/text()             |         |       |        |     |       |
   |   |   |   |   | last_update_time         | datetime        |     | lastUpdateTime/text()                     |         |       |        |     |       |
   |   |   |   |   | ptaar                    | integer         |     | ptaar/text()                              |         |       |        |     |       |
   |   |   |   |   | phipoteka                | integer         |     | phipoteka/text()                          |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | Klaida6                      |                 |     | /getTzByTRAResponse/klaida                |         |       |        |     |       |
   |   |   |   |   | aprasymas                | string          |     | Aprasymas/text()                          |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | SearchParameters8            |                 |     | /getTzByWagonResponse/searchParameters    |         |       |        |     |       |
   |   |   |   |   | code                     | string required |     | code/text()                               |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | Extracttz8                   |                 |     | /getTzByWagonResponse/extracttz           |         |       |        |     |       |
   |   |   |   |   | extract_preparation_time | datetime        |     | extractPreparationTime/text()             |         |       |        |     |       |
   |   |   |   |   | last_update_time         | datetime        |     | lastUpdateTime/text()                     |         |       |        |     |       |
   |   |   |   |   | ptaar                    | integer         |     | ptaar/text()                              |         |       |        |     |       |
   |   |   |   |   | phipoteka                | integer         |     | phipoteka/text()                          |         |       |        |     |       |
   |                                          |                 |     |                                           |         |       |        |     |       |
   |   |   |   | Klaida7                      |                 |     | /getTzByWagonResponse/klaida              |         |       |        |     |       |
   |   |   |   |   | aprasymas                | string          |     | Aprasymas/text()                          |         |       |        |     |       |
"""
    path = tmp_path / 'manifest.xsd'
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table


def test_xsd_rc717(rc: RawConfig, tmp_path: Path):
    # separate simple type
    xsd = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">

<xs:element name="COVID19_TYRIMAI">
  <xs:annotation><xs:documentation>COVID19 tyrimų duomenys</xs:documentation></xs:annotation>
  <xs:complexType>
    <xs:sequence>
      <xs:element ref="TYRIMAS" minOccurs="0" maxOccurs="unbounded"/>
    </xs:sequence>
  </xs:complexType>
</xs:element>

<xs:element name="TYRIMAS">
  <xs:annotation><xs:documentation></xs:documentation></xs:annotation>
    <xs:complexType>
      <xs:sequence>
      <xs:element minOccurs="1" maxOccurs="1" ref="CT_ID" />
      <xs:element minOccurs="1" maxOccurs="1" ref="CT_IRASO_DATA" />
      <xs:element minOccurs="1" maxOccurs="1" ref="CT_E200_FC_ID" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_E200ATS_FC_ID" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_E200_FORMA" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_E200_ISTG_ESPBI_ID" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_E200_ISTG_SVEIDRA_ID" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_E200_ISTAIGA" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_E200_ISTG_TLK_ID" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_E200_ISTG_SAV_ID" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_E200_SAV_PAV" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_E200_SUKURIMO_DATA" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_E200_DUOMENU_BUKLE" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_E200_DOK_PASIRASYMAS" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_E200ATS_DUOM_SUKURTI" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_E200ATS_DUOMENU_BUKLE" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_E200ATS_FORMA" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_E200ATS_ISTAIGA" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_E200ATS_DOK_PASIRASYMAS" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_PACIENTO_AMZIUS" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_PACIENTO_AK" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_PACIENTO_VARDAS" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_PACIENTO_PAVARDE" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_PACIENTO_SPI_SVEIDRA_ID" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_PACIENTO_ESI" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_PACIENTO_APSKR_ID" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_PACIENTO_SAV_ID" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_PACIENTO_SAVIVALDYBE" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_PACIENTO_SPI" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_PACIENTO_GYDYTOJO_VARDAS" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_PACIENTO_GYDYTOJO_PAVARDE" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_PACIENTO_GYDYTOJO_SPAUDAS" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_E200ATS_TYRIMO_METODAS" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_E200ATS_TYRIMO_REZULTATAS" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_E200ATS_REZULTATAS" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_TYRIMO_TRUKME" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_CTD_TYRIMAS_UZSAKYTAS" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_CTD_EMINYS_PAIMTAS" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_CTD_EMINIO_REG_KODAS" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_CTD_EMINYS_GAUTAS" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_CTD_TYRIMAS_ATLIKTAS" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_SVEIKATOS_SPECIALISTAS" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_EV_KODAS" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_TELEFONO_NR" />
      <xs:element minOccurs="0" maxOccurs="1" ref="CT_TYRIMO_TIPO_ID" />
    </xs:sequence>
  </xs:complexType>
</xs:element>

<xs:element name="CT_ID" type="xs:long">
  <xs:annotation><xs:documentation>Lentelės įrašų identifikatorius, pirminis raktas</xs:documentation></xs:annotation>
</xs:element>

<xs:element name="CT_IRASO_DATA" type="data_laikas">
  <xs:annotation><xs:documentation>Įrašo sukūrimo šioje lentelėje data</xs:documentation></xs:annotation>
</xs:element>

<xs:element name="CT_E200_FC_ID" type="xs:long">
  <xs:annotation><xs:documentation>E200 duomenų kompozicijos unikalus identifikatorius</xs:documentation></xs:annotation>
</xs:element>

<xs:element name="CT_E200ATS_FC_ID" type="xs:long">
  <xs:annotation><xs:documentation>E200-ats duomenų kompozicijos unikalus identifikatorius</xs:documentation></xs:annotation>
</xs:element>

<xs:element name="CT_E200_FORMA">
  <xs:annotation><xs:documentation>E200 medicininės formos pavadinimas</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="4"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_E200_ISTG_ESPBI_ID" type="xs:string">
  <xs:annotation><xs:documentation>Įstaigos, suformavusios tyrimą (E200), ESPBI identifikatorius (resurso ID)</xs:documentation></xs:annotation>
</xs:element>

<xs:element name="CT_E200_ISTG_SVEIDRA_ID" type="xs:string">
  <xs:annotation><xs:documentation>Įstaigos, suformavusios tyrimą (E200), SVEIDRA identifikatorius</xs:documentation></xs:annotation>
</xs:element>

<xs:element name="CT_E200_ISTAIGA">
  <xs:annotation><xs:documentation>Įstaiga, suformavusi tyrimo (E200) paėmimo duomenis</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="1024"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_E200_ISTG_TLK_ID">
  <xs:annotation><xs:documentation>Įstaigos, suformavusios tyrimą (E200), apskrities identifikatorius</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:int">
      <xs:totalDigits value="5"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_E200_ISTG_SAV_ID">
  <xs:annotation><xs:documentation>Įstaigos, suformavusios tyrimą (E200) savivaldybės identifikatorius (ne iš Adresų registro)</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:int">
      <xs:totalDigits value="10"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_E200_SAV_PAV">
  <xs:annotation><xs:documentation>Įstaigos, suformavusios tyrimą (E200) savivaldybės pavadinimas</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="250"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_E200_SUKURIMO_DATA" type="data_laikas">
  <xs:annotation><xs:documentation>E200 duomenų sukūrimo data ir laikas</xs:documentation></xs:annotation>
</xs:element>

<xs:element name="CT_E200_DUOMENU_BUKLE">
  <xs:annotation><xs:documentation>E200  duomenų įvedimo užbaigtumas</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="30"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_E200_DOK_PASIRASYMAS">
  <xs:annotation><xs:documentation>E200  dokumento pasirašymas. final - suformuotas, bet nepasirašytas, signed - suformuotas ir pasirašytas</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="30"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_E200ATS_DUOM_SUKURTI" type="data_laikas">
  <xs:annotation><xs:documentation>E200-ats duomenų sukūrimo data ir laikas</xs:documentation></xs:annotation>
</xs:element>

<xs:element name="CT_E200ATS_DUOMENU_BUKLE">
  <xs:annotation><xs:documentation>E200-ats  duomenų įvedimo užbaigtumas</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="30"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_E200ATS_FORMA">
  <xs:annotation><xs:documentation>E200-ats medicininės formos pavadinimas</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="8"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_E200ATS_ISTAIGA">
  <xs:annotation><xs:documentation>Įstaiga, suformavusi tyrimo (E200-ats) atsakymo duomenis</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="1024"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_E200ATS_DOK_PASIRASYMAS">
  <xs:annotation><xs:documentation>E200-ats dokumento pasirašymas. final - suformuotas, bet nepasirašytas, signed -  suformuotas ir pasirašytas</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="30"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_PACIENTO_AMZIUS">
  <xs:annotation><xs:documentation>Neredaguojamas laukas. Išskaičiuojamas dinamiškai</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:int">
      <xs:totalDigits value="5"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_PACIENTO_AK">
  <xs:annotation><xs:documentation>Paciento asmens kodas</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="30"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_PACIENTO_VARDAS">
  <xs:annotation><xs:documentation>Paciento vardas</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="93"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_PACIENTO_PAVARDE">
  <xs:annotation><xs:documentation>Paciento pavardė</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="93"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_PACIENTO_SPI_SVEIDRA_ID">
  <xs:annotation><xs:documentation>Paciento prisirašymo įstaigos SVEIDRA identifikatorius</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:int">
      <xs:totalDigits value="10"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_PACIENTO_ESI">
  <xs:annotation><xs:documentation>Paciento ESI numeris</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string"><xs:maxLength value="1024"/></xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_PACIENTO_APSKR_ID">
  <xs:annotation><xs:documentation>Paciento prisirašymo įstaigos apskrities identifikatorius</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:int">
      <xs:totalDigits value="5"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_PACIENTO_SAV_ID">
  <xs:annotation><xs:documentation>Paciento prisirašymo įstaigos savivaldybės identifikatorius (ne iš Adresų registro)</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:int">
      <xs:totalDigits value="10"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_PACIENTO_SAVIVALDYBE">
  <xs:annotation><xs:documentation>Paciento prisirašymo įstaigos savivaldybės pavadinimas</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="250"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_PACIENTO_SPI">
  <xs:annotation><xs:documentation>Paciento prisirašymo įstaigos pavadinimas</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="300"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_PACIENTO_GYDYTOJO_VARDAS">
  <xs:annotation><xs:documentation>Paciento prisirašymo įstaigoje šeimos gydytojo vardas</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="45"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_PACIENTO_GYDYTOJO_PAVARDE">
  <xs:annotation><xs:documentation>Paciento prisirašymo įstaigoje šeimos gydytojo pavardė</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="45"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_PACIENTO_GYDYTOJO_SPAUDAS">
  <xs:annotation><xs:documentation>Paciento prisirašymo įstaigoje šeimos gydytojo spaudo numeris</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="12"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_E200ATS_TYRIMO_METODAS">
  <xs:annotation><xs:documentation>Laboratorijos naudoto metodo tyrimui pavadinimas</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="200"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_E200ATS_TYRIMO_REZULTATAS">
  <xs:annotation><xs:documentation>Laboratorijos tyrimo rezultato duomuo E200-ats dokumente</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="1000"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_E200ATS_REZULTATAS">
  <xs:annotation><xs:documentation>Neredaguojamas laukas. Jame priklausomai nuo reikšmės lauke CT_E200ATS_TYRIMO_REZULTATAS rodomas 0 jei tyrimo rezultatas neigiamas, 1 - jei teigiamas</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:int">
      <xs:totalDigits value="2"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_TYRIMO_TRUKME" type="xs:decimal">
  <xs:annotation><xs:documentation>Tyrimo trukmė valandomis, skaičiuojama tarp E200 ir E200-ats duomenų įvedimo datų</xs:documentation></xs:annotation>
</xs:element>

<xs:element name="CT_CTD_TYRIMAS_UZSAKYTAS" type="data_laikas">
  <xs:annotation><xs:documentation>Tyrimo užsakymo data</xs:documentation></xs:annotation>
</xs:element>

<xs:element name="CT_CTD_EMINYS_PAIMTAS" type="data_laikas">
  <xs:annotation><xs:documentation>Ėminio paėmimo data</xs:documentation></xs:annotation>
</xs:element>

<xs:element name="CT_CTD_EMINIO_REG_KODAS">
  <xs:annotation><xs:documentation>Ėminio registravimo kodas</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="1024"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_CTD_EMINYS_GAUTAS" type="data_laikas">
  <xs:annotation><xs:documentation>Ėminio gavimo data</xs:documentation></xs:annotation>
</xs:element>

<xs:element name="CT_CTD_TYRIMAS_ATLIKTAS" type="data_laikas">
  <xs:annotation><xs:documentation>Tyrimo atlikimo data</xs:documentation></xs:annotation>
</xs:element>

<xs:element name="CT_SVEIKATOS_SPECIALISTAS" type="xs:int">
  <xs:annotation><xs:documentation>Požymis ar asmuo yra sveikatos specialistas</xs:documentation></xs:annotation>
</xs:element>

<xs:element name="CT_EV_KODAS">
  <xs:annotation><xs:documentation>Paciento darboviečių ekonominės veiklos kodai</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="2000"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_TELEFONO_NR">
  <xs:annotation><xs:documentation>Paciento telefono numeris(-ai)</xs:documentation></xs:annotation>
  <xs:simpleType>
    <xs:restriction base="xs:string">
      <xs:maxLength value="2000"/>
    </xs:restriction>
  </xs:simpleType>
</xs:element>

<xs:element name="CT_TYRIMO_TIPO_ID" type="xs:long">
  <xs:annotation><xs:documentation>Tyrimo tipo identifikatorius</xs:documentation></xs:annotation>
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
id | d | r | b | m | property                     | type             | ref            | source                              | prepare | level | access | uri                                           | title | description
   | manifest                                     |                  |                |                                     |         |       |        |                                               |       |
   |   | resource1                                | xml              |                |                                     |         |       |        |                                               |       |
   |                                              |                  |                |                                     |         |       |        |                                               |       |
   |   |   |   | Resource                         |                  |                | /                                   |         |       |        | http://www.w3.org/2000/01/rdf-schema#Resource |       | Įvairūs duomenys
   |   |   |   |   | ct_id                        | integer          |                | CT_ID/text()                        |         |       |        |                                               |       | Lentelės įrašų identifikatorius, pirminis raktas
   |   |   |   |   | ct_iraso_data                | string           |                | CT_IRASO_DATA/text()                |         |       |        |                                               |       | Įrašo sukūrimo šioje lentelėje data
   |   |   |   |   | ct_e200_fc_id                | integer          |                | CT_E200_FC_ID/text()                |         |       |        |                                               |       | E200 duomenų kompozicijos unikalus identifikatorius
   |   |   |   |   | ct_e200ats_fc_id             | integer          |                | CT_E200ATS_FC_ID/text()             |         |       |        |                                               |       | E200-ats duomenų kompozicijos unikalus identifikatorius
   |   |   |   |   | ct_e200_forma                | string           |                | CT_E200_FORMA/text()                |         |       |        |                                               |       | E200 medicininės formos pavadinimas
   |   |   |   |   | ct_e200_istg_espbi_id        | string           |                | CT_E200_ISTG_ESPBI_ID/text()        |         |       |        |                                               |       | Įstaigos, suformavusios tyrimą (E200), ESPBI identifikatorius (resurso ID)
   |   |   |   |   | ct_e200_istg_sveidra_id      | string           |                | CT_E200_ISTG_SVEIDRA_ID/text()      |         |       |        |                                               |       | Įstaigos, suformavusios tyrimą (E200), SVEIDRA identifikatorius
   |   |   |   |   | ct_e200_istaiga              | string           |                | CT_E200_ISTAIGA/text()              |         |       |        |                                               |       | Įstaiga, suformavusi tyrimo (E200) paėmimo duomenis
   |   |   |   |   | ct_e200_istg_tlk_id          | integer          |                | CT_E200_ISTG_TLK_ID/text()          |         |       |        |                                               |       | Įstaigos, suformavusios tyrimą (E200), apskrities identifikatorius
   |   |   |   |   | ct_e200_istg_sav_id          | integer          |                | CT_E200_ISTG_SAV_ID/text()          |         |       |        |                                               |       | Įstaigos, suformavusios tyrimą (E200) savivaldybės identifikatorius (ne iš Adresų registro)
   |   |   |   |   | ct_e200_sav_pav              | string           |                | CT_E200_SAV_PAV/text()              |         |       |        |                                               |       | Įstaigos, suformavusios tyrimą (E200) savivaldybės pavadinimas
   |   |   |   |   | ct_e200_sukurimo_data        | string           |                | CT_E200_SUKURIMO_DATA/text()        |         |       |        |                                               |       | E200 duomenų sukūrimo data ir laikas
   |   |   |   |   | ct_e200_duomenu_bukle        | string           |                | CT_E200_DUOMENU_BUKLE/text()        |         |       |        |                                               |       | E200  duomenų įvedimo užbaigtumas
   |   |   |   |   | ct_e200_dok_pasirasymas      | string           |                | CT_E200_DOK_PASIRASYMAS/text()      |         |       |        |                                               |       | E200  dokumento pasirašymas. final - suformuotas, bet nepasirašytas, signed - suformuotas ir pasirašytas
   |   |   |   |   | ct_e200ats_duom_sukurti      | string           |                | CT_E200ATS_DUOM_SUKURTI/text()      |         |       |        |                                               |       | E200-ats duomenų sukūrimo data ir laikas
   |   |   |   |   | ct_e200ats_duomenu_bukle     | string           |                | CT_E200ATS_DUOMENU_BUKLE/text()     |         |       |        |                                               |       | E200-ats  duomenų įvedimo užbaigtumas
   |   |   |   |   | ct_e200ats_forma             | string           |                | CT_E200ATS_FORMA/text()             |         |       |        |                                               |       | E200-ats medicininės formos pavadinimas
   |   |   |   |   | ct_e200ats_istaiga           | string           |                | CT_E200ATS_ISTAIGA/text()           |         |       |        |                                               |       | Įstaiga, suformavusi tyrimo (E200-ats) atsakymo duomenis
   |   |   |   |   | ct_e200ats_dok_pasirasymas   | string           |                | CT_E200ATS_DOK_PASIRASYMAS/text()   |         |       |        |                                               |       | E200-ats dokumento pasirašymas. final - suformuotas, bet nepasirašytas, signed -  suformuotas ir pasirašytas
   |   |   |   |   | ct_paciento_amzius           | integer          |                | CT_PACIENTO_AMZIUS/text()           |         |       |        |                                               |       | Neredaguojamas laukas. Išskaičiuojamas dinamiškai
   |   |   |   |   | ct_paciento_ak               | string           |                | CT_PACIENTO_AK/text()               |         |       |        |                                               |       | Paciento asmens kodas
   |   |   |   |   | ct_paciento_vardas           | string           |                | CT_PACIENTO_VARDAS/text()           |         |       |        |                                               |       | Paciento vardas
   |   |   |   |   | ct_paciento_pavarde          | string           |                | CT_PACIENTO_PAVARDE/text()          |         |       |        |                                               |       | Paciento pavardė
   |   |   |   |   | ct_paciento_spi_sveidra_id   | integer          |                | CT_PACIENTO_SPI_SVEIDRA_ID/text()   |         |       |        |                                               |       | Paciento prisirašymo įstaigos SVEIDRA identifikatorius
   |   |   |   |   | ct_paciento_esi              | string           |                | CT_PACIENTO_ESI/text()              |         |       |        |                                               |       | Paciento ESI numeris
   |   |   |   |   | ct_paciento_apskr_id         | integer          |                | CT_PACIENTO_APSKR_ID/text()         |         |       |        |                                               |       | Paciento prisirašymo įstaigos apskrities identifikatorius
   |   |   |   |   | ct_paciento_sav_id           | integer          |                | CT_PACIENTO_SAV_ID/text()           |         |       |        |                                               |       | Paciento prisirašymo įstaigos savivaldybės identifikatorius (ne iš Adresų registro)
   |   |   |   |   | ct_paciento_savivaldybe      | string           |                | CT_PACIENTO_SAVIVALDYBE/text()      |         |       |        |                                               |       | Paciento prisirašymo įstaigos savivaldybės pavadinimas
   |   |   |   |   | ct_paciento_spi              | string           |                | CT_PACIENTO_SPI/text()              |         |       |        |                                               |       | Paciento prisirašymo įstaigos pavadinimas
   |   |   |   |   | ct_paciento_gydytojo_vardas  | string           |                | CT_PACIENTO_GYDYTOJO_VARDAS/text()  |         |       |        |                                               |       | Paciento prisirašymo įstaigoje šeimos gydytojo vardas
   |   |   |   |   | ct_paciento_gydytojo_pavarde | string           |                | CT_PACIENTO_GYDYTOJO_PAVARDE/text() |         |       |        |                                               |       | Paciento prisirašymo įstaigoje šeimos gydytojo pavardė
   |   |   |   |   | ct_paciento_gydytojo_spaudas | string           |                | CT_PACIENTO_GYDYTOJO_SPAUDAS/text() |         |       |        |                                               |       | Paciento prisirašymo įstaigoje šeimos gydytojo spaudo numeris
   |   |   |   |   | ct_e200ats_tyrimo_metodas    | string           |                | CT_E200ATS_TYRIMO_METODAS/text()    |         |       |        |                                               |       | Laboratorijos naudoto metodo tyrimui pavadinimas
   |   |   |   |   | ct_e200ats_tyrimo_rezultatas | string           |                | CT_E200ATS_TYRIMO_REZULTATAS/text() |         |       |        |                                               |       | Laboratorijos tyrimo rezultato duomuo E200-ats dokumente
   |   |   |   |   | ct_e200ats_rezultatas        | integer          |                | CT_E200ATS_REZULTATAS/text()        |         |       |        |                                               |       | Neredaguojamas laukas. Jame priklausomai nuo reikšmės lauke CT_E200ATS_TYRIMO_REZULTATAS rodomas 0 jei tyrimo rezultatas neigiamas, 1 - jei teigiamas
   |   |   |   |   | ct_tyrimo_trukme             | number           |                | CT_TYRIMO_TRUKME/text()             |         |       |        |                                               |       | Tyrimo trukmė valandomis, skaičiuojama tarp E200 ir E200-ats duomenų įvedimo datų
   |   |   |   |   | ct_ctd_tyrimas_uzsakytas     | string           |                | CT_CTD_TYRIMAS_UZSAKYTAS/text()     |         |       |        |                                               |       | Tyrimo užsakymo data
   |   |   |   |   | ct_ctd_eminys_paimtas        | string           |                | CT_CTD_EMINYS_PAIMTAS/text()        |         |       |        |                                               |       | Ėminio paėmimo data
   |   |   |   |   | ct_ctd_eminio_reg_kodas      | string           |                | CT_CTD_EMINIO_REG_KODAS/text()      |         |       |        |                                               |       | Ėminio registravimo kodas
   |   |   |   |   | ct_ctd_eminys_gautas         | string           |                | CT_CTD_EMINYS_GAUTAS/text()         |         |       |        |                                               |       | Ėminio gavimo data
   |   |   |   |   | ct_ctd_tyrimas_atliktas      | string           |                | CT_CTD_TYRIMAS_ATLIKTAS/text()      |         |       |        |                                               |       | Tyrimo atlikimo data
   |   |   |   |   | ct_sveikatos_specialistas    | integer          |                | CT_SVEIKATOS_SPECIALISTAS/text()    |         |       |        |                                               |       | Požymis ar asmuo yra sveikatos specialistas
   |   |   |   |   | ct_ev_kodas                  | string           |                | CT_EV_KODAS/text()                  |         |       |        |                                               |       | Paciento darboviečių ekonominės veiklos kodai
   |   |   |   |   | ct_telefono_nr               | string           |                | CT_TELEFONO_NR/text()               |         |       |        |                                               |       | Paciento telefono numeris(-ai)
   |   |   |   |   | ct_tyrimo_tipo_id            | integer          |                | CT_TYRIMO_TIPO_ID/text()            |         |       |        |                                               |       | Tyrimo tipo identifikatorius
   |                                              |                  |                |                                     |         |       |        |                                               |       |
   |   |   |   | Tyrimas                          |                  |                | /COVID19_TYRIMAI/TYRIMAS            |         |       |        |                                               |       |
   |   |   |   |   | covid19_tyrimai              | ref              | Covid19Tyrimai |                                     |         |       |        |                                               |       |
   |   |   |   |   | ct_id                        | integer required |                | CT_ID/text()                        |         |       |        |                                               |       | Lentelės įrašų identifikatorius, pirminis raktas
   |   |   |   |   | ct_iraso_data                | string required  |                | CT_IRASO_DATA/text()                |         |       |        |                                               |       | Įrašo sukūrimo šioje lentelėje data
   |   |   |   |   | ct_e200_fc_id                | integer required |                | CT_E200_FC_ID/text()                |         |       |        |                                               |       | E200 duomenų kompozicijos unikalus identifikatorius
   |   |   |   |   | ct_e200ats_fc_id             | integer          |                | CT_E200ATS_FC_ID/text()             |         |       |        |                                               |       | E200-ats duomenų kompozicijos unikalus identifikatorius
   |   |   |   |   | ct_e200_forma                | string           |                | CT_E200_FORMA/text()                |         |       |        |                                               |       | E200 medicininės formos pavadinimas
   |   |   |   |   | ct_e200_istg_espbi_id        | string           |                | CT_E200_ISTG_ESPBI_ID/text()        |         |       |        |                                               |       | Įstaigos, suformavusios tyrimą (E200), ESPBI identifikatorius (resurso ID)
   |   |   |   |   | ct_e200_istg_sveidra_id      | string           |                | CT_E200_ISTG_SVEIDRA_ID/text()      |         |       |        |                                               |       | Įstaigos, suformavusios tyrimą (E200), SVEIDRA identifikatorius
   |   |   |   |   | ct_e200_istaiga              | string           |                | CT_E200_ISTAIGA/text()              |         |       |        |                                               |       | Įstaiga, suformavusi tyrimo (E200) paėmimo duomenis
   |   |   |   |   | ct_e200_istg_tlk_id          | integer          |                | CT_E200_ISTG_TLK_ID/text()          |         |       |        |                                               |       | Įstaigos, suformavusios tyrimą (E200), apskrities identifikatorius
   |   |   |   |   | ct_e200_istg_sav_id          | integer          |                | CT_E200_ISTG_SAV_ID/text()          |         |       |        |                                               |       | Įstaigos, suformavusios tyrimą (E200) savivaldybės identifikatorius (ne iš Adresų registro)
   |   |   |   |   | ct_e200_sav_pav              | string           |                | CT_E200_SAV_PAV/text()              |         |       |        |                                               |       | Įstaigos, suformavusios tyrimą (E200) savivaldybės pavadinimas
   |   |   |   |   | ct_e200_sukurimo_data        | string           |                | CT_E200_SUKURIMO_DATA/text()        |         |       |        |                                               |       | E200 duomenų sukūrimo data ir laikas
   |   |   |   |   | ct_e200_duomenu_bukle        | string           |                | CT_E200_DUOMENU_BUKLE/text()        |         |       |        |                                               |       | E200  duomenų įvedimo užbaigtumas
   |   |   |   |   | ct_e200_dok_pasirasymas      | string           |                | CT_E200_DOK_PASIRASYMAS/text()      |         |       |        |                                               |       | E200  dokumento pasirašymas. final - suformuotas, bet nepasirašytas, signed - suformuotas ir pasirašytas
   |   |   |   |   | ct_e200ats_duom_sukurti      | string           |                | CT_E200ATS_DUOM_SUKURTI/text()      |         |       |        |                                               |       | E200-ats duomenų sukūrimo data ir laikas
   |   |   |   |   | ct_e200ats_duomenu_bukle     | string           |                | CT_E200ATS_DUOMENU_BUKLE/text()     |         |       |        |                                               |       | E200-ats  duomenų įvedimo užbaigtumas
   |   |   |   |   | ct_e200ats_forma             | string           |                | CT_E200ATS_FORMA/text()             |         |       |        |                                               |       | E200-ats medicininės formos pavadinimas
   |   |   |   |   | ct_e200ats_istaiga           | string           |                | CT_E200ATS_ISTAIGA/text()           |         |       |        |                                               |       | Įstaiga, suformavusi tyrimo (E200-ats) atsakymo duomenis
   |   |   |   |   | ct_e200ats_dok_pasirasymas   | string           |                | CT_E200ATS_DOK_PASIRASYMAS/text()   |         |       |        |                                               |       | E200-ats dokumento pasirašymas. final - suformuotas, bet nepasirašytas, signed -  suformuotas ir pasirašytas
   |   |   |   |   | ct_paciento_amzius           | integer          |                | CT_PACIENTO_AMZIUS/text()           |         |       |        |                                               |       | Neredaguojamas laukas. Išskaičiuojamas dinamiškai
   |   |   |   |   | ct_paciento_ak               | string           |                | CT_PACIENTO_AK/text()               |         |       |        |                                               |       | Paciento asmens kodas
   |   |   |   |   | ct_paciento_vardas           | string           |                | CT_PACIENTO_VARDAS/text()           |         |       |        |                                               |       | Paciento vardas
   |   |   |   |   | ct_paciento_pavarde          | string           |                | CT_PACIENTO_PAVARDE/text()          |         |       |        |                                               |       | Paciento pavardė
   |   |   |   |   | ct_paciento_spi_sveidra_id   | integer          |                | CT_PACIENTO_SPI_SVEIDRA_ID/text()   |         |       |        |                                               |       | Paciento prisirašymo įstaigos SVEIDRA identifikatorius
   |   |   |   |   | ct_paciento_esi              | string           |                | CT_PACIENTO_ESI/text()              |         |       |        |                                               |       | Paciento ESI numeris
   |   |   |   |   | ct_paciento_apskr_id         | integer          |                | CT_PACIENTO_APSKR_ID/text()         |         |       |        |                                               |       | Paciento prisirašymo įstaigos apskrities identifikatorius
   |   |   |   |   | ct_paciento_sav_id           | integer          |                | CT_PACIENTO_SAV_ID/text()           |         |       |        |                                               |       | Paciento prisirašymo įstaigos savivaldybės identifikatorius (ne iš Adresų registro)
   |   |   |   |   | ct_paciento_savivaldybe      | string           |                | CT_PACIENTO_SAVIVALDYBE/text()      |         |       |        |                                               |       | Paciento prisirašymo įstaigos savivaldybės pavadinimas
   |   |   |   |   | ct_paciento_spi              | string           |                | CT_PACIENTO_SPI/text()              |         |       |        |                                               |       | Paciento prisirašymo įstaigos pavadinimas
   |   |   |   |   | ct_paciento_gydytojo_vardas  | string           |                | CT_PACIENTO_GYDYTOJO_VARDAS/text()  |         |       |        |                                               |       | Paciento prisirašymo įstaigoje šeimos gydytojo vardas
   |   |   |   |   | ct_paciento_gydytojo_pavarde | string           |                | CT_PACIENTO_GYDYTOJO_PAVARDE/text() |         |       |        |                                               |       | Paciento prisirašymo įstaigoje šeimos gydytojo pavardė
   |   |   |   |   | ct_paciento_gydytojo_spaudas | string           |                | CT_PACIENTO_GYDYTOJO_SPAUDAS/text() |         |       |        |                                               |       | Paciento prisirašymo įstaigoje šeimos gydytojo spaudo numeris
   |   |   |   |   | ct_e200ats_tyrimo_metodas    | string           |                | CT_E200ATS_TYRIMO_METODAS/text()    |         |       |        |                                               |       | Laboratorijos naudoto metodo tyrimui pavadinimas
   |   |   |   |   | ct_e200ats_tyrimo_rezultatas | string           |                | CT_E200ATS_TYRIMO_REZULTATAS/text() |         |       |        |                                               |       | Laboratorijos tyrimo rezultato duomuo E200-ats dokumente
   |   |   |   |   | ct_e200ats_rezultatas        | integer          |                | CT_E200ATS_REZULTATAS/text()        |         |       |        |                                               |       | Neredaguojamas laukas. Jame priklausomai nuo reikšmės lauke CT_E200ATS_TYRIMO_REZULTATAS rodomas 0 jei tyrimo rezultatas neigiamas, 1 - jei teigiamas
   |   |   |   |   | ct_tyrimo_trukme             | number           |                | CT_TYRIMO_TRUKME/text()             |         |       |        |                                               |       | Tyrimo trukmė valandomis, skaičiuojama tarp E200 ir E200-ats duomenų įvedimo datų
   |   |   |   |   | ct_ctd_tyrimas_uzsakytas     | string           |                | CT_CTD_TYRIMAS_UZSAKYTAS/text()     |         |       |        |                                               |       | Tyrimo užsakymo data
   |   |   |   |   | ct_ctd_eminys_paimtas        | string           |                | CT_CTD_EMINYS_PAIMTAS/text()        |         |       |        |                                               |       | Ėminio paėmimo data
   |   |   |   |   | ct_ctd_eminio_reg_kodas      | string           |                | CT_CTD_EMINIO_REG_KODAS/text()      |         |       |        |                                               |       | Ėminio registravimo kodas
   |   |   |   |   | ct_ctd_eminys_gautas         | string           |                | CT_CTD_EMINYS_GAUTAS/text()         |         |       |        |                                               |       | Ėminio gavimo data
   |   |   |   |   | ct_ctd_tyrimas_atliktas      | string           |                | CT_CTD_TYRIMAS_ATLIKTAS/text()      |         |       |        |                                               |       | Tyrimo atlikimo data
   |   |   |   |   | ct_sveikatos_specialistas    | integer          |                | CT_SVEIKATOS_SPECIALISTAS/text()    |         |       |        |                                               |       | Požymis ar asmuo yra sveikatos specialistas
   |   |   |   |   | ct_ev_kodas                  | string           |                | CT_EV_KODAS/text()                  |         |       |        |                                               |       | Paciento darboviečių ekonominės veiklos kodai
   |   |   |   |   | ct_telefono_nr               | string           |                | CT_TELEFONO_NR/text()               |         |       |        |                                               |       | Paciento telefono numeris(-ai)
   |   |   |   |   | ct_tyrimo_tipo_id            | integer          |                | CT_TYRIMO_TIPO_ID/text()            |         |       |        |                                               |       | Tyrimo tipo identifikatorius
   |                                              |                  |                |                                     |         |       |        |                                               |       |
   |   |   |   | Covid19Tyrimai                   |                  |                | /COVID19_TYRIMAI                    |         |       |        |                                               |       | COVID19 tyrimų duomenys
   |   |   |   |   | tyrimas[]                    | backref          | Tyrimas        |                                     |         |       |        |                                               |       |
   |                                              |                  |                |                                     |         |       |        |                                               |       |
   |   |   |   | Tyrimas1                         |                  |                | /TYRIMAS                            |         |       |        |                                               |       |
   |   |   |   |   | ct_id                        | integer required |                | CT_ID/text()                        |         |       |        |                                               |       | Lentelės įrašų identifikatorius, pirminis raktas
   |   |   |   |   | ct_iraso_data                | string required  |                | CT_IRASO_DATA/text()                |         |       |        |                                               |       | Įrašo sukūrimo šioje lentelėje data
   |   |   |   |   | ct_e200_fc_id                | integer required |                | CT_E200_FC_ID/text()                |         |       |        |                                               |       | E200 duomenų kompozicijos unikalus identifikatorius
   |   |   |   |   | ct_e200ats_fc_id             | integer          |                | CT_E200ATS_FC_ID/text()             |         |       |        |                                               |       | E200-ats duomenų kompozicijos unikalus identifikatorius
   |   |   |   |   | ct_e200_forma                | string           |                | CT_E200_FORMA/text()                |         |       |        |                                               |       | E200 medicininės formos pavadinimas
   |   |   |   |   | ct_e200_istg_espbi_id        | string           |                | CT_E200_ISTG_ESPBI_ID/text()        |         |       |        |                                               |       | Įstaigos, suformavusios tyrimą (E200), ESPBI identifikatorius (resurso ID)
   |   |   |   |   | ct_e200_istg_sveidra_id      | string           |                | CT_E200_ISTG_SVEIDRA_ID/text()      |         |       |        |                                               |       | Įstaigos, suformavusios tyrimą (E200), SVEIDRA identifikatorius
   |   |   |   |   | ct_e200_istaiga              | string           |                | CT_E200_ISTAIGA/text()              |         |       |        |                                               |       | Įstaiga, suformavusi tyrimo (E200) paėmimo duomenis
   |   |   |   |   | ct_e200_istg_tlk_id          | integer          |                | CT_E200_ISTG_TLK_ID/text()          |         |       |        |                                               |       | Įstaigos, suformavusios tyrimą (E200), apskrities identifikatorius
   |   |   |   |   | ct_e200_istg_sav_id          | integer          |                | CT_E200_ISTG_SAV_ID/text()          |         |       |        |                                               |       | Įstaigos, suformavusios tyrimą (E200) savivaldybės identifikatorius (ne iš Adresų registro)
   |   |   |   |   | ct_e200_sav_pav              | string           |                | CT_E200_SAV_PAV/text()              |         |       |        |                                               |       | Įstaigos, suformavusios tyrimą (E200) savivaldybės pavadinimas
   |   |   |   |   | ct_e200_sukurimo_data        | string           |                | CT_E200_SUKURIMO_DATA/text()        |         |       |        |                                               |       | E200 duomenų sukūrimo data ir laikas
   |   |   |   |   | ct_e200_duomenu_bukle        | string           |                | CT_E200_DUOMENU_BUKLE/text()        |         |       |        |                                               |       | E200  duomenų įvedimo užbaigtumas
   |   |   |   |   | ct_e200_dok_pasirasymas      | string           |                | CT_E200_DOK_PASIRASYMAS/text()      |         |       |        |                                               |       | E200  dokumento pasirašymas. final - suformuotas, bet nepasirašytas, signed - suformuotas ir pasirašytas
   |   |   |   |   | ct_e200ats_duom_sukurti      | string           |                | CT_E200ATS_DUOM_SUKURTI/text()      |         |       |        |                                               |       | E200-ats duomenų sukūrimo data ir laikas
   |   |   |   |   | ct_e200ats_duomenu_bukle     | string           |                | CT_E200ATS_DUOMENU_BUKLE/text()     |         |       |        |                                               |       | E200-ats  duomenų įvedimo užbaigtumas
   |   |   |   |   | ct_e200ats_forma             | string           |                | CT_E200ATS_FORMA/text()             |         |       |        |                                               |       | E200-ats medicininės formos pavadinimas
   |   |   |   |   | ct_e200ats_istaiga           | string           |                | CT_E200ATS_ISTAIGA/text()           |         |       |        |                                               |       | Įstaiga, suformavusi tyrimo (E200-ats) atsakymo duomenis
   |   |   |   |   | ct_e200ats_dok_pasirasymas   | string           |                | CT_E200ATS_DOK_PASIRASYMAS/text()   |         |       |        |                                               |       | E200-ats dokumento pasirašymas. final - suformuotas, bet nepasirašytas, signed -  suformuotas ir pasirašytas
   |   |   |   |   | ct_paciento_amzius           | integer          |                | CT_PACIENTO_AMZIUS/text()           |         |       |        |                                               |       | Neredaguojamas laukas. Išskaičiuojamas dinamiškai
   |   |   |   |   | ct_paciento_ak               | string           |                | CT_PACIENTO_AK/text()               |         |       |        |                                               |       | Paciento asmens kodas
   |   |   |   |   | ct_paciento_vardas           | string           |                | CT_PACIENTO_VARDAS/text()           |         |       |        |                                               |       | Paciento vardas
   |   |   |   |   | ct_paciento_pavarde          | string           |                | CT_PACIENTO_PAVARDE/text()          |         |       |        |                                               |       | Paciento pavardė
   |   |   |   |   | ct_paciento_spi_sveidra_id   | integer          |                | CT_PACIENTO_SPI_SVEIDRA_ID/text()   |         |       |        |                                               |       | Paciento prisirašymo įstaigos SVEIDRA identifikatorius
   |   |   |   |   | ct_paciento_esi              | string           |                | CT_PACIENTO_ESI/text()              |         |       |        |                                               |       | Paciento ESI numeris
   |   |   |   |   | ct_paciento_apskr_id         | integer          |                | CT_PACIENTO_APSKR_ID/text()         |         |       |        |                                               |       | Paciento prisirašymo įstaigos apskrities identifikatorius
   |   |   |   |   | ct_paciento_sav_id           | integer          |                | CT_PACIENTO_SAV_ID/text()           |         |       |        |                                               |       | Paciento prisirašymo įstaigos savivaldybės identifikatorius (ne iš Adresų registro)
   |   |   |   |   | ct_paciento_savivaldybe      | string           |                | CT_PACIENTO_SAVIVALDYBE/text()      |         |       |        |                                               |       | Paciento prisirašymo įstaigos savivaldybės pavadinimas
   |   |   |   |   | ct_paciento_spi              | string           |                | CT_PACIENTO_SPI/text()              |         |       |        |                                               |       | Paciento prisirašymo įstaigos pavadinimas
   |   |   |   |   | ct_paciento_gydytojo_vardas  | string           |                | CT_PACIENTO_GYDYTOJO_VARDAS/text()  |         |       |        |                                               |       | Paciento prisirašymo įstaigoje šeimos gydytojo vardas
   |   |   |   |   | ct_paciento_gydytojo_pavarde | string           |                | CT_PACIENTO_GYDYTOJO_PAVARDE/text() |         |       |        |                                               |       | Paciento prisirašymo įstaigoje šeimos gydytojo pavardė
   |   |   |   |   | ct_paciento_gydytojo_spaudas | string           |                | CT_PACIENTO_GYDYTOJO_SPAUDAS/text() |         |       |        |                                               |       | Paciento prisirašymo įstaigoje šeimos gydytojo spaudo numeris
   |   |   |   |   | ct_e200ats_tyrimo_metodas    | string           |                | CT_E200ATS_TYRIMO_METODAS/text()    |         |       |        |                                               |       | Laboratorijos naudoto metodo tyrimui pavadinimas
   |   |   |   |   | ct_e200ats_tyrimo_rezultatas | string           |                | CT_E200ATS_TYRIMO_REZULTATAS/text() |         |       |        |                                               |       | Laboratorijos tyrimo rezultato duomuo E200-ats dokumente
   |   |   |   |   | ct_e200ats_rezultatas        | integer          |                | CT_E200ATS_REZULTATAS/text()        |         |       |        |                                               |       | Neredaguojamas laukas. Jame priklausomai nuo reikšmės lauke CT_E200ATS_TYRIMO_REZULTATAS rodomas 0 jei tyrimo rezultatas neigiamas, 1 - jei teigiamas
   |   |   |   |   | ct_tyrimo_trukme             | number           |                | CT_TYRIMO_TRUKME/text()             |         |       |        |                                               |       | Tyrimo trukmė valandomis, skaičiuojama tarp E200 ir E200-ats duomenų įvedimo datų
   |   |   |   |   | ct_ctd_tyrimas_uzsakytas     | string           |                | CT_CTD_TYRIMAS_UZSAKYTAS/text()     |         |       |        |                                               |       | Tyrimo užsakymo data
   |   |   |   |   | ct_ctd_eminys_paimtas        | string           |                | CT_CTD_EMINYS_PAIMTAS/text()        |         |       |        |                                               |       | Ėminio paėmimo data
   |   |   |   |   | ct_ctd_eminio_reg_kodas      | string           |                | CT_CTD_EMINIO_REG_KODAS/text()      |         |       |        |                                               |       | Ėminio registravimo kodas
   |   |   |   |   | ct_ctd_eminys_gautas         | string           |                | CT_CTD_EMINYS_GAUTAS/text()         |         |       |        |                                               |       | Ėminio gavimo data
   |   |   |   |   | ct_ctd_tyrimas_atliktas      | string           |                | CT_CTD_TYRIMAS_ATLIKTAS/text()      |         |       |        |                                               |       | Tyrimo atlikimo data
   |   |   |   |   | ct_sveikatos_specialistas    | integer          |                | CT_SVEIKATOS_SPECIALISTAS/text()    |         |       |        |                                               |       | Požymis ar asmuo yra sveikatos specialistas
   |   |   |   |   | ct_ev_kodas                  | string           |                | CT_EV_KODAS/text()                  |         |       |        |                                               |       | Paciento darboviečių ekonominės veiklos kodai
   |   |   |   |   | ct_telefono_nr               | string           |                | CT_TELEFONO_NR/text()               |         |       |        |                                               |       | Paciento telefono numeris(-ai)
   |   |   |   |   | ct_tyrimo_tipo_id            | integer          |                | CT_TYRIMO_TIPO_ID/text()            |         |       |        |                                               |       | Tyrimo tipo identifikatorius
"""
    path = tmp_path / 'manifest.xsd'
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table


def test_xsd_rc6872(rc: RawConfig, tmp_path: Path):
    # choice in a sequence with a sequence inseide
    xsd = """
<s:schema xmlns:s="http://www.w3.org/2001/XMLSchema" xmlns:tns="http://rc/ireg/1.0/" targetNamespace="http://rc/ireg/1.0/" elementFormDefault="qualified" attributeFormDefault="unqualified">
    <s:element name="data">
        <s:complexType>
            <s:sequence>
                <s:element minOccurs="1" maxOccurs="1" name="responseCode">
                    <s:simpleType>
                        <s:annotation>
                            <s:documentation>1 = OK (užklausa įvykdyta, atsakymas grąžintas)</s:documentation>
                            <s:documentation>0 = NOTFOUND (užklausa įvykdyta, duomenų nerasta)</s:documentation>
                            <s:documentation>-1 = ERROR (įvyko sistemos klaida, užklausa neįvykdyta)</s:documentation>
                        </s:annotation>
                        <s:restriction base="s:int">
                            <s:enumeration value="-1" />
                            <s:enumeration value="0" />
                            <s:enumeration value="1" />
                        </s:restriction>
                    </s:simpleType>
                </s:element>
                <s:element minOccurs="0" maxOccurs="1" name="responseData" type="tns:summary" />
                <s:element minOccurs="0" maxOccurs="1" name="responseMessage" type="s:string" />
            </s:sequence>
        </s:complexType>
    </s:element>

    <s:complexType name="summary">
        <s:choice>
            <s:sequence>
                <s:element minOccurs="1" maxOccurs="1" name="statementId" />
                <s:element minOccurs="1" maxOccurs="1" name="title" />
                <s:element minOccurs="1" maxOccurs="1" name="titleOfType" />
                <s:element minOccurs="1" maxOccurs="1" name="date" type="s:dateTime" />
                <s:element minOccurs="1" maxOccurs="1" name="documents" type="tns:documents"></s:element>
            </s:sequence>
            <s:sequence>
                <s:element minOccurs="1" maxOccurs="1" name="file" type="s:base64Binary" />
            </s:sequence>
        </s:choice>
    </s:complexType>

    <s:complexType name="documents">
        <s:sequence>
            <s:element minOccurs="0" maxOccurs="unbounded" name="document" type="tns:document" />
        </s:sequence>
    </s:complexType>

    <s:complexType name="document">
        <s:sequence>
            <s:element minOccurs="0" maxOccurs="1" name="parentDocument" type="tns:parentDocument" />
            <s:element minOccurs="1" maxOccurs="1" name="documentId" />
            <s:element minOccurs="1" maxOccurs="1" name="documentTypeTitle">
                <s:annotation>
                    <s:documentation>Dokumento tipo pavadinimas</s:documentation>
                </s:annotation>
                <s:simpleType>
                    <s:restriction base="s:string">
                        <s:enumeration value="ĮGALIOJIMAS" />
                        <s:enumeration value="INFORMACINIŲ TECHNOLOGIJŲ PRIEMONĖMIS SUDARYTAS ĮGALIOJIMAS" />
                        <s:enumeration value="PROKŪRA" />
                        <s:enumeration value="INFORMACINIŲ TECHNOLOGIJŲ PRIEMONĖMIS SUDARYTA PROKŪRA" />
                    </s:restriction>
                </s:simpleType>
            </s:element>
            <s:element minOccurs="0" maxOccurs="1" name="termType">
                <s:annotation>
                    <s:documentation>Termino tipas</s:documentation>
                </s:annotation>
                <s:simpleType>
                    <s:restriction base="s:string">
                        <s:enumeration value="Neapibrėžtas" />
                        <s:enumeration value="Apibrėžtas data" />
                        <s:enumeration value="Apibrėžtas įvykiu" />
                    </s:restriction>
                </s:simpleType>
            </s:element>
            <s:element minOccurs="0" maxOccurs="1" name="termDate" type="s:date" />
            <s:element minOccurs="0" maxOccurs="1" name="termAction" />
            <s:element minOccurs="1" maxOccurs="1" name="persons" type="tns:persons" />
        </s:sequence>
    </s:complexType>

    <s:complexType name="parentDocument">
        <s:annotation>
            <s:documentation>Duomenys apie įgaliojimą, kuris buvo perįgaliotas: documentId - Identifikavimo kodas registre (pirmo tėvinio įgaliojimo (ne perįgaliojimo!) kodas) </s:documentation>
        </s:annotation>
        <s:sequence>
            <s:element minOccurs="1" maxOccurs="1" name="documentId" />
            <s:element minOccurs="1" maxOccurs="1" name="persons" type="tns:persons" />
        </s:sequence>
    </s:complexType>

    <s:complexType name="persons">
        <s:sequence>
            <s:element minOccurs="1" maxOccurs="unbounded" name="person" type="tns:person" />
        </s:sequence>
    </s:complexType>

    <s:complexType name="person">
        <s:sequence>
            <s:element minOccurs="0" maxOccurs="1" name="code" type="s:string" />
            <s:element minOccurs="0" maxOccurs="1" name="iltu_code" nillable="true" type="s:string" />

            <s:element minOccurs="1" maxOccurs="1" name="isInactive">
                <s:annotation>
                    <s:documentation>Įgaliojimas šio asmens atžvilgiu pasibaigė</s:documentation>
                </s:annotation>
                <s:simpleType>
                    <s:restriction base="s:string">
                        <s:enumeration value="TAIP" />
                        <s:enumeration value="NE" />
                    </s:restriction>
                </s:simpleType>
            </s:element>

            <s:element minOccurs="0" maxOccurs="1" name="role">
                <s:annotation>
                    <s:documentation>Asmens rolė objekte</s:documentation>
                </s:annotation>
                <s:simpleType>
                    <s:restriction base="s:string">
                        <s:enumeration value="Įgaliotojas" />
                        <s:enumeration value="Įgaliotinis" />
                        <s:enumeration value="Prokūrą išdavęs juridinis asmuo" />
                        <s:enumeration value="Prokuristas" />
                    </s:restriction>
                </s:simpleType>
            </s:element>

            <s:element minOccurs="1" maxOccurs="1" name="type">
                <s:simpleType>
                    <s:restriction base="s:string">
                        <s:enumeration value="FIZ" />
                        <s:enumeration value="JUR" />
                    </s:restriction>
                </s:simpleType>
            </s:element>

            <s:element name="countryCode" minOccurs="0" maxOccurs="1">
                <s:annotation>
                    <s:documentation>Valstybės kodas (ISO 3166-1, alpha-3)</s:documentation>
                </s:annotation>
                <s:simpleType>
                    <s:restriction base="s:string">
                        <s:pattern value="[a-zA-Z]{3}" />
                    </s:restriction>
                </s:simpleType>
            </s:element>

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
id | d | r | b | m | property            | type              | ref | source                                                              | prepare  | level | access | uri | title | description
   | manifest                            |                   |     |                                                                     |          |       |        |     |       |
   |   | resource1                       | xml               |     |                                                                     |          |       |        |     |       |
   |                                     |                   |     |                                                                     |          |       |        |     |       |
   |   |   |   | Person1                 |                   |     | /data/responseData/documents/document/parentDocument/persons/person |          |       |        |     |       |
   |   |   |   |   | birth_date          | string            |     | birthDate/text()                                                    |          |       |        |     |       |
   |   |   |   |   | last_name           | string            |     | lastName/text()                                                     |          |       |        |     |       |
   |   |   |   |   | first_name          | string            |     | firstName/text()                                                    |          |       |        |     |       |
   |   |   |   |   | code                | string            |     | code/text()                                                         |          |       |        |     |       |
   |   |   |   |   | iltu_code           | string            |     | iltu_code/text()                                                    |          |       |        |     |       |
   |   |   |   |   | is_inactive         | string required   |     | isInactive/text()                                                   |          |       |        |     |       | Įgaliojimas šio asmens atžvilgiu pasibaigė
   |                                     | enum              |     | TAIP                                                                |          |       |        |     |       |
   |                                     |                   |     | NE                                                                  |          |       |        |     |       |
   |   |   |   |   | role                | string            |     | role/text()                                                         |          |       |        |     |       | Asmens rolė objekte
   |                                     | enum              |     | Įgaliotojas                                                         |          |       |        |     |       |
   |                                     |                   |     | Įgaliotinis                                                         |          |       |        |     |       |
   |                                     |                   |     | Prokūrą išdavęs juridinis asmuo                                     |          |       |        |     |       |
   |                                     |                   |     | Prokuristas                                                         |          |       |        |     |       |
   |   |   |   |   | type                | string required   |     | type/text()                                                         |          |       |        |     |       |
   |                                     | enum              |     | FIZ                                                                 |          |       |        |     |       |
   |                                     |                   |     | JUR                                                                 |          |       |        |     |       |
   |   |   |   |   | country_code        | string            |     | countryCode/text()                                                  |          |       |        |     |       | Valstybės kodas (ISO 3166-1, alpha-3)
   |                                     |                   |     |                                                                     |          |       |        |     |       |
   |   |   |   | Person2                 |                   |     | /data/responseData/documents/document/parentDocument/persons/person |          |       |        |     |       |
   |   |   |   |   | business_name       | string            |     | businessName/text()                                                 |          |       |        |     |       |
   |   |   |   |   | code                | string            |     | code/text()                                                         |          |       |        |     |       |
   |   |   |   |   | iltu_code           | string            |     | iltu_code/text()                                                    |          |       |        |     |       |
   |   |   |   |   | is_inactive         | string required   |     | isInactive/text()                                                   |          |       |        |     |       | Įgaliojimas šio asmens atžvilgiu pasibaigė
   |                                     | enum              |     | TAIP                                                                |          |       |        |     |       |
   |                                     |                   |     | NE                                                                  |          |       |        |     |       |
   |   |   |   |   | role                | string            |     | role/text()                                                         |          |       |        |     |       | Asmens rolė objekte
   |                                     | enum              |     | Įgaliotojas                                                         |          |       |        |     |       |
   |                                     |                   |     | Įgaliotinis                                                         |          |       |        |     |       |
   |                                     |                   |     | Prokūrą išdavęs juridinis asmuo                                     |          |       |        |     |       |
   |                                     |                   |     | Prokuristas                                                         |          |       |        |     |       |
   |   |   |   |   | type                | string required   |     | type/text()                                                         |          |       |        |     |       |
   |                                     | enum              |     | FIZ                                                                 |          |       |        |     |       |
   |                                     |                   |     | JUR                                                                 |          |       |        |     |       |
   |   |   |   |   | country_code        | string            |     | countryCode/text()                                                  |          |       |        |     |       | Valstybės kodas (ISO 3166-1, alpha-3)
   |                                     |                   |     |                                                                     |          |       |        |     |       |
   |   |   |   | ParentDocument          |                   |     | /data/responseData/documents/document/parentDocument                |          |       |        |     |       |
   |   |   |   |   | document_id         | string required   |     | documentId/text()                                                   |          |       |        |     |       |
   |                                     |                   |     |                                                                     |          |       |        |     |       |
   |   |   |   | Person3                 |                   |     | /data/responseData/documents/document/persons/person                |          |       |        |     |       |
   |   |   |   |   | code                | string            |     | code/text()                                                         |          |       |        |     |       |
   |   |   |   |   | iltu_code           | string            |     | iltu_code/text()                                                    |          |       |        |     |       |
   |   |   |   |   | is_inactive         | string required   |     | isInactive/text()                                                   |          |       |        |     |       | Įgaliojimas šio asmens atžvilgiu pasibaigė
   |                                     | enum              |     | TAIP                                                                |          |       |        |     |       |
   |                                     |                   |     | NE                                                                  |          |       |        |     |       |
   |   |   |   |   | role                | string            |     | role/text()                                                         |          |       |        |     |       | Asmens rolė objekte
   |                                     | enum              |     | Įgaliotojas                                                         |          |       |        |     |       |
   |                                     |                   |     | Įgaliotinis                                                         |          |       |        |     |       |
   |                                     |                   |     | Prokūrą išdavęs juridinis asmuo                                     |          |       |        |     |       |
   |                                     |                   |     | Prokuristas                                                         |          |       |        |     |       |
   |   |   |   |   | type                | string required   |     | type/text()                                                         |          |       |        |     |       |
   |                                     | enum              |     | FIZ                                                                 |          |       |        |     |       |
   |                                     |                   |     | JUR                                                                 |          |       |        |     |       |
   |   |   |   |   | country_code        | string            |     | countryCode/text()                                                  |          |       |        |     |       | Valstybės kodas (ISO 3166-1, alpha-3)
   |                                     |                   |     |                                                                     |          |       |        |     |       |
   |   |   |   | Document                |                   |     | /data/responseData/documents/document                               |          |       |        |     |       |
   |   |   |   |   | document_id         | string required   |     | documentId/text()                                                   |          |       |        |     |       |
   |   |   |   |   | document_type_title | string required   |     | documentTypeTitle/text()                                            |          |       |        |     |       | Dokumento tipo pavadinimas
   |                                     | enum              |     | ĮGALIOJIMAS                                                         |          |       |        |     |       |
   |                                     |                   |     | INFORMACINIŲ TECHNOLOGIJŲ PRIEMONĖMIS SUDARYTAS ĮGALIOJIMAS         |          |       |        |     |       |
   |                                     |                   |     | PROKŪRA                                                             |          |       |        |     |       |
   |                                     |                   |     | INFORMACINIŲ TECHNOLOGIJŲ PRIEMONĖMIS SUDARYTA PROKŪRA              |          |       |        |     |       |
   |   |   |   |   | term_type           | string            |     | termType/text()                                                     |          |       |        |     |       | Termino tipas
   |                                     | enum              |     | Neapibrėžtas                                                        |          |       |        |     |       |
   |                                     |                   |     | Apibrėžtas data                                                     |          |       |        |     |       |
   |                                     |                   |     | Apibrėžtas įvykiu                                                   |          |       |        |     |       |
   |   |   |   |   | term_date           | date              |     | termDate/text()                                                     |          |       |        |     |       |
   |   |   |   |   | term_action         | string            |     | termAction/text()                                                   |          |       |        |     |       |
   |                                     |                   |     |                                                                     |          |       |        |     |       |
   |   |   |   | ResponseData1           |                   |     | /data/responseData                                                  |          |       |        |     |       |
   |   |   |   |   | statement_id        | string required   |     | statementId/text()                                                  |          |       |        |     |       |
   |   |   |   |   | title               | string required   |     | title/text()                                                        |          |       |        |     |       |
   |   |   |   |   | title_of_type       | string required   |     | titleOfType/text()                                                  |          |       |        |     |       |
   |   |   |   |   | date                | datetime required |     | date/text()                                                         |          |       |        |     |       |
   |                                     |                   |     |                                                                     |          |       |        |     |       |
   |   |   |   | ResponseData2           |                   |     | /data/responseData                                                  |          |       |        |     |       |
   |   |   |   |   | file                | binary required   |     | file/text()                                                         | 'base64' |       |        |     |       |
   |                                     |                   |     |                                                                     |          |       |        |     |       |
   |   |   |   | Data                    |                   |     | /data                                                               |          |       |        |     |       |
   |   |   |   |   | response_code       | integer required  |     | responseCode/text()                                                 |          |       |        |     |       | 1 = OK (užklausa įvykdyta, atsakymas grąžintas) 0 = NOTFOUND (užklausa įvykdyta, duomenų nerasta) -1 = ERROR (įvyko sistemos klaida, užklausa neįvykdyta)
   |                                     | enum              |     | -1                                                                  |          |       |        |     |       |
   |                                     |                   |     | 0                                                                   |          |       |        |     |       |
   |                                     |                   |     | 1                                                                   |          |       |        |     |       |
   |   |   |   |   | response_message    | string            |     | responseMessage/text()                                              |          |       |        |     |       |
"""
    path = tmp_path / 'manifest.xsd'
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table


def test_xsd_rc1706(rc: RawConfig, tmp_path: Path):
    # complexContent
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
	<xs:complexType name="ArrayOfBE_FULL_doclist">
		<xs:sequence>
			<xs:element minOccurs="0" maxOccurs="unbounded" name="BE_FULL_doclist" nillable="true" type="BE_FULL_doclist"/>
		</xs:sequence>
	</xs:complexType>
	<xs:complexType name="BE_FULL_doclist">
		<xs:complexContent mixed="false">
			<xs:extension base="BusinessEntityOfBE_FULL_doclist">
				<xs:sequence>
					<xs:element minOccurs="0" maxOccurs="1" name="Actor" type="BE_XSLT_DS_ACTOR"/>
					<xs:element minOccurs="0" maxOccurs="1" name="CarerList" type="ArrayOfBE_XSLT_DS_CARER"/>
					<xs:element minOccurs="0" maxOccurs="1" name="FileList" type="ArrayOfBE_File"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Files" type="ArrayOfBE_FAILAI"/>
					<xs:element minOccurs="1" maxOccurs="1" name="DocumentKindID" type="xs:int"/>
					<xs:element minOccurs="0" maxOccurs="1" name="IdentCode" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="CourtName" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="CourtAddress" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="JugdeFirstName" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="JugdeLastName" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="DecisionDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="1" maxOccurs="1" name="DecisionInureDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="1" maxOccurs="1" name="UrgentExecution" nillable="true" type="xs:boolean"/>
					<xs:element minOccurs="0" maxOccurs="1" name="CivilCaseNo" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="ProcessCaseNo" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="FirstCourtName" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="FirstJugdeFirstName" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="FirstJugdeLastName" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="FirstDecisionDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="1" maxOccurs="1" name="FirstDecisionInureDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="1" maxOccurs="1" name="FirstUrgentExecution" nillable="true" type="xs:boolean"/>
					<xs:element minOccurs="0" maxOccurs="1" name="FirstCivilCaseNo" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="FirstProcessCaseNo" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Comments" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="ProprestrictionKindID" nillable="true" type="xs:int"/>
					<xs:element minOccurs="0" maxOccurs="1" name="ProprestrictionKind" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="CapabilityRestriction" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="InCapabilityKindID" nillable="true" type="xs:int"/>
					<xs:element minOccurs="0" maxOccurs="1" name="InCapabilityKind" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="DataDefinaBase" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="DataDefineComment" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="DecisionCK" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="RegistrationDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="1" maxOccurs="1" name="FirstRegDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="1" maxOccurs="1" name="TypeId" type="xs:int"/>
				</xs:sequence>
			</xs:extension>
		</xs:complexContent>
	</xs:complexType>
	<xs:complexType name="BusinessEntityOfBE_FULL_doclist" abstract="true"/>
	<xs:complexType name="BE_XSLT_DS_ACTOR">
		<xs:complexContent mixed="false">
			<xs:extension base="BusinessEntityOfBE_XSLT_DS_ACTOR">
				<xs:sequence>
					<xs:element minOccurs="1" maxOccurs="1" name="TypeID" type="xs:decimal"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Code" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="FirstName" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="LastName" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Gender" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="BirthDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="1" maxOccurs="1" name="DeathDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="0" maxOccurs="1" name="BirthPlace" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="CityDistrict" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="StreetVillage" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Country" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="DeclaredCityDistrict" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="DeclaredStreetVillage" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="DeclaredCountry" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="MarriageDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Limit_Area_List" type="ArrayOfBE_XSLT_DS_LIMIT_AREA"/>
				</xs:sequence>
			</xs:extension>
		</xs:complexContent>
	</xs:complexType>
	<xs:complexType name="BusinessEntityOfBE_XSLT_DS_ACTOR" abstract="true"/>
	<xs:complexType name="ArrayOfBE_XSLT_DS_LIMIT_AREA">
		<xs:sequence>
			<xs:element minOccurs="0" maxOccurs="unbounded" name="BE_XSLT_DS_LIMIT_AREA" nillable="true" type="BE_XSLT_DS_LIMIT_AREA"/>
		</xs:sequence>
	</xs:complexType>
	<xs:complexType name="BE_XSLT_DS_LIMIT_AREA">
		<xs:complexContent mixed="false">
			<xs:extension base="BusinessEntityOfBE_XSLT_DS_LIMIT_AREA">
				<xs:sequence>
					<xs:element minOccurs="0" maxOccurs="1" name="NAR_code" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Name" type="xs:string"/>
				</xs:sequence>
			</xs:extension>
		</xs:complexContent>
	</xs:complexType>
	<xs:complexType name="BusinessEntityOfBE_XSLT_DS_LIMIT_AREA" abstract="true"/>
	<xs:complexType name="ArrayOfBE_XSLT_DS_CARER">
		<xs:sequence>
			<xs:element minOccurs="0" maxOccurs="unbounded" name="Carer" nillable="true" type="BE_XSLT_DS_CARER"/>
		</xs:sequence>
	</xs:complexType>
	<xs:complexType name="BE_XSLT_DS_CARER">
		<xs:complexContent mixed="false">
			<xs:extension base="BusinessEntityOfBE_XSLT_DS_CARER">
				<xs:sequence>
					<xs:element minOccurs="1" maxOccurs="1" name="ID" nillable="true" type="xs:int"/>
					<xs:element minOccurs="1" maxOccurs="1" name="RefId" nillable="true" type="xs:int"/>
					<xs:element minOccurs="1" maxOccurs="1" name="TypeID" nillable="true" type="xs:int"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Code" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="FirstName" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="LastName" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="FullName" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="CityDistrict" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="StreetVillage" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Country" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="BirthDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="0" maxOccurs="1" name="DeclaredCityDistrict" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="DeclaredStreetVillage" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="DeclaredCountry" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="NarCode" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="DeathDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="0" maxOccurs="1" name="IsDead" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="CompTypeId" nillable="true" type="xs:int"/>
					<xs:element minOccurs="0" maxOccurs="1" name="CompTypeName" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="CompRegistrationOutdate" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="CompCode" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="CourtName" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="DecisionDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="1" maxOccurs="1" name="DecisionInureDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="1" maxOccurs="1" name="UrgentExecution" nillable="true" type="xs:boolean"/>
					<xs:element minOccurs="0" maxOccurs="1" name="CivilCaseNo" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="ProcessCaseNo" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="RegistrationDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Limit_Area_List" type="ArrayOfBE_XSLT_DS_LIMIT_AREA"/>
					<xs:element minOccurs="1" maxOccurs="1" name="HasRemovedAreas" nillable="true" type="xs:boolean"/>
					<xs:element minOccurs="1" maxOccurs="1" name="HasAddedAreas" nillable="true" type="xs:boolean"/>
				</xs:sequence>
			</xs:extension>
		</xs:complexContent>
	</xs:complexType>
	<xs:complexType name="BusinessEntityOfBE_XSLT_DS_CARER" abstract="true"/>
	<xs:complexType name="ArrayOfBE_File">
		<xs:sequence>
			<xs:element minOccurs="0" maxOccurs="unbounded" name="BE_File" nillable="true" type="BE_File"/>
		</xs:sequence>
	</xs:complexType>
	<xs:complexType name="BE_File">
		<xs:complexContent mixed="false">
			<xs:extension base="BE_FileBase">
				<xs:sequence>
					<xs:element minOccurs="0" maxOccurs="1" name="filecontentbase64string" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="DecodedId" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Href" type="xs:string"/>
				</xs:sequence>
			</xs:extension>
		</xs:complexContent>
	</xs:complexType>
	<xs:complexType name="BE_FileBase" abstract="true">
		<xs:sequence>
			<xs:element minOccurs="1" maxOccurs="1" name="Id" type="xs:int"/>
			<xs:element minOccurs="0" maxOccurs="1" name="FileName" type="xs:string"/>
			<xs:element minOccurs="0" maxOccurs="1" name="FileContent" type="xs:base64Binary"/>
			<xs:element minOccurs="0" maxOccurs="1" name="FileType" type="xs:string"/>
			<xs:element minOccurs="1" maxOccurs="1" name="FileSize" nillable="true" type="xs:int"/>
		</xs:sequence>
	</xs:complexType>
	<xs:complexType name="ArrayOfBE_FAILAI">
		<xs:sequence>
			<xs:element minOccurs="0" maxOccurs="unbounded" name="BE_FAILAI" nillable="true" type="BE_FAILAI"/>
		</xs:sequence>
	</xs:complexType>
	<xs:complexType name="BE_FAILAI">
		<xs:sequence>
			<xs:element minOccurs="0" maxOccurs="1" name="filecontentbase64string" type="xs:string"/>
			<xs:element minOccurs="0" maxOccurs="1" name="FileName" type="xs:string"/>
		</xs:sequence>
	</xs:complexType>
	<xs:complexType name="ArrayOfBE_FULL_preorder">
		<xs:sequence>
			<xs:element minOccurs="0" maxOccurs="unbounded" name="BE_FULL_preorder" nillable="true" type="BE_FULL_preorder"/>
		</xs:sequence>
	</xs:complexType>
	<xs:complexType name="BE_FULL_preorder">
		<xs:complexContent mixed="false">
			<xs:extension base="BusinessEntityOfBE_FULL_preorder">
				<xs:sequence>
					<xs:element minOccurs="1" maxOccurs="1" name="RegistrationDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="1" maxOccurs="1" name="FirstRegDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Actor" type="BE_XSLT_DS_ACTOR"/>
					<xs:element minOccurs="1" maxOccurs="1" name="PreorderApproveDate" type="xs:dateTime"/>
					<xs:element minOccurs="0" maxOccurs="1" name="PreorderApprovePlace" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="PreorderInureDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="0" maxOccurs="1" name="NotaryRegNo" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="DocumentKindID" type="xs:int"/>
					<xs:element minOccurs="0" maxOccurs="1" name="IdentCode" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Preorder_Term" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="Term_Id" type="xs:int"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Term_Event" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="Term_Date" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="0" maxOccurs="1" name="RejectedReason" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="ErrorCorrection" nillable="true" type="xs:int"/>
					<xs:element minOccurs="0" maxOccurs="1" name="CivilCaseNo" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="CourtName" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="TypeClas" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="JudgeProcessCaseNo" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="UrgentExcecution" type="xs:boolean"/>
					<xs:element minOccurs="1" maxOccurs="1" name="DesicionDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="1" maxOccurs="1" name="PreorderTermDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="0" maxOccurs="1" name="PreorderTermEvent" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="ApproverName" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="ApproverLastName" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="ApproverPosition" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="ApproverCompanyName" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="ApproverStreetVillage" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="ApproverCityDistrict" type="xs:string"/>
				</xs:sequence>
			</xs:extension>
		</xs:complexContent>
	</xs:complexType>
	<xs:complexType name="BusinessEntityOfBE_FULL_preorder" abstract="true"/>
	<xs:complexType name="ArrayOfBE_FULL_contract">
		<xs:sequence>
			<xs:element minOccurs="0" maxOccurs="unbounded" name="BE_FULL_contract" nillable="true" type="BE_FULL_contract"/>
		</xs:sequence>
	</xs:complexType>
	<xs:complexType name="BE_FULL_contract">
		<xs:complexContent mixed="false">
			<xs:extension base="BusinessEntityOfBE_FULL_contract">
				<xs:sequence>
					<xs:element minOccurs="0" maxOccurs="1" name="Actor" type="BE_XSLT_DS_ACTOR"/>
					<xs:element minOccurs="0" maxOccurs="1" name="AssistanceProvider" type="ArrayOfBE_XSLT_DS_ASSISTANCE_PROVIDER"/>
					<xs:element minOccurs="0" maxOccurs="1" name="statusas" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="RegistrationDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="1" maxOccurs="1" name="FirstRegDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="1" maxOccurs="1" name="DocumentKindID" type="xs:int"/>
					<xs:element minOccurs="1" maxOccurs="1" name="RejectedReasonID" nillable="true" type="xs:int"/>
					<xs:element minOccurs="1" maxOccurs="1" name="DateReceived" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="1" maxOccurs="1" name="ContractApproveDate" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="0" maxOccurs="1" name="ContractApprovePlace" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="IdentCode" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="NotaryRegNo" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="ApproverName" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="ApproverLastName" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="ApproverStreetVillage" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="ApproverCityDistrict" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="ApproverTypeId" type="xs:int"/>
					<xs:element minOccurs="0" maxOccurs="1" name="ApproverCompanyName" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="ApproverPosition" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="ErrorCorrection" nillable="true" type="xs:int"/>
				</xs:sequence>
			</xs:extension>
		</xs:complexContent>
	</xs:complexType>
	<xs:complexType name="BusinessEntityOfBE_FULL_contract" abstract="true"/>
	<xs:complexType name="ArrayOfBE_XSLT_DS_ASSISTANCE_PROVIDER">
		<xs:sequence>
			<xs:element minOccurs="0" maxOccurs="unbounded" name="BE_XSLT_DS_ASSISTANCE_PROVIDER" nillable="true" type="BE_XSLT_DS_ASSISTANCE_PROVIDER"/>
		</xs:sequence>
	</xs:complexType>
	<xs:complexType name="BE_XSLT_DS_ASSISTANCE_PROVIDER">
		<xs:complexContent mixed="false">
			<xs:extension base="BusinessEntityOfBE_XSLT_DS_ASSISTANCE_PROVIDER">
				<xs:sequence>
					<xs:element minOccurs="1" maxOccurs="1" name="Type_Id" type="xs:int"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Name" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Last_Name" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Code" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="City_District" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Street_Village" type="xs:string"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Country" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="Birth_Date" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Gender" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="Death_Date" nillable="true" type="xs:dateTime"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Status" type="xs:string"/>
					<xs:element minOccurs="1" maxOccurs="1" name="EditingState" type="xs:int"/>
					<xs:element minOccurs="0" maxOccurs="1" name="LimitAreas" type="ArrayOfBE_XSLT_DS_LIMIT_AREA"/>
					<xs:element minOccurs="0" maxOccurs="1" name="Limit_Areas" type="ArrayOfBE_XSLT_DS_LIMIT_AREA"/>
				</xs:sequence>
			</xs:extension>
		</xs:complexContent>
	</xs:complexType>
	<xs:complexType name="BusinessEntityOfBE_XSLT_DS_ASSISTANCE_PROVIDER" abstract="true"/>
</xs:schema>
"""

    table = """
id | d | r | b | m | property                   | type              | ref | source                                                                                                                     | prepare | level | access | uri | title | description
   | manifest                                   |                   |     |                                                                                                                            |         |       |        |     |       |
   |   | resource1                              | xml               |     |                                                                                                                            |         |       |        |     |       |
   |                                            |                   |     |                                                                                                                            |         |       |        |     |       |
   |   |   |   | BeXsltDsLimitArea              |                   |     | /BE_FULL/DocList/BE_FULL_doclist/Actor/Limit_Area_List/BE_XSLT_DS_LIMIT_AREA                                               |         |       |        |     |       |
   |   |   |   |   | nar_code                   | string            |     | NAR_code/text()                                                                                                            |         |       |        |     |       |
   |   |   |   |   | name                       | string            |     | Name/text()                                                                                                                |         |       |        |     |       |
   |                                            |                   |     |                                                                                                                            |         |       |        |     |       |
   |   |   |   | Actor                          |                   |     | /BE_FULL/DocList/BE_FULL_doclist/Actor                                                                                     |         |       |        |     |       |
   |   |   |   |   | typeid                     | number required   |     | TypeID/text()                                                                                                              |         |       |        |     |       |
   |   |   |   |   | code                       | string            |     | Code/text()                                                                                                                |         |       |        |     |       |
   |   |   |   |   | first_name                 | string            |     | FirstName/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | last_name                  | string            |     | LastName/text()                                                                                                            |         |       |        |     |       |
   |   |   |   |   | gender                     | string            |     | Gender/text()                                                                                                              |         |       |        |     |       |
   |   |   |   |   | birth_date                 | datetime          |     | BirthDate/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | death_date                 | datetime          |     | DeathDate/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | birth_place                | string            |     | BirthPlace/text()                                                                                                          |         |       |        |     |       |
   |   |   |   |   | city_district              | string            |     | CityDistrict/text()                                                                                                        |         |       |        |     |       |
   |   |   |   |   | street_village             | string            |     | StreetVillage/text()                                                                                                       |         |       |        |     |       |
   |   |   |   |   | country                    | string            |     | Country/text()                                                                                                             |         |       |        |     |       |
   |   |   |   |   | declared_city_district     | string            |     | DeclaredCityDistrict/text()                                                                                                |         |       |        |     |       |
   |   |   |   |   | declared_street_village    | string            |     | DeclaredStreetVillage/text()                                                                                               |         |       |        |     |       |
   |   |   |   |   | declared_country           | string            |     | DeclaredCountry/text()                                                                                                     |         |       |        |     |       |
   |   |   |   |   | marriage_date              | datetime          |     | MarriageDate/text()                                                                                                        |         |       |        |     |       |
   |                                            |                   |     |                                                                                                                            |         |       |        |     |       |
   |   |   |   | BeXsltDsLimitArea1             |                   |     | /BE_FULL/DocList/BE_FULL_doclist/CarerList/Carer/Limit_Area_List/BE_XSLT_DS_LIMIT_AREA                                     |         |       |        |     |       |
   |   |   |   |   | nar_code                   | string            |     | NAR_code/text()                                                                                                            |         |       |        |     |       |
   |   |   |   |   | name                       | string            |     | Name/text()                                                                                                                |         |       |        |     |       |
   |                                            |                   |     |                                                                                                                            |         |       |        |     |       |
   |   |   |   | Carer                          |                   |     | /BE_FULL/DocList/BE_FULL_doclist/CarerList/Carer                                                                           |         |       |        |     |       |
   |   |   |   |   | id                         | integer           |     | ID/text()                                                                                                                  |         |       |        |     |       |
   |   |   |   |   | ref_id                     | integer           |     | RefId/text()                                                                                                               |         |       |        |     |       |
   |   |   |   |   | typeid                     | integer           |     | TypeID/text()                                                                                                              |         |       |        |     |       |
   |   |   |   |   | code                       | string            |     | Code/text()                                                                                                                |         |       |        |     |       |
   |   |   |   |   | first_name                 | string            |     | FirstName/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | last_name                  | string            |     | LastName/text()                                                                                                            |         |       |        |     |       |
   |   |   |   |   | full_name                  | string            |     | FullName/text()                                                                                                            |         |       |        |     |       |
   |   |   |   |   | city_district              | string            |     | CityDistrict/text()                                                                                                        |         |       |        |     |       |
   |   |   |   |   | street_village             | string            |     | StreetVillage/text()                                                                                                       |         |       |        |     |       |
   |   |   |   |   | country                    | string            |     | Country/text()                                                                                                             |         |       |        |     |       |
   |   |   |   |   | birth_date                 | datetime          |     | BirthDate/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | declared_city_district     | string            |     | DeclaredCityDistrict/text()                                                                                                |         |       |        |     |       |
   |   |   |   |   | declared_street_village    | string            |     | DeclaredStreetVillage/text()                                                                                               |         |       |        |     |       |
   |   |   |   |   | declared_country           | string            |     | DeclaredCountry/text()                                                                                                     |         |       |        |     |       |
   |   |   |   |   | nar_code                   | string            |     | NarCode/text()                                                                                                             |         |       |        |     |       |
   |   |   |   |   | death_date                 | datetime          |     | DeathDate/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | is_dead                    | string            |     | IsDead/text()                                                                                                              |         |       |        |     |       |
   |   |   |   |   | comp_type_id               | integer           |     | CompTypeId/text()                                                                                                          |         |       |        |     |       |
   |   |   |   |   | comp_type_name             | string            |     | CompTypeName/text()                                                                                                        |         |       |        |     |       |
   |   |   |   |   | comp_registration_outdate  | string            |     | CompRegistrationOutdate/text()                                                                                             |         |       |        |     |       |
   |   |   |   |   | comp_code                  | string            |     | CompCode/text()                                                                                                            |         |       |        |     |       |
   |   |   |   |   | court_name                 | string            |     | CourtName/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | decision_date              | datetime          |     | DecisionDate/text()                                                                                                        |         |       |        |     |       |
   |   |   |   |   | decision_inure_date        | datetime          |     | DecisionInureDate/text()                                                                                                   |         |       |        |     |       |
   |   |   |   |   | urgent_execution           | boolean           |     | UrgentExecution/text()                                                                                                     |         |       |        |     |       |
   |   |   |   |   | civil_case_no              | string            |     | CivilCaseNo/text()                                                                                                         |         |       |        |     |       |
   |   |   |   |   | process_case_no            | string            |     | ProcessCaseNo/text()                                                                                                       |         |       |        |     |       |
   |   |   |   |   | registration_date          | datetime          |     | RegistrationDate/text()                                                                                                    |         |       |        |     |       |
   |   |   |   |   | has_removed_areas          | boolean           |     | HasRemovedAreas/text()                                                                                                     |         |       |        |     |       |
   |   |   |   |   | has_added_areas            | boolean           |     | HasAddedAreas/text()                                                                                                       |         |       |        |     |       |
   |                                            |                   |     |                                                                                                                            |         |       |        |     |       |
   |   |   |   | BEFile                         |                   |     | /BE_FULL/DocList/BE_FULL_doclist/FileList/BE_File                                                                          |         |       |        |     |       |
   |   |   |   |   | filecontentbase64string    | string            |     | filecontentbase64string/text()                                                                                             |         |       |        |     |       |
   |   |   |   |   | decoded_id                 | string            |     | DecodedId/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | href                       | string            |     | Href/text()                                                                                                                |         |       |        |     |       |
   |                                            |                   |     |                                                                                                                            |         |       |        |     |       |
   |   |   |   | BeFailai                       |                   |     | /BE_FULL/DocList/BE_FULL_doclist/Files/BE_FAILAI                                                                           |         |       |        |     |       |
   |   |   |   |   | filecontentbase64string    | string            |     | filecontentbase64string/text()                                                                                             |         |       |        |     |       |
   |   |   |   |   | file_name                  | string            |     | FileName/text()                                                                                                            |         |       |        |     |       |
   |                                            |                   |     |                                                                                                                            |         |       |        |     |       |
   |   |   |   | BEFULLDoclist                  |                   |     | /BE_FULL/DocList/BE_FULL_doclist                                                                                           |         |       |        |     |       |
   |   |   |   |   | document_kindid            | integer required  |     | DocumentKindID/text()                                                                                                      |         |       |        |     |       |
   |   |   |   |   | ident_code                 | string            |     | IdentCode/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | court_name                 | string            |     | CourtName/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | court_address              | string            |     | CourtAddress/text()                                                                                                        |         |       |        |     |       |
   |   |   |   |   | jugde_first_name           | string            |     | JugdeFirstName/text()                                                                                                      |         |       |        |     |       |
   |   |   |   |   | jugde_last_name            | string            |     | JugdeLastName/text()                                                                                                       |         |       |        |     |       |
   |   |   |   |   | decision_date              | datetime          |     | DecisionDate/text()                                                                                                        |         |       |        |     |       |
   |   |   |   |   | decision_inure_date        | datetime          |     | DecisionInureDate/text()                                                                                                   |         |       |        |     |       |
   |   |   |   |   | urgent_execution           | boolean           |     | UrgentExecution/text()                                                                                                     |         |       |        |     |       |
   |   |   |   |   | civil_case_no              | string            |     | CivilCaseNo/text()                                                                                                         |         |       |        |     |       |
   |   |   |   |   | process_case_no            | string            |     | ProcessCaseNo/text()                                                                                                       |         |       |        |     |       |
   |   |   |   |   | first_court_name           | string            |     | FirstCourtName/text()                                                                                                      |         |       |        |     |       |
   |   |   |   |   | first_jugde_first_name     | string            |     | FirstJugdeFirstName/text()                                                                                                 |         |       |        |     |       |
   |   |   |   |   | first_jugde_last_name      | string            |     | FirstJugdeLastName/text()                                                                                                  |         |       |        |     |       |
   |   |   |   |   | first_decision_date        | datetime          |     | FirstDecisionDate/text()                                                                                                   |         |       |        |     |       |
   |   |   |   |   | first_decision_inure_date  | datetime          |     | FirstDecisionInureDate/text()                                                                                              |         |       |        |     |       |
   |   |   |   |   | first_urgent_execution     | boolean           |     | FirstUrgentExecution/text()                                                                                                |         |       |        |     |       |
   |   |   |   |   | first_civil_case_no        | string            |     | FirstCivilCaseNo/text()                                                                                                    |         |       |        |     |       |
   |   |   |   |   | first_process_case_no      | string            |     | FirstProcessCaseNo/text()                                                                                                  |         |       |        |     |       |
   |   |   |   |   | comments                   | string            |     | Comments/text()                                                                                                            |         |       |        |     |       |
   |   |   |   |   | proprestriction_kindid     | integer           |     | ProprestrictionKindID/text()                                                                                               |         |       |        |     |       |
   |   |   |   |   | proprestriction_kind       | string            |     | ProprestrictionKind/text()                                                                                                 |         |       |        |     |       |
   |   |   |   |   | capability_restriction     | string            |     | CapabilityRestriction/text()                                                                                               |         |       |        |     |       |
   |   |   |   |   | in_capability_kindid       | integer           |     | InCapabilityKindID/text()                                                                                                  |         |       |        |     |       |
   |   |   |   |   | in_capability_kind         | string            |     | InCapabilityKind/text()                                                                                                    |         |       |        |     |       |
   |   |   |   |   | data_defina_base           | string            |     | DataDefinaBase/text()                                                                                                      |         |       |        |     |       |
   |   |   |   |   | data_define_comment        | string            |     | DataDefineComment/text()                                                                                                   |         |       |        |     |       |
   |   |   |   |   | decisionck                 | string            |     | DecisionCK/text()                                                                                                          |         |       |        |     |       |
   |   |   |   |   | registration_date          | datetime          |     | RegistrationDate/text()                                                                                                    |         |       |        |     |       |
   |   |   |   |   | first_reg_date             | datetime          |     | FirstRegDate/text()                                                                                                        |         |       |        |     |       |
   |   |   |   |   | type_id                    | integer required  |     | TypeId/text()                                                                                                              |         |       |        |     |       |
   |                                            |                   |     |                                                                                                                            |         |       |        |     |       |
   |   |   |   | BeXsltDsLimitArea2             |                   |     | /BE_FULL/PreorderList/BE_FULL_preorder/Actor/Limit_Area_List/BE_XSLT_DS_LIMIT_AREA                                         |         |       |        |     |       |
   |   |   |   |   | nar_code                   | string            |     | NAR_code/text()                                                                                                            |         |       |        |     |       |
   |   |   |   |   | name                       | string            |     | Name/text()                                                                                                                |         |       |        |     |       |
   |                                            |                   |     |                                                                                                                            |         |       |        |     |       |
   |   |   |   | Actor1                         |                   |     | /BE_FULL/PreorderList/BE_FULL_preorder/Actor                                                                               |         |       |        |     |       |
   |   |   |   |   | typeid                     | number required   |     | TypeID/text()                                                                                                              |         |       |        |     |       |
   |   |   |   |   | code                       | string            |     | Code/text()                                                                                                                |         |       |        |     |       |
   |   |   |   |   | first_name                 | string            |     | FirstName/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | last_name                  | string            |     | LastName/text()                                                                                                            |         |       |        |     |       |
   |   |   |   |   | gender                     | string            |     | Gender/text()                                                                                                              |         |       |        |     |       |
   |   |   |   |   | birth_date                 | datetime          |     | BirthDate/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | death_date                 | datetime          |     | DeathDate/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | birth_place                | string            |     | BirthPlace/text()                                                                                                          |         |       |        |     |       |
   |   |   |   |   | city_district              | string            |     | CityDistrict/text()                                                                                                        |         |       |        |     |       |
   |   |   |   |   | street_village             | string            |     | StreetVillage/text()                                                                                                       |         |       |        |     |       |
   |   |   |   |   | country                    | string            |     | Country/text()                                                                                                             |         |       |        |     |       |
   |   |   |   |   | declared_city_district     | string            |     | DeclaredCityDistrict/text()                                                                                                |         |       |        |     |       |
   |   |   |   |   | declared_street_village    | string            |     | DeclaredStreetVillage/text()                                                                                               |         |       |        |     |       |
   |   |   |   |   | declared_country           | string            |     | DeclaredCountry/text()                                                                                                     |         |       |        |     |       |
   |   |   |   |   | marriage_date              | datetime          |     | MarriageDate/text()                                                                                                        |         |       |        |     |       |
   |                                            |                   |     |                                                                                                                            |         |       |        |     |       |
   |   |   |   | BEFULLPreorder                 |                   |     | /BE_FULL/PreorderList/BE_FULL_preorder                                                                                     |         |       |        |     |       |
   |   |   |   |   | registration_date          | datetime          |     | RegistrationDate/text()                                                                                                    |         |       |        |     |       |
   |   |   |   |   | first_reg_date             | datetime          |     | FirstRegDate/text()                                                                                                        |         |       |        |     |       |
   |   |   |   |   | preorder_approve_date      | datetime required |     | PreorderApproveDate/text()                                                                                                 |         |       |        |     |       |
   |   |   |   |   | preorder_approve_place     | string            |     | PreorderApprovePlace/text()                                                                                                |         |       |        |     |       |
   |   |   |   |   | preorder_inure_date        | datetime          |     | PreorderInureDate/text()                                                                                                   |         |       |        |     |       |
   |   |   |   |   | notary_reg_no              | string            |     | NotaryRegNo/text()                                                                                                         |         |       |        |     |       |
   |   |   |   |   | document_kindid            | integer required  |     | DocumentKindID/text()                                                                                                      |         |       |        |     |       |
   |   |   |   |   | ident_code                 | string            |     | IdentCode/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | preorder_term              | string            |     | Preorder_Term/text()                                                                                                       |         |       |        |     |       |
   |   |   |   |   | term_id                    | integer required  |     | Term_Id/text()                                                                                                             |         |       |        |     |       |
   |   |   |   |   | term_event                 | string            |     | Term_Event/text()                                                                                                          |         |       |        |     |       |
   |   |   |   |   | term_date                  | datetime          |     | Term_Date/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | rejected_reason            | string            |     | RejectedReason/text()                                                                                                      |         |       |        |     |       |
   |   |   |   |   | error_correction           | integer           |     | ErrorCorrection/text()                                                                                                     |         |       |        |     |       |
   |   |   |   |   | civil_case_no              | string            |     | CivilCaseNo/text()                                                                                                         |         |       |        |     |       |
   |   |   |   |   | court_name                 | string            |     | CourtName/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | type_clas                  | string            |     | TypeClas/text()                                                                                                            |         |       |        |     |       |
   |   |   |   |   | judge_process_case_no      | string            |     | JudgeProcessCaseNo/text()                                                                                                  |         |       |        |     |       |
   |   |   |   |   | urgent_excecution          | boolean required  |     | UrgentExcecution/text()                                                                                                    |         |       |        |     |       |
   |   |   |   |   | desicion_date              | datetime          |     | DesicionDate/text()                                                                                                        |         |       |        |     |       |
   |   |   |   |   | preorder_term_date         | datetime          |     | PreorderTermDate/text()                                                                                                    |         |       |        |     |       |
   |   |   |   |   | preorder_term_event        | string            |     | PreorderTermEvent/text()                                                                                                   |         |       |        |     |       |
   |   |   |   |   | approver_name              | string            |     | ApproverName/text()                                                                                                        |         |       |        |     |       |
   |   |   |   |   | approver_last_name         | string            |     | ApproverLastName/text()                                                                                                    |         |       |        |     |       |
   |   |   |   |   | approver_position          | string            |     | ApproverPosition/text()                                                                                                    |         |       |        |     |       |
   |   |   |   |   | approver_company_name      | string            |     | ApproverCompanyName/text()                                                                                                 |         |       |        |     |       |
   |   |   |   |   | approver_street_village    | string            |     | ApproverStreetVillage/text()                                                                                               |         |       |        |     |       |
   |   |   |   |   | approver_city_district     | string            |     | ApproverCityDistrict/text()                                                                                                |         |       |        |     |       |
   |                                            |                   |     |                                                                                                                            |         |       |        |     |       |
   |   |   |   | BeXsltDsLimitArea3             |                   |     | /BE_FULL/ContractList/BE_FULL_contract/Actor/Limit_Area_List/BE_XSLT_DS_LIMIT_AREA                                         |         |       |        |     |       |
   |   |   |   |   | nar_code                   | string            |     | NAR_code/text()                                                                                                            |         |       |        |     |       |
   |   |   |   |   | name                       | string            |     | Name/text()                                                                                                                |         |       |        |     |       |
   |                                            |                   |     |                                                                                                                            |         |       |        |     |       |
   |   |   |   | Actor2                         |                   |     | /BE_FULL/ContractList/BE_FULL_contract/Actor                                                                               |         |       |        |     |       |
   |   |   |   |   | typeid                     | number required   |     | TypeID/text()                                                                                                              |         |       |        |     |       |
   |   |   |   |   | code                       | string            |     | Code/text()                                                                                                                |         |       |        |     |       |
   |   |   |   |   | first_name                 | string            |     | FirstName/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | last_name                  | string            |     | LastName/text()                                                                                                            |         |       |        |     |       |
   |   |   |   |   | gender                     | string            |     | Gender/text()                                                                                                              |         |       |        |     |       |
   |   |   |   |   | birth_date                 | datetime          |     | BirthDate/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | death_date                 | datetime          |     | DeathDate/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | birth_place                | string            |     | BirthPlace/text()                                                                                                          |         |       |        |     |       |
   |   |   |   |   | city_district              | string            |     | CityDistrict/text()                                                                                                        |         |       |        |     |       |
   |   |   |   |   | street_village             | string            |     | StreetVillage/text()                                                                                                       |         |       |        |     |       |
   |   |   |   |   | country                    | string            |     | Country/text()                                                                                                             |         |       |        |     |       |
   |   |   |   |   | declared_city_district     | string            |     | DeclaredCityDistrict/text()                                                                                                |         |       |        |     |       |
   |   |   |   |   | declared_street_village    | string            |     | DeclaredStreetVillage/text()                                                                                               |         |       |        |     |       |
   |   |   |   |   | declared_country           | string            |     | DeclaredCountry/text()                                                                                                     |         |       |        |     |       |
   |   |   |   |   | marriage_date              | datetime          |     | MarriageDate/text()                                                                                                        |         |       |        |     |       |
   |                                            |                   |     |                                                                                                                            |         |       |        |     |       |
   |   |   |   | BeXsltDsLimitArea4             |                   |     | /BE_FULL/ContractList/BE_FULL_contract/AssistanceProvider/BE_XSLT_DS_ASSISTANCE_PROVIDER/LimitAreas/BE_XSLT_DS_LIMIT_AREA  |         |       |        |     |       |
   |   |   |   |   | nar_code                   | string            |     | NAR_code/text()                                                                                                            |         |       |        |     |       |
   |   |   |   |   | name                       | string            |     | Name/text()                                                                                                                |         |       |        |     |       |
   |                                            |                   |     |                                                                                                                            |         |       |        |     |       |
   |   |   |   | BeXsltDsLimitArea5             |                   |     | /BE_FULL/ContractList/BE_FULL_contract/AssistanceProvider/BE_XSLT_DS_ASSISTANCE_PROVIDER/Limit_Areas/BE_XSLT_DS_LIMIT_AREA |         |       |        |     |       |
   |   |   |   |   | nar_code                   | string            |     | NAR_code/text()                                                                                                            |         |       |        |     |       |
   |   |   |   |   | name                       | string            |     | Name/text()                                                                                                                |         |       |        |     |       |
   |                                            |                   |     |                                                                                                                            |         |       |        |     |       |
   |   |   |   | BeXsltDsAssistanceProvider     |                   |     | /BE_FULL/ContractList/BE_FULL_contract/AssistanceProvider/BE_XSLT_DS_ASSISTANCE_PROVIDER                                   |         |       |        |     |       |
   |   |   |   |   | type_id                    | integer required  |     | Type_Id/text()                                                                                                             |         |       |        |     |       |
   |   |   |   |   | name                       | string            |     | Name/text()                                                                                                                |         |       |        |     |       |
   |   |   |   |   | last_name                  | string            |     | Last_Name/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | code                       | string            |     | Code/text()                                                                                                                |         |       |        |     |       |
   |   |   |   |   | city_district              | string            |     | City_District/text()                                                                                                       |         |       |        |     |       |
   |   |   |   |   | street_village             | string            |     | Street_Village/text()                                                                                                      |         |       |        |     |       |
   |   |   |   |   | country                    | string            |     | Country/text()                                                                                                             |         |       |        |     |       |
   |   |   |   |   | birth_date                 | datetime          |     | Birth_Date/text()                                                                                                          |         |       |        |     |       |
   |   |   |   |   | gender                     | string            |     | Gender/text()                                                                                                              |         |       |        |     |       |
   |   |   |   |   | death_date                 | datetime          |     | Death_Date/text()                                                                                                          |         |       |        |     |       |
   |   |   |   |   | status                     | string            |     | Status/text()                                                                                                              |         |       |        |     |       |
   |   |   |   |   | editing_state              | integer required  |     | EditingState/text()                                                                                                        |         |       |        |     |       |
   |                                            |                   |     |                                                                                                                            |         |       |        |     |       |
   |   |   |   | BEFULLContract                 |                   |     | /BE_FULL/ContractList/BE_FULL_contract                                                                                     |         |       |        |     |       |
   |   |   |   |   | statusas                   | string            |     | statusas/text()                                                                                                            |         |       |        |     |       |
   |   |   |   |   | registration_date          | datetime          |     | RegistrationDate/text()                                                                                                    |         |       |        |     |       |
   |   |   |   |   | first_reg_date             | datetime          |     | FirstRegDate/text()                                                                                                        |         |       |        |     |       |
   |   |   |   |   | document_kindid            | integer required  |     | DocumentKindID/text()                                                                                                      |         |       |        |     |       |
   |   |   |   |   | rejected_reasonid          | integer           |     | RejectedReasonID/text()                                                                                                    |         |       |        |     |       |
   |   |   |   |   | date_received              | datetime          |     | DateReceived/text()                                                                                                        |         |       |        |     |       |
   |   |   |   |   | contract_approve_date      | datetime          |     | ContractApproveDate/text()                                                                                                 |         |       |        |     |       |
   |   |   |   |   | contract_approve_place     | string            |     | ContractApprovePlace/text()                                                                                                |         |       |        |     |       |
   |   |   |   |   | ident_code                 | string            |     | IdentCode/text()                                                                                                           |         |       |        |     |       |
   |   |   |   |   | notary_reg_no              | string            |     | NotaryRegNo/text()                                                                                                         |         |       |        |     |       |
   |   |   |   |   | approver_name              | string            |     | ApproverName/text()                                                                                                        |         |       |        |     |       |
   |   |   |   |   | approver_last_name         | string            |     | ApproverLastName/text()                                                                                                    |         |       |        |     |       |
   |   |   |   |   | approver_street_village    | string            |     | ApproverStreetVillage/text()                                                                                               |         |       |        |     |       |
   |   |   |   |   | approver_city_district     | string            |     | ApproverCityDistrict/text()                                                                                                |         |       |        |     |       |
   |   |   |   |   | approver_type_id           | integer required  |     | ApproverTypeId/text()                                                                                                      |         |       |        |     |       |
   |   |   |   |   | approver_company_name      | string            |     | ApproverCompanyName/text()                                                                                                 |         |       |        |     |       |
   |   |   |   |   | approver_position          | string            |     | ApproverPosition/text()                                                                                                    |         |       |        |     |       |
   |   |   |   |   | error_correction           | integer           |     | ErrorCorrection/text()                                                                                                     |         |       |        |     |       |
   |                                            |                   |     |                                                                                                                            |         |       |        |     |       |
   |   |   |   | BeFull                         |                   |     | /BE_FULL                                                                                                                   |         |       |        |     |       |
   |   |   |   |   | title1                     | string            |     | title1/text()                                                                                                              |         |       |        |     |       |
   |   |   |   |   | title2                     | string            |     | title2/text()                                                                                                              |         |       |        |     |       |
   |   |   |   |   | printeddate                | string            |     | printeddate/text()                                                                                                         |         |       |        |     |       |
   |   |   |   |   | searchparameter1           | string            |     | searchparameter1/text()                                                                                                    |         |       |        |     |       |
   |   |   |   |   | searchparameter2           | string            |     | searchparameter2/text()                                                                                                    |         |       |        |     |       |
   |   |   |   |   | searchparameter3           | string            |     | searchparameter3/text()                                                                                                    |         |       |        |     |       |
   |   |   |   |   | statusas                   | string            |     | statusas/text()                                                                                                            |         |       |        |     |       |
   |   |   |   |   | preorder_statusas          | string            |     | PreorderStatusas/text()                                                                                                    |         |       |        |     |       |
   |   |   |   |   | contract_statusas          | string            |     | ContractStatusas/text()                                                                                                    |         |       |        |     |       |
"""
    path = tmp_path / 'manifest.xsd'
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table


def test_xsd_rc5550(rc: RawConfig, tmp_path: Path):
    # recursion in XSD
    xsd = """
<s:schema xmlns:s="http://www.w3.org/2001/XMLSchema" xmlns:tns="http://rc/ireg/1.0/" targetNamespace="http://rc/ireg/1.0/" elementFormDefault="qualified" attributeFormDefault="unqualified">
	<s:element name="data">
		<s:complexType>
			<s:sequence>
				<s:element minOccurs="1" maxOccurs="1" name="responseCode">
					<s:simpleType>
						<s:restriction base="s:int">
							<s:enumeration value="-1"/>
							<s:enumeration value="0"/>
							<s:enumeration value="1"/>
						</s:restriction>
					</s:simpleType>
				</s:element>
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

            <s:element minOccurs="1" maxOccurs="1" name="parent_code">
				<s:annotation>
					<s:documentation>Tėvinės paslaugos kodas (RC kodas)</s:documentation>
				</s:annotation>
			</s:element>

            <s:element minOccurs="1" maxOccurs="1" name="pasis_code">
				<s:annotation>
					<s:documentation>PASIS kodas</s:documentation>
				</s:annotation>
			</s:element>

			<s:element name="type" minOccurs="1" maxOccurs="1">
				<s:simpleType>
					<s:annotation>
						<s:documentation>Paslaugos tipas. Reikšmės: public-service - Viešoji paslauga, administrative-service - Administracinė paslauga, neither-public-nor-admin-service - Nei viešoji, nei administracinė paslauga</s:documentation>
					</s:annotation>
					<s:restriction base="s:string">
						<s:enumeration value="public-service"/>
						<s:enumeration value="administrative-service"/>
						<s:enumeration value="neither-public-nor-admin-service"/>
					</s:restriction>
				</s:simpleType>
			</s:element>

			<s:element name="who_may_consitute" minOccurs="1" maxOccurs="1">
				<s:simpleType>
					<s:annotation>
						<s:documentation>Įgaliojimą gali sudaryti. Reikšmės: fiz - Paslaugos, kurias per įgaliotinį gali gauti tik fizinis asmuo (kai įgaliotiniui nereikia notarinio įgaliojimo), fiz-notarial - Paslaugos, kurias per įgaliotinį gali gauti tik fizinis asmuo (kai įgaliotinis pateikia notarinį įgaliojimą), jur - Paslaugos, kurias per įgaliotinį gali gauti tik juridinis asmuo (kai įgaliotiniui nereikia notarinio įgaliojimo), jur-notarial - Paslaugos, kurias per įgaliotinį gali gauti tik juridinis asmuo (kai įgaliotinis pateikia notarinį įgaliojimą), fiz-jur - Paslaugos, kurias per įgaliotinį gali gauti tiek fizinis, tiek juridinis asmuo (kai abiem atvejais įgaliotiniui nereikia notarinio įgaliojimo), fiz-notarial-jur-notarial - Paslaugos, kurias per įgaliotinį gali gauti tiek fizinis, tiek juridinis asmuo (kai abiem atvejais įgaliotiniui reikia notarinio įgaliojimo), fiz-notarial-jur - Paslaugos, kurias per įgaliotinį gali gauti tiek fizinis asmuo (kai įgaliotinis pateikia notarinį įgaliojimą), tiek juridinis asmuo (kai įgaliotiniui nereikia notarinio įgaliojimo), fiz-jur-notarial - Paslaugos, kurias per įgaliotinį gali gauti tiek fizinis asmuo (kai įgaliotiniui nereikia notarinio įgaliojimo), tiek juridinis asmuo (kai įgaliotinis pateikia notarinį įgaliojimą)</s:documentation>
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

			<s:element name="title_lt" minOccurs="1" maxOccurs="1">
				<s:annotation>
					<s:documentation>Paslaugos pavadinimas lietuvių kalba</s:documentation>
				</s:annotation>
			</s:element>

			<s:element name="title_en" minOccurs="1" maxOccurs="1">
				<s:annotation>
					<s:documentation>Paslaugos pavadinimas anglų kalba</s:documentation>
				</s:annotation>
			</s:element>

            <s:element name="default_description_lt" minOccurs="1" maxOccurs="1">
				<s:annotation>
					<s:documentation>Numatytasis paslaugos aprašymas lietuvių kalba</s:documentation>
				</s:annotation>
			</s:element>

            <s:element name="default_description_en" minOccurs="1" maxOccurs="1">
				<s:annotation>
					<s:documentation>Numatytasis paslaugos aprašymas anglų kalba</s:documentation>
				</s:annotation>
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

			<s:element name="valid_from" minOccurs="1" maxOccurs="1" type="s:date">
				<s:annotation>
					<s:documentation>Galioja nuo (YYYY-MM-DD)</s:documentation>
				</s:annotation>
			</s:element>

            <s:element name="valid_to" minOccurs="1" maxOccurs="1">
				<s:annotation>
					<s:documentation>Galioja iki (YYYY-MM-DD)</s:documentation>
				</s:annotation>
			</s:element>

            <s:element name="is_action" minOccurs="1" maxOccurs="1">
				<s:simpleType>
					<s:annotation>
						<s:documentation>Ar yra veiksmas? 0 - NE, 1 - TAIP</s:documentation>
					</s:annotation>
					<s:restriction base="s:string">
						<s:enumeration value="0"/>
						<s:enumeration value="1"/>
					</s:restriction>
				</s:simpleType>
			</s:element>

            <s:element minOccurs="1" maxOccurs="unbounded" name="children" type="tns:children" />
		</s:sequence>
	</s:complexType>
</s:schema>
"""

    table = """
id | d | r | b | m | property                     | type             | ref | source                              | prepare | level | access | uri | title | description
   | manifest                                     |                  |     |                                     |         |       |        |     |       |
   |   | resource1                                | xml              |     |                                     |         |       |        |     |       |
   |                                              |                  |     |                                     |         |       |        |     |       |
   |   |   |   | Action                           |                  |     | /data/responseData/actions/action   |         |       |        |     |       |
   |   |   |   |   | code                         | string required  |     | code/text()                         |         |       |        |     |       | Paslaugos kodas (RC kodas)
   |   |   |   |   | parent_code                  | string required  |     | parent_code/text()                  |         |       |        |     |       | Tėvinės paslaugos kodas (RC kodas)
   |   |   |   |   | pasis_code                   | string required  |     | pasis_code/text()                   |         |       |        |     |       | PASIS kodas
   |   |   |   |   | type                         | string required  |     | type/text()                         |         |       |        |     |       | Paslaugos tipas. Reikšmės: public-service - Viešoji paslauga, administrative-service - Administracinė paslauga, neither-public-nor-admin-service - Nei viešoji, nei administracinė paslauga
   |                                              | enum             |     | public-service                      |         |       |        |     |       |
   |                                              |                  |     | administrative-service              |         |       |        |     |       |
   |                                              |                  |     | neither-public-nor-admin-service    |         |       |        |     |       |
   |   |   |   |   | who_may_consitute            | string required  |     | who_may_consitute/text()            |         |       |        |     |       | Įgaliojimą gali sudaryti. Reikšmės: fiz - Paslaugos, kurias per įgaliotinį gali gauti tik fizinis asmuo (kai įgaliotiniui nereikia notarinio įgaliojimo), fiz-notarial - Paslaugos, kurias per įgaliotinį gali gauti tik fizinis asmuo (kai įgaliotinis pateikia notarinį įgaliojimą), jur - Paslaugos, kurias per įgaliotinį gali gauti tik juridinis asmuo (kai įgaliotiniui nereikia notarinio įgaliojimo), jur-notarial - Paslaugos, kurias per įgaliotinį gali gauti tik juridinis asmuo (kai įgaliotinis pateikia notarinį įgaliojimą), fiz-jur - Paslaugos, kurias per įgaliotinį gali gauti tiek fizinis, tiek juridinis asmuo (kai abiem atvejais įgaliotiniui nereikia notarinio įgaliojimo), fiz-notarial-jur-notarial - Paslaugos, kurias per įgaliotinį gali gauti tiek fizinis, tiek juridinis asmuo (kai abiem atvejais įgaliotiniui reikia notarinio įgaliojimo), fiz-notarial-jur - Paslaugos, kurias per įgaliotinį gali gauti tiek fizinis asmuo (kai įgaliotinis pateikia notarinį įgaliojimą), tiek juridinis asmuo (kai įgaliotiniui nereikia notarinio įgaliojimo), fiz-jur-notarial - Paslaugos, kurias per įgaliotinį gali gauti tiek fizinis asmuo (kai įgaliotiniui nereikia notarinio įgaliojimo), tiek juridinis asmuo (kai įgaliotinis pateikia notarinį įgaliojimą)
   |                                              | enum             |     | fiz                                 |         |       |        |     |       |
   |                                              |                  |     | fiz-notarial                        |         |       |        |     |       |
   |                                              |                  |     | jur                                 |         |       |        |     |       |
   |                                              |                  |     | jur-notarial                        |         |       |        |     |       |
   |                                              |                  |     | fiz-jur                             |         |       |        |     |       |
   |                                              |                  |     | fiz-notarial-jur-notarial           |         |       |        |     |       |
   |                                              |                  |     | fiz-notarial-jur                    |         |       |        |     |       |
   |                                              |                  |     | fiz-jur-notarial                    |         |       |        |     |       |
   |   |   |   |   | title_lt                     | string required  |     | title_lt/text()                     |         |       |        |     |       | Paslaugos pavadinimas lietuvių kalba
   |   |   |   |   | title_en                     | string required  |     | title_en/text()                     |         |       |        |     |       | Paslaugos pavadinimas anglų kalba
   |   |   |   |   | default_description_lt       | string required  |     | default_description_lt/text()       |         |       |        |     |       | Numatytasis paslaugos aprašymas lietuvių kalba
   |   |   |   |   | default_description_en       | string required  |     | default_description_en/text()       |         |       |        |     |       | Numatytasis paslaugos aprašymas anglų kalba
   |   |   |   |   | default_description_editable | string required  |     | default_description_editable/text() |         |       |        |     |       | Ar numatytasis aprašymas gali būti redaguojamas? 0 - NE, 1 - TAIP
   |                                              | enum             |     | 0                                   |         |       |        |     |       |
   |                                              |                  |     | 1                                   |         |       |        |     |       |
   |   |   |   |   | digital_service              | string required  |     | digital_service/text()              |         |       |        |     |       | El. paslauga. Reikšmės: digital - Tik elektroninė paslauga, analog - Tik neelektroninė paslauga, digital-or-analog - Elektroninė arba neelektroninė paslauga
   |                                              | enum             |     | digital                             |         |       |        |     |       |
   |                                              |                  |     | analog                              |         |       |        |     |       |
   |                                              |                  |     | digital-or-analog                   |         |       |        |     |       |
   |   |   |   |   | valid_from                   | date required    |     | valid_from/text()                   |         |       |        |     |       | Galioja nuo (YYYY-MM-DD)
   |   |   |   |   | valid_to                     | string required  |     | valid_to/text()                     |         |       |        |     |       | Galioja iki (YYYY-MM-DD)
   |   |   |   |   | is_action                    | string required  |     | is_action/text()                    |         |       |        |     |       | Ar yra veiksmas? 0 - NE, 1 - TAIP
   |                                              | enum             |     | 0                                   |         |       |        |     |       |
   |                                              |                  |     | 1                                   |         |       |        |     |       |
   |                                              |                  |     |                                     |         |       |        |     |       |
   |   |   |   | Data                             |                  |     | /data                               |         |       |        |     |       |
   |   |   |   |   | response_code                | integer required |     | responseCode/text()                 |         |       |        |     |       |
   |                                              | enum             |     | -1                                  |         |       |        |     |       |
   |                                              |                  |     | 0                                   |         |       |        |     |       |
   |                                              |                  |     | 1                                   |         |       |        |     |       |
   |   |   |   |   | response_message             | string           |     | responseMessage/text()              |         |       |        |     |       |
"""
    path = tmp_path / 'manifest.xsd'
    with open(path, "w") as xsd_file:
        xsd_file.write(xsd)
    manifest = load_manifest(rc, path)
    assert manifest == table
