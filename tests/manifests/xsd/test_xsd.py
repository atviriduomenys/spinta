from pathlib import Path

from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest


def test_xsd(context, rc: RawConfig, tmp_path: Path):

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


# def test_xsd_rc729(context, rc: RawConfig, tmp_path: Path):
#
#     xsd = """
#
# <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
#
#
# <xs:element name="klientu_saraso_rezultatas">
#   <xs:complexType mixed="true">
#     <xs:sequence>
#       <xs:element ref="asmenys"               minOccurs="0" maxOccurs="1" />
#     </xs:sequence>
#   </xs:complexType>
# </xs:element>
#
#
# <xs:element name="klaida" type="xs:string">
#   <xs:annotation><xs:documentation>Klaidos atveju - klaidos pranešimas</xs:documentation></xs:annotation>
# </xs:element>
#
#
# <xs:element name="asmenys">
#   <xs:complexType mixed="true">
#     <xs:sequence>
#       <xs:element ref="asmuo"                 minOccurs="0" maxOccurs="unbounded" />
#     </xs:sequence>
#
#     <xs:attribute name="puslapis" type="xs:long" use="required">
#       <xs:annotation><xs:documentation>rezultatu puslapio numeris</xs:documentation></xs:annotation>
#     </xs:attribute>
#
#     <xs:attribute name="viso_puslapiu" type="xs:long" use="required">
#       <xs:annotation><xs:documentation>rezultatu puslapiu skaicius</xs:documentation></xs:annotation>
#     </xs:attribute>
#
#   </xs:complexType>
# </xs:element>
#
#
#
# <xs:element name="asmuo">
#   <xs:complexType mixed="true">
#
#       <xs:attribute name="id"     type="xs:string" use="required">
#       </xs:attribute>
#
#       <xs:attribute name="ak"  type="xs:string" use="required">
#       </xs:attribute>
#
#   </xs:complexType>
# </xs:element>
#
#
# </xs:schema>
#     """
#
#     table = """
# id | d | r | b | m | property                | type             | ref      | source                                   | prepare | level | access | uri | title | description
#    | manifest                                |                  |          |                                          |         |       |        |     |       |
#    |   | resource1                           | xml              |          |                                          |         |       |        |     |       |
#    |                                         |                  |          |                                          |         |       |        |     |       |
#    |   |   |   | /Resource                   |                  |          | /                                        |         |       |        | http://www.w3.org/2000/01/rdf-schema#Resource |       | Įvairūs duomenys
#    |   |   |   |   | klaida                  | string           |          | klaida/text()                            |         |       |        |     |       | Klaidos atveju - klaidos pranešimas
#    |                                         |                  |          |                                          |         |       |        |     |       |
#    |   |   |   | Asmuo                       |                  |          | /klientu_saraso_rezultatas/asmenys/asmuo |         |       |        |     |       |
#    |   |   |   |   | asmenys                 | ref unique       | Asmenys  |                                          |         |       |        |     |       |
#    |   |   |   |   | id                      | string required  |          | @id                                      |         |       |        |     |       |
#    |   |   |   |   | ak                      | string required  |          | @ak                                      |         |       |        |     |       |
#    |   |   |   |   | text                    | string           |          | text()                                   |         |       |        |     |       |
#    |                                         |                  |          |                                          |         |       |        |     |       |
#    |   |   |   | Asmenys                     |                  |          | /klientu_saraso_rezultatas/asmenys       |         |       |        |     |       |
#    |   |   |   |   | puslapis                | integer required |          | @puslapis                                |         |       |        |     |       | rezultatu puslapio numeris
#    |   |   |   |   | viso_puslapiu           | integer required |          | @viso_puslapiu                           |         |       |        |     |       | rezultatu puslapiu skaicius
#    |   |   |   |   | text                    | string           |          | text()                                   |         |       |        |     |       |
#    |   |   |   |   | asmuo[]                 | backref          | Asmuo    |                                          |         |       |        |     |       |
#    |                                         |                  |          |                                          |         |       |        |     |       |
#    |   |   |   | KlientuSarasoRezultatas     |                  |          | /klientu_saraso_rezultatas               |         |       |        |     |       |
#    |   |   |   |   | text                    | string           |          | text()                                   |         |       |        |     |       |
#    |   |   |   |   | asmenys                 | ref              | Asmenys  |                                          |         |       |        |     |       |
#    |                                         |                  |          |                                          |         |       |        |     |       |
#    |   |   |   | Asmuo1                      |                  |          | /asmenys/asmuo                           |         |       |        |     |       |
#    |   |   |   |   | asmenys1                | ref unique       | Asmenys1 |                                          |         |       |        |     |       |
#    |   |   |   |   | id                      | string required  |          | @id                                      |         |       |        |     |       |
#    |   |   |   |   | ak                      | string required  |          | @ak                                      |         |       |        |     |       |
#    |   |   |   |   | text                    | string           |          | text()                                   |         |       |        |     |       |
#    |                                         |                  |          |                                          |         |       |        |     |       |
#    |   |   |   | Asmenys1                    |                  |          | /asmenys                                 |         |       |        |     |       |
#    |   |   |   |   | puslapis                | integer required |          | @puslapis                                |         |       |        |     |       | rezultatu puslapio numeris
#    |   |   |   |   | viso_puslapiu           | integer required |          | @viso_puslapiu                           |         |       |        |     |       | rezultatu puslapiu skaicius
#    |   |   |   |   | text                    | string           |          | text()                                   |         |       |        |     |       |
#    |   |   |   |   | asmuo1[]                | backref          | Asmuo1   |                                          |         |       |        |     |       |
#    |                                         |                  |          |                                          |         |       |        |     |       |
#    |   |   |   | Asmuo2                      |                  |          | /asmuo                                   |         |       |        |     |       |
#    |   |   |   |   | id                      | string required  |          | @id                                      |         |       |        |     |       |
#    |   |   |   |   | ak                      | string required  |          | @ak                                      |         |       |        |     |       |
#    |   |   |   |   | text                    | string           |          | text()                                   |         |       |        |     |       |
#   """
#
#     path = tmp_path / 'manifest.xsd'
#     with open(path, "w") as xsd_file:
#         xsd_file.write(xsd)
#     manifest = load_manifest(rc, path)
#     print(manifest)
#     assert manifest == table

def test_xsd_rc729_variant(context, rc: RawConfig, tmp_path: Path):

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
