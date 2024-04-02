from pathlib import Path

from spinta import commands
from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest_and_context, compare_manifest


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

    path = tmp_path / 'manifest.xsd'
    path.write_text(xsd)
    context, manifest = load_manifest_and_context(rc, path)
    commands.get_dataset(context, manifest, "dataset1").resources["resource1"].external = "manifest.xsd"

    result = """
d | r | b | m | property        | type    | ref | source                            | prepare | level | access | uri | title | description
dataset1                        |         |     |                                   |         |       |        |     |       |
  | resource1                   | xml     |     | manifest.xsd                      |         |       |        |     |       |
                                |         |     |                                   |         |       |        |     |       |
  |   |   |  Administracinis    |         |     | /ADMINISTRACINIAI/ADMINISTRACINIS |         |       |        |     |       |
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
  |   |   |  Gyvenviete         |         |     | /GYVENVIETES/GYVENVIETE           |         |       |        |     |       |
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
  |   |   |  Gatve              |         |     | /GATVES/GATVE                     |         |       |        |     |       |
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
  |   |   |  Adresas            |         |     | /ADRESAI/ADRESAS                  |         |       |        |     |       |
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
  |   |   |  Patalpa            |         |     | /PATALPOS/PATALPA                 |         |       |        |     |       |
  |   |   |   | pat_kodas       | integer required |     | PAT_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | pat_id          | integer required |     | PAT_ID/text()                     |         |       |        |     |       |
  |   |   |   | aob_kodas       | integer required |     | AOB_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | patalpos_nr     | string required |     | PATALPOS_NR/text()                |         |       |        |     |       |
  |   |   |   | nuo             | date required |     | NUO/text()                        |         |       |        |     |       |
  |   |   |   | iki             | date required |     | IKI/text()                        |         |       |        |     |       |
  |   |   |   | pat_nuo         | date required |     | PAT_NUO/text()                    |         |       |        |     |       |
  |   |   |   | pat_iki         | date required |     | PAT_IKI/text()                    |         |       |        |     |       |
                                |         |     |                                   |         |       |        |     |       |
  |   |   |  Kodas              |         |     | /KODAI/KODAS                      |         |       |        |     |       |
  |   |   |   | pasto_kodas     | string required |     | PASTO_KODAS/text()                |         |       |        |     |       |
  |   |   |   | pasto_viet_pav  | string required |     | PASTO_VIET_PAV/text()             |         |       |        |     |       |
  |   |   |   | nuo             | date required |     | NUO/text()                        |         |       |        |     |       |
  |   |   |   | iki             | date required |     | IKI/text()                        |         |       |        |     |       |
  """

    a, b = compare_manifest(manifest, result, context)

    print(a)
    print(b)

    assert a == b
