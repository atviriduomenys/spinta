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
  | resource1                   | xml     |     |                                   |         |       |        |     |       |
  |   |   |  Administracinis    |         |     | /ADMINISTRACINIAI/ADMINISTRACINIS |         |       |        |     |       |
  |   |   |   | adm_kodas       | integer |     | ADM_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | adm_id          | integer |     | ADM_ID/text()                     |         |       |        |     |       |
  |   |   |   | tipas           | string  |     | TIPAS/text()                      |         |       |        |     |       |
  |   |   |   | tipo_santrumpa  | string  |     | TIPO_SANTRUMPA/text()             |         |       |        |     |       |
  |   |   |   | vardas_k        | string  |     | VARDAS_K/text()                   |         |       |        |     |       |
  |   |   |   | vardas_k_lot    | string  |     | VARDAS_K_LOT/text()               |         |       |        |     |       |
  |   |   |   | priklauso_kodas | integer |     | PRIKLAUSO_KODAS/text()            |         |       |        |     |       |
  |   |   |   | gyv_kodas       | integer |     | GYV_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | nuo             | date    |     | NUO/text()                        |         |       |        |     |       |
  |   |   |   | iki             | date    |     | IKI/text()                        |         |       |        |     |       |
  |   |   |   | adm_nuo         | date    |     | ADM_NUO/text()                    |         |       |        |     |       |
  |   |   |   | adm_iki         | date    |     | ADM_IKI/text()                    |         |       |        |     |       |
                                |         |     |                                   |         |       |        |     |       |
  |   |   |  Gyvenviete         |         |     | /GYVENVIETES/GYVENVIETE           |         |       |        |     |       |
  |   |   |   | gyv_kodas       | integer |     | GYV_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | gyv_id          | integer |     | GYV_ID/text()                     |         |       |        |     |       |
  |   |   |   | tipas           | string  |     | TIPAS/text()                      |         |       |        |     |       |
  |   |   |   | tipo_santrumpa  | string  |     | TIPO_SANTRUMPA/text()             |         |       |        |     |       |
  |   |   |   | vardas_v        | string  |     | VARDAS_V/text()                   |         |       |        |     |       |
  |   |   |   | vardas_v_lot    | string  |     | VARDAS_V_LOT/text()               |         |       |        |     |       |
  |   |   |   | vardas_k        | string  |     | VARDAS_K/text()                   |         |       |        |     |       |
  |   |   |   | vardas_k_lot    | string  |     | VARDAS_K_LOT/text()               |         |       |        |     |       |
  |   |   |   | adm_kodas       | integer |     | ADM_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | nuo             | date    |     | NUO/text()                        |         |       |        |     |       |
  |   |   |   | iki             | date    |     | IKI/text()                        |         |       |        |     |       |
  |   |   |   | gyv_nuo         | date    |     | GYV_NUO/text()                    |         |       |        |     |       |
  |   |   |   | gyv_iki         | date    |     | GYV_IKI/text()                    |         |       |        |     |       |
                                |         |     |                                   |         |       |        |     |       |
  |   |   |  Gatve              |         |     | /GATVES/GATVE                     |         |       |        |     |       |
  |   |   |   | gat_kodas       | integer |     | GAT_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | gat_id          | integer |     | GAT_ID/text()                     |         |       |        |     |       |
  |   |   |   | tipas           | string  |     | TIPAS/text()                      |         |       |        |     |       |
  |   |   |   | tipo_santrumpa  | string  |     | TIPO_SANTRUMPA/text()             |         |       |        |     |       |
  |   |   |   | vardas_k        | string  |     | VARDAS_K/text()                   |         |       |        |     |       |
  |   |   |   | vardas_k_lot    | string  |     | VARDAS_K_LOT/text()               |         |       |        |     |       |
  |   |   |   | gyv_kodas       | integer |     | GYV_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | nuo             | date    |     | NUO/text()                        |         |       |        |     |       |
  |   |   |   | iki             | date    |     | IKI/text()                        |         |       |        |     |       |
  |   |   |   | gat_nuo         | date    |     | GAT_NUO/text()                    |         |       |        |     |       |
  |   |   |   | gat_iki         | date    |     | GAT_IKI/text()                    |         |       |        |     |       |
                                |         |     |                                   |         |       |        |     |       |
  |   |   |  Adresas            |         |     | /ADRESAI/ADRESAS                  |         |       |        |     |       |
  |   |   |   | aob_kodas       | integer |     | AOB_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | aob_id          | integer |     | AOB_ID/text()                     |         |       |        |     |       |
  |   |   |   | gyv_kodas       | integer |     | GYV_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | gat_kodas       | integer |     | GAT_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | nr              | string  |     | NR/text()                         |         |       |        |     |       |
  |   |   |   | korpuso_nr      | string  |     | KORPUSO_NR/text()                 |         |       |        |     |       |
  |   |   |   | pasto_kodas     | string  |     | PASTO_KODAS/text()                |         |       |        |     |       |
  |   |   |   | nuo             | date    |     | NUO/text()                        |         |       |        |     |       |
  |   |   |   | iki             | date    |     | IKI/text()                        |         |       |        |     |       |
  |   |   |   | aob_nuo         | date    |     | AOB_NUO/text()                    |         |       |        |     |       |
  |   |   |   | aob_iki         | date    |     | AOB_IKI/text()                    |         |       |        |     |       |
                                |         |     |                                   |         |       |        |     |       |
  |   |   |  Patalpa            |         |     | /PATALPOS/PATALPA                 |         |       |        |     |       |
  |   |   |   | pat_kodas       | integer |     | PAT_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | pat_id          | integer |     | PAT_ID/text()                     |         |       |        |     |       |
  |   |   |   | aob_kodas       | integer |     | AOB_KODAS/text()                  |         |       |        |     |       |
  |   |   |   | patalpos_nr     | string  |     | PATALPOS_NR/text()                |         |       |        |     |       |
  |   |   |   | nuo             | date    |     | NUO/text()                        |         |       |        |     |       |
  |   |   |   | iki             | date    |     | IKI/text()                        |         |       |        |     |       |
  |   |   |   | pat_nuo         | date    |     | PAT_NUO/text()                    |         |       |        |     |       |
  |   |   |   | pat_iki         | date    |     | PAT_IKI/text()                    |         |       |        |     |       |
                                |         |     |                                   |         |       |        |     |       |
  |   |   |  Kodas              |         |     | /KODAI/KODAS                      |         |       |        |     |       |
  |   |   |   | pasto_kodas     | string  |     | PASTO_KODAS/text()                |         |       |        |     |       |
  |   |   |   | pasto_viet_pav  | string  |     | PASTO_VIET_PAV/text()             |         |       |        |     |       |
  |   |   |   | nuo             | date    |     | NUO/text()                        |         |       |        |     |       |
  |   |   |   | iki             | date    |     | IKI/text()                        |         |       |        |     |       |
  """
    a, b = a, b = compare_manifest(manifest, result, context)

