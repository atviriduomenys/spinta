from pathlib import Path
from unittest.mock import ANY


from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.testing.client import create_test_client
from spinta.testing.manifest import prepare_manifest


def test_composite_ref_two_levels_returns_data(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <participant>
            <code>P001</code>
            <asset_code>AT001</asset_code>
            <asset_name>Equipment</asset_name>
        </participant>
        <participant>
            <code>P002</code>
            <asset_code>AT002</asset_code>
            <asset_name>Building</asset_name>
        </participant>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property             | type       | ref       | source              | access | level
    example                              |            |           |                     |        |
      | data                             | dask/xml   |           | {path}              |        |
      |   |   | AssetType                |            | code      | /root/participant   |        | 5
      |   |   |   | code                 | string     |           | asset_code          | open   | 5
      |   |   |   | name                 | string     |           | asset_name          | open   | 5
      |   |   | Participant              |            |           | /root/participant   |        | 5
      |   |   |   | code                 | string     |           | code                | open   | 5
      |   |   |   | asset_type           | ref required | AssetType | asset_code         | open   | 5
      |   |   |   | asset_type.code      | string     |           | asset_code          | open   | 5
      |   |   |   | asset_type.name      | string     |           | asset_name          | open   | 5
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Participant", ["getall"])
    app.authmodel("example/AssetType", ["getall"])

    resp = app.get("/example/Participant")
    assert resp.status_code == 200

    data = resp.json()["_data"]
    assert data == [
        {
            "_type": "example/Participant",
            "_id": ANY,
            "_revision": None,
            "code": "P001",
            "asset_type": {"_id": ANY, "name": "Equipment"},
        },
        {
            "_type": "example/Participant",
            "_id": ANY,
            "_revision": None,
            "code": "P002",
            "asset_type": {"_id": ANY, "name": "Building"},
        },
    ]


def test_composite_ref_three_levels_xyz(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <order>
            <id>ORD001</id>
            <vendor_code>VEND001</vendor_code>
            <country_code>LT</country_code>
            <country_name>Lithuania</country_name>
        </order>
        <order>
            <id>ORD002</id>
            <vendor_code>VEND002</vendor_code>
            <country_code>PL</country_code>
            <country_name>Poland</country_name>
        </order>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                | type       | ref      | source              | access
    example                                 |            |          |                     |
      | data                                | dask/xml   |          | {path}              |
      |   |   | Country                     |            | code     | /root/order         |
      |   |   |   | code                    | string     |          | country_code        | open
      |   |   |   | name                    | string     |          | country_name        | open
      |   |   | Vendor                      |            | code     | /root/order         |
      |   |   |   | code                    | string     |          | vendor_code         | open
      |   |   |   | country                 | ref required | Country | country_code        | open
      |   |   |   | country.code            | string     |          | country_code        | open
      |   |   |   | country.name            | string     |          | country_name        | open
      |   |   | Order                       |            | id       | /root/order         |
      |   |   |   | id                      | string     |          | id                  | open
      |   |   |   | vendor                  | ref required | Vendor  | vendor_code         | open
      |   |   |   | vendor.country.code     | string     |          | country_code        | open
      |   |   |   | vendor.country.name     | string     |          | country_name        | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Order", ["getall"])
    app.authmodel("example/Vendor", ["getall"])
    app.authmodel("example/Country", ["getall"])

    resp = app.get("/example/Order")
    assert resp.status_code == 200
    data = resp.json()["_data"]

    # Verify x.y.z composite properties work
    assert len(data) == 2
    assert data[0]["id"] == "ORD001"
    assert data[0]["vendor"]["_id"] == ANY
    assert data[0]["vendor"]["code"] == "VEND001"
    assert data[0]["vendor"]["country"]["code"] == "LT"
    assert data[0]["vendor"]["country"]["name"] == "Lithuania"

    assert data[1]["id"] == "ORD002"
    assert data[1]["vendor"]["_id"] == ANY
    assert data[1]["vendor"]["code"] == "VEND002"
    assert data[1]["vendor"]["country"]["code"] == "PL"
    assert data[1]["vendor"]["country"]["name"] == "Poland"


def test_composite_ref_four_levels_xyze(rc: RawConfig, tmp_path: Path):
    """Test composite ref properties at four levels (x.y.z.e).

    This test validates that the fix works with very deep property chains:
    - order -> vendor (ref) -> country (ref) -> region
    - Accessing order.vendor.country.region.code as x.y.z.e property
    """
    xml = """
    <root>
        <order>
            <id>ORD001</id>
            <vendor_code>VEND001</vendor_code>
            <country_code>LT</country_code>
            <region_code>REG001</region_code>
            <region_name>Vilnius Region</region_name>
        </order>
        <order>
            <id>ORD002</id>
            <vendor_code>VEND002</vendor_code>
            <country_code>PL</country_code>
            <region_code>REG002</region_code>
            <region_name>Warsaw Region</region_name>
        </order>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                      | type       | ref      | source              | access
    example                                       |            |          |                     |
      | data                                      | dask/xml   |          | {path}              |
      |   |   | Region                            |            | code     | /root/order         |
      |   |   |   | code                          | string     |          | region_code         | open
      |   |   |   | name                          | string     |          | region_name         | open
      |   |   | Country                           |            | code     | /root/order         |
      |   |   |   | code                          | string     |          | country_code        | open
      |   |   |   | region                        | ref required | Region  | region_code         | open
      |   |   |   | region.code                   | string     |          | region_code         | open
      |   |   |   | region.name                   | string     |          | region_name         | open
      |   |   | Vendor                            |            | code     | /root/order         |
      |   |   |   | code                          | string     |          | vendor_code         | open
      |   |   |   | country                       | ref required | Country | country_code        | open
      |   |   |   | country.code                  | string     |          | country_code        | open
      |   |   |   | country.region.code           | string     |          | region_code         | open
      |   |   |   | country.region.name           | string     |          | region_name         | open
      |   |   | Order                             |            | id       | /root/order         |
      |   |   |   | id                            | string     |          | id                  | open
      |   |   |   | vendor                        | ref required | Vendor  | vendor_code         | open
      |   |   |   | vendor.country.code           | string     |          | country_code        | open
      |   |   |   | vendor.country.region.code    | string     |          | region_code         | open
      |   |   |   | vendor.country.region.name    | string     |          | region_name         | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Order", ["getall"])
    app.authmodel("example/Vendor", ["getall"])
    app.authmodel("example/Country", ["getall"])
    app.authmodel("example/Region", ["getall"])

    resp = app.get("/example/Order")
    assert resp.status_code == 200
    data = resp.json()["_data"]

    # Verify x.y.z.e composite properties work
    assert len(data) == 2
    assert data[0]["id"] == "ORD001"
    assert data[0]["vendor"]["_id"] == ANY
    assert data[0]["vendor"]["code"] == "VEND001"
    assert data[0]["vendor"]["country"]["code"] == "LT"
    assert data[0]["vendor"]["country"]["region"]["code"] == "REG001"
    assert data[0]["vendor"]["country"]["region"]["name"] == "Vilnius Region"

    assert data[1]["id"] == "ORD002"
    assert data[1]["vendor"]["_id"] == ANY
    assert data[1]["vendor"]["code"] == "VEND002"
    assert data[1]["vendor"]["country"]["code"] == "PL"
    assert data[1]["vendor"]["country"]["region"]["code"] == "REG002"
    assert data[1]["vendor"]["country"]["region"]["name"] == "Warsaw Region"
