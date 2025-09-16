from spinta.testing.client import create_test_client
from spinta.testing.manifest import bootstrap_manifest
from _pytest.fixtures import FixtureRequest
import xml.etree.ElementTree as ET
import pytest


@pytest.mark.manifests("internal_sql", "csv")
def test_getall_level4(manifest_type, rc, tmp_path, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type    | ref     | access | level
    example/lvl4             |         |         |        |
      |   |   | Country      |         |         |        |
      |   |   |   | name     | string  |         | open   |
      |   |   |   | cities[] | ref     | City    | open   | 4
      |   |   | City         |         | name    |        |
      |   |   |   | name     | string  |         | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields", "spinta_getone"])
    app.authmodel("example/lvl4/City", ["insert"])
    vilnius_id = "e06708e5-7857-46f5-95e7-36fa7bcf1c1c"
    kaunas_id = "214ec691-dea6-4a0b-8f51-ee61eaa6117e"
    app.post("/example/lvl4/City", json={"_id": vilnius_id, "name": "Vilnius"})
    app.post("/example/lvl4/City", json={"_id": kaunas_id, "name": "Kaunas"})

    lietuva_id = "6ffebdd7-2c5f-4e1f-b38b-e7a5c35e7216"
    app.authmodel("example/lvl4/Country", ["insert", "getall", "search"])
    app.post(
        "/example/lvl4/Country",
        json={"_id": lietuva_id, "name": "Lietuva", "cities": [{"_id": vilnius_id}, {"_id": kaunas_id}]},
    )

    result = app.get("/example/lvl4/Country")
    assert result.json()["_data"][0]["cities"] == []

    result = app.get("/example/lvl4/Country?expand()")
    assert result.json()["_data"][0]["cities"] == [
        {"_id": vilnius_id},
        {"_id": kaunas_id},
    ]

    result = app.get(f"/example/lvl4/Country/{lietuva_id}")
    assert result.json()["cities"] == [
        {"_id": vilnius_id},
        {"_id": kaunas_id},
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_getall_level3(manifest_type, rc, tmp_path, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type    | ref     | access | level
    example/lvl3             |         |         |        |
      |   |   | Country      |         |         |        |
      |   |   |   | name     | string  |         | open   |
      |   |   |   | cities[] | ref     | City    | open   | 3
      |   |   | City         |         | name    |        |
      |   |   |   | name     | string  |         | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields", "spinta_getone"])
    app.authmodel("example/lvl3/City", ["insert"])
    vilnius_id = "e06708e5-7857-46f5-95e7-36fa7bcf1c1c"
    kaunas_id = "214ec691-dea6-4a0b-8f51-ee61eaa6117e"
    app.post("/example/lvl3/City", json={"_id": vilnius_id, "name": "Vilnius"})
    app.post("/example/lvl3/City", json={"_id": kaunas_id, "name": "Kaunas"})

    lietuva_id = "6ffebdd7-2c5f-4e1f-b38b-e7a5c35e7216"
    app.authmodel("example/lvl3/Country", ["insert", "getall", "search"])
    app.post(
        "/example/lvl3/Country",
        json={"_id": lietuva_id, "name": "Lietuva", "cities": [{"name": "Vilnius"}, {"name": "Kaunas"}]},
    )

    result = app.get("/example/lvl3/Country")
    assert result.json()["_data"][0]["cities"] == []

    result = app.get("/example/lvl3/Country?expand()")
    assert result.json()["_data"][0]["cities"] == [
        {"name": "Vilnius"},
        {"name": "Kaunas"},
    ]

    result = app.get(f"/example/lvl3/Country/{lietuva_id}")
    assert result.json()["cities"] == [
        {"name": "Vilnius"},
        {"name": "Kaunas"},
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_getall_simple_type(manifest_type, rc, tmp_path, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type    | ref     | access | level
    example/simple           |         |         |        |
      |   |   | Country      |         |         |        |
      |   |   |   | name     | string  |         | open   |
      |   |   |   | cities[] | string  |         | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields", "spinta_getone"])

    lietuva_id = "6ffebdd7-2c5f-4e1f-b38b-e7a5c35e7216"
    app.authmodel("example/simple/Country", ["insert", "getall", "search"])
    app.post("/example/simple/Country", json={"_id": lietuva_id, "name": "Lietuva", "cities": ["Vilnius", "Kaunas"]})

    result = app.get(f"/example/simple/Country/{lietuva_id}")
    assert result.json()["cities"] == ["Vilnius", "Kaunas"]


@pytest.mark.manifests("internal_sql", "csv")
def test_array_shortcut_inherit_access_open(manifest_type, rc, tmp_path, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property    | type    | ref      | access
    example/dtypes/array/open        |         |          |
                                |         |          |
      |   |   | Language        |         |          |
      |   |   |   | name        | string  |          | open
                                |         |          |
      |   |   | Country         |         |          |
      |   |   |   | name        | string  |          | open
      |   |   |   | languages[] | ref     | Language | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("example/dtypes/array/open", ["insert", "getone", "getall"])

    LIT = "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
    app.post(
        "example/dtypes/array/open/Language",
        json={
            "_id": LIT,
            "name": "Lithuanian",
        },
    )

    LT = "d73306fb-4ee5-483d-9bad-d86f98e1869c"
    app.post("example/dtypes/array/open/Country", json={"_id": LT, "name": "Lithuania", "languages": [{"_id": LIT}]})

    result = app.get("/example/dtypes/array/open/Country")
    assert result.json()["_data"][0]["languages"] == []

    result = app.get("/example/dtypes/array/open/Country?expand()")
    assert result.json()["_data"][0]["languages"] == [
        {"_id": LIT},
    ]

    result = app.get(f"/example/dtypes/array/open/Country/{LT}")
    assert result.json()["languages"] == [
        {"_id": LIT},
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_array_shortcut_inherit_access_private(manifest_type, rc, tmp_path, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property    | type    | ref      | access
    example/dtypes/array/private        |         |          |
                                |         |          |
      |   |   | Language        |         |          |
      |   |   |   | name        | string  |          | open
                                |         |          |
      |   |   | Country         |         |          |
      |   |   |   | name        | string  |          | open
      |   |   |   | languages[] | ref     | Language | private
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("example/dtypes/array/private", ["insert", "getone", "getall"])

    LIT = "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
    app.post(
        "example/dtypes/array/private/Language",
        json={
            "_id": LIT,
            "name": "Lithuanian",
        },
    )

    LT = "d73306fb-4ee5-483d-9bad-d86f98e1869c"
    app.post("example/dtypes/array/private/Country", json={"_id": LT, "name": "Lithuania", "languages": [{"_id": LIT}]})
    result = app.get("/example/dtypes/array/private/Country")
    assert "languages" not in result.json()["_data"][0]

    result = app.get("/example/dtypes/array/private/Country?expand()")
    assert "languages" not in result.json()["_data"][0]

    result = app.get(f"/example/dtypes/array/private/Country/{LT}")
    assert "languages" not in result.json()


@pytest.mark.manifests("internal_sql", "csv")
def test_array_select_only_array(manifest_type, rc, tmp_path, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property    | type    | ref      | access
    example/dtypes/array/one    |         |          |
                                |         |          |
      |   |   | Language        |         |          |
      |   |   |   | name        | string  |          | open
                                |         |          |
      |   |   | Country         |         |          |
      |   |   |   | name        | string  |          | open
      |   |   |   | languages[] | ref     | Language | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("example/dtypes/array/one", ["insert", "getone", "getall", "search"])

    LIT = "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
    app.post(
        "example/dtypes/array/one/Language",
        json={
            "_id": LIT,
            "name": "Lithuanian",
        },
    )

    LT = "d73306fb-4ee5-483d-9bad-d86f98e1869c"
    app.post("example/dtypes/array/one/Country", json={"_id": LT, "name": "Lithuania", "languages": [{"_id": LIT}]})
    result = app.get("/example/dtypes/array/one/Country?select(languages)")
    assert result.json()["_data"] == [{"languages": []}]

    result = app.get("/example/dtypes/array/one/Country?select(languages)&expand()")
    assert result.json()["_data"] == [{"languages": [{"_id": LIT}]}]


@pytest.mark.skip(reason="format support not implemented")
def test_array_rdf(
    manifest_type: str,
    tmp_path,
    rc,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property    | type    | ref      | access | level
    example/dtypes/array/rdf    |         |          |        |
                                |         |          |        |
      |   |   | Item            |         | id, name |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | tags[]      | string  |          | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("example/dtypes/array/rdf", ["insert", "getone", "getall"])

    item1_id = "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
    item2_id = "1cccf6f6-9fe2-4055-b003-915a7c1abee8"
    app.post(
        "example/dtypes/array/rdf/Item", json={"_id": item1_id, "id": 0, "name": "Item1", "tags": ["tag1", "tag2"]}
    )
    app.post("example/dtypes/array/rdf/Item", json={"_id": item2_id, "id": 1, "name": "Item2", "tags": ["tag3"]})

    resp = app.get("example/dtypes/array/rdf/Item/:format/rdf?expand()")
    assert resp.status_code == 200

    root = ET.fromstring(resp.text)
    rdf_descriptions = root.findall("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description")
    version_values = [desc.attrib["{http://purl.org/pav/}version"] for desc in rdf_descriptions]
    assert (
        resp.text == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns="https://testserver/">\n'
        f'<rdf:Description rdf:about="/example/dtypes/array/rdf/Item/c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0" rdf:type="example/dtypes/array/rdf/Item" pav:version="{version_values[0]}">\n'
        f"  <_page>WzAsICJJdGVtMSIsICJjOGU0Y2Q2MC0wYjE1LTRiMjMtYTY5MS0wOWNkZjJlYmQ5YzAiXQ==</_page>\n"
        f"  <id>0</id>\n"
        f"  <name>Item1</name>\n"
        f"  <tags>['tag1', 'tag2']</tags>\n"
        f"</rdf:Description>\n"
        f'<rdf:Description rdf:about="/example/dtypes/array/rdf/Item/1cccf6f6-9fe2-4055-b003-915a7c1abee8" rdf:type="example/dtypes/array/rdf/Item" pav:version="{version_values[1]}">\n'
        f"  <_page>WzEsICJJdGVtMiIsICIxY2NjZjZmNi05ZmUyLTQwNTUtYjAwMy05MTVhN2MxYWJlZTgiXQ==</_page>\n"
        f"  <id>1</id>\n"
        f"  <name>Item2</name>\n"
        f"  <tags>['tag3']</tags>\n"
        f"</rdf:Description>\n"
        f"</rdf:RDF>\n"
    )
