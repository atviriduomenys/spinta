import uuid
from pathlib import Path

import pytest
from _pytest.fixtures import FixtureRequest

from spinta.auth import AdminToken
from spinta.backends.helpers import get_table_identifier
from spinta.cli.helpers.push.utils import get_data_checksum
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import encode_page_values_manually, listdata
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.utils import get_error_codes


def _prep_context(context: Context):
    context.set("auth.token", AdminToken())


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_search", "spinta_set_meta_fields"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields"],
    ],
)
@pytest.mark.manifests("internal_sql", "csv")
def test_getall(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | Country        |         |         |
      |   |   |   | name       | string  |         | open
      |   |   | City           |         |         |
      |   |   |   | name       | string  |         | open
      |   |   |   | country    | ref     | Country | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    _prep_context(context)
    app = create_test_client(context)
    app.authorize(scope)
    lithuania_id = "d3482081-1c30-43a4-ae6f-faf6a40c954a"
    vilnius_id = "3aed7394-18da-4c17-ac29-d501d5dd0ed7"
    app.post("/example/Country", json={"_id": lithuania_id, "name": "Lithuania"})
    app.post("/example/City", json={"_id": vilnius_id, "country": {"_id": lithuania_id}, "name": "Vilnius"})
    response = app.get("/example/City?select(_id, _revision, _type, country.name)").json()["_data"]
    revision = response[0]["_revision"]
    assert response == [
        {
            "_id": "3aed7394-18da-4c17-ac29-d501d5dd0ed7",
            "_revision": revision,
            "_type": "example/City",
            "country": {"name": "Lithuania"},
        },
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_getall_pagination_disabled(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type    | ref     | access  | uri
            example/getall/test      |         |         |         |
              |   |   | Test         |         | value   |         | 
              |   |   |   | value    | integer |         | open    | 
            """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/getall/test", ["create", "getall", "search"])
    app.post("/example/getall/test/Test", json={"value": 0})
    app.post("/example/getall/test/Test", json={"value": 3})
    resp_2 = app.post("/example/getall/test/Test", json={"value": 410}).json()
    app.post("/example/getall/test/Test", json={"value": 707})
    app.post("/example/getall/test/Test", json={"value": 1000})
    encoded_page = {"value": resp_2["value"], "_id": resp_2["_id"]}
    encoded_page = encode_page_values_manually(encoded_page)
    response = app.get(f"/example/getall/test/Test?page({encoded_page}, disable: true)")
    json_response = response.json()
    assert len(json_response["_data"]) == 5
    assert "_page" not in json_response


@pytest.mark.manifests("internal_sql", "csv")
def test_getall_pagination_enabled(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type    | ref     | access  | uri
            example/getall/test      |         |         |         |
              |   |   | Test         |         | value   |         | 
              |   |   |   | value    | integer |         | open    | 
            """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/getall/test", ["create", "getall", "search"])
    app.post("/example/getall/test/Test", json={"value": 0})
    app.post("/example/getall/test/Test", json={"value": 3})
    resp_2 = app.post("/example/getall/test/Test", json={"value": 410}).json()
    app.post("/example/getall/test/Test", json={"value": 707})
    app.post("/example/getall/test/Test", json={"value": 1000})
    encoded_page = {"value": resp_2["value"], "_id": resp_2["_id"]}
    encoded_page = encode_page_values_manually(encoded_page)
    response = app.get(f'/example/getall/test/Test?page("{encoded_page}")')
    json_response = response.json()
    assert len(json_response["_data"]) == 2
    assert "_page" in json_response


@pytest.mark.manifests("internal_sql", "csv")
def test_getall_pagination_disabled_in_config(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    rc = rc.fork({"enable_pagination": False})
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type    | ref     | access  | uri
            example/getall/test      |         |         |         |
              |   |   | Test         |         | value   |         | 
              |   |   |   | value    | integer |         | open    | 
            """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/getall/test", ["create", "getall", "search"])
    app.post("/example/getall/test/Test", json={"value": 0})
    app.post("/example/getall/test/Test", json={"value": 3})
    resp_2 = app.post("/example/getall/test/Test", json={"value": 410}).json()
    app.post("/example/getall/test/Test", json={"value": 707})
    app.post("/example/getall/test/Test", json={"value": 1000})
    encoded_page = {"value": resp_2["value"], "_id": resp_2["_id"]}
    encoded_page = encode_page_values_manually(encoded_page)
    response = app.get(f'/example/getall/test/Test?page("{encoded_page}")')
    json_response = response.json()
    assert len(json_response["_data"]) == 5
    assert "_page" not in json_response


@pytest.mark.manifests("internal_sql", "csv")
def test_getall_pagination_disabled_in_config_enabled_in_request(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    rc = rc.fork({"enable_pagination": False})
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type    | ref     | access  | uri
            example/getall/test      |         |         |         |
              |   |   | Test         |         | value   |         | 
              |   |   |   | value    | integer |         | open    | 
            """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/getall/test", ["create", "getall", "search"])
    app.post("/example/getall/test/Test", json={"value": 0})
    app.post("/example/getall/test/Test", json={"value": 3})
    resp_2 = app.post("/example/getall/test/Test", json={"value": 410}).json()
    app.post("/example/getall/test/Test", json={"value": 707})
    app.post("/example/getall/test/Test", json={"value": 1000})
    encoded_page = {"value": resp_2["value"], "_id": resp_2["_id"]}
    encoded_page = encode_page_values_manually(encoded_page)
    response = app.get(f'/example/getall/test/Test?page("{encoded_page}", disable:false)')
    json_response = response.json()
    assert len(json_response["_data"]) == 2
    assert "_page" in json_response


@pytest.mark.manifests("internal_sql", "csv")
def test_get_date(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type    | ref     | access  | uri
            example/date/test      |         |         |         |
              |   |   | Test         |         | date   |         | 
              |   |   |   | date    | date |         | open    | 
            """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/date/test", ["create", "getall", "search"])
    app.post("/example/date/test/Test", json={"date": "2020-01-01"})
    response = app.get("/example/date/test/Test")
    json_response = response.json()
    assert len(json_response["_data"]) == 1
    assert json_response["_data"][0]["date"] == "2020-01-01"


@pytest.mark.manifests("internal_sql", "csv")
def test_get_datetime(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type    | ref     | access  | uri
            example/datetime/test      |         |         |         |
              |   |   | Test         |         | date   |         | 
              |   |   |   | date    | datetime |         | open    | 
            """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/datetime/test", ["create", "getall", "search"])
    app.post("/example/datetime/test/Test", json={"date": "2020-01-01T10:00:10"})
    response = app.get("/example/datetime/test/Test")
    json_response = response.json()
    assert len(json_response["_data"]) == 1
    assert json_response["_data"][0]["date"] == "2020-01-01T10:00:10"


@pytest.mark.manifests("internal_sql", "csv")
def test_get_time(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type    | ref     | access  | uri
            example/time/test      |         |         |         |
              |   |   | Test         |         | date   |         | 
              |   |   |   | date    | time |         | open    | 
            """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/time/test", ["create", "getall", "search"])
    app.post("/example/time/test/Test", json={"date": "10:00:10"})
    response = app.get("/example/time/test/Test")
    json_response = response.json()
    assert len(json_response["_data"]) == 1
    assert json_response["_data"][0]["date"] == "10:00:10"


@pytest.mark.manifests("internal_sql", "csv")
def test_get_paginate_with_none_simple(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type    | ref     | access  | uri
            example/page/null/simple |         |         |         |
              |   |   | Test         |         | id      | open    | 
              |   |   |   | id       | integer |         |         | 
              |   |   |   | name     | string  |         |         |
            """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/page/null/simple", ["create", "getall", "search"])
    app.post("/example/page/null/simple/Test", json={"id": 0, "name": "Test0"})
    app.post("/example/page/null/simple/Test", json={"id": None, "name": "Test1"})
    app.post("/example/page/null/simple/Test", json={"id": 1, "name": "Test2"})
    app.post("/example/page/null/simple/Test", json={"id": 2, "name": "Test3"})

    response = app.get("/example/page/null/simple/Test?page(size:1)")
    json_response = response.json()
    assert len(json_response["_data"]) == 4
    assert listdata(response, "id", "name") == [
        (0, "Test0"),
        (1, "Test2"),
        (2, "Test3"),
        (None, "Test1"),
    ]

    response = app.get("/example/page/null/simple/Test?page(size:2)")
    json_response = response.json()
    assert len(json_response["_data"]) == 4
    assert listdata(response, "id", "name") == [
        (0, "Test0"),
        (1, "Test2"),
        (2, "Test3"),
        (None, "Test1"),
    ]

    response = app.get("/example/page/null/simple/Test?page(size:100)")
    json_response = response.json()
    assert len(json_response["_data"]) == 4
    assert listdata(response, "id", "name") == [
        (0, "Test0"),
        (1, "Test2"),
        (2, "Test3"),
        (None, "Test1"),
    ]

    response = app.get("/example/page/null/simple/Test?sort(id, name)&page(size:100)")
    json_response = response.json()
    assert len(json_response["_data"]) == 4
    assert listdata(response, "id", "name") == [
        (0, "Test0"),
        (1, "Test2"),
        (2, "Test3"),
        (None, "Test1"),
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_get_paginate_invalid_type(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type    | ref      | access  | uri
            example/page/invalid     |         |          |         |
              |   |   | Test         |         | id, name, ref | open    | 
              |   |   |   | id       | integer |          |         | 
              |   |   |   | name     | string  |          |         |
              |   |   |   | ref      | ref     | Ref      |         |
              |   |   | Ref          |         | id       | open    | 
              |   |   |   | id       | integer |          |         | 
            """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/page/invalid", ["create", "getall", "search"])
    app.post("/example/page/invalid/Test", json={"id": 0, "name": "Test0"})
    app.post("/example/page/invalid/Test", json={"id": 0, "name": "Test1"})
    app.post("/example/page/invalid/Test", json={"id": 0, "name": None})
    app.post("/example/page/invalid/Test", json={"id": 1, "name": "Test2"})
    app.post("/example/page/invalid/Test", json={"id": 2, "name": "Test3"})
    app.post("/example/page/invalid/Test", json={"id": None, "name": "Test"})
    app.post("/example/page/invalid/Test", json={"id": None, "name": "Test1"})
    app.post("/example/page/invalid/Test", json={"id": None, "name": None})

    response = app.get("/example/page/invalid/Test")
    json_response = response.json()
    assert len(json_response["_data"]) == 8
    assert "_page" in json_response
    assert listdata(response, "id", "name") == [
        (0, "Test0"),
        (0, "Test1"),
        (0, None),
        (1, "Test2"),
        (2, "Test3"),
        (None, "Test"),
        (None, "Test1"),
        (None, None),
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_get_paginate_with_none_multi_key(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type    | ref      | access  | uri
            example/page/null/multi  |         |          |         |
              |   |   | Test         |         | id, name | open    | 
              |   |   |   | id       | integer |          |         | 
              |   |   |   | name     | string  |          |         |
            """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/page/null/multi", ["create", "getall", "search"])
    app.post("/example/page/null/multi/Test", json={"id": 0, "name": "Test0"})
    app.post("/example/page/null/multi/Test", json={"id": 0, "name": "Test1"})
    app.post("/example/page/null/multi/Test", json={"id": 0, "name": None})
    app.post("/example/page/null/multi/Test", json={"id": 1, "name": "Test2"})
    app.post("/example/page/null/multi/Test", json={"id": 2, "name": "Test3"})
    app.post("/example/page/null/multi/Test", json={"id": None, "name": "Test"})
    app.post("/example/page/null/multi/Test", json={"id": None, "name": "Test1"})
    app.post("/example/page/null/multi/Test", json={"id": None, "name": None})

    response = app.get("/example/page/null/multi/Test?page(size:1)")
    json_response = response.json()
    assert len(json_response["_data"]) == 8
    assert listdata(response, "id", "name") == [
        (0, "Test0"),
        (0, "Test1"),
        (0, None),
        (1, "Test2"),
        (2, "Test3"),
        (None, "Test"),
        (None, "Test1"),
        (None, None),
    ]

    response = app.get("/example/page/null/multi/Test?page(size:3)")
    json_response = response.json()
    assert len(json_response["_data"]) == 8
    assert listdata(response, "id", "name") == [
        (0, "Test0"),
        (0, "Test1"),
        (0, None),
        (1, "Test2"),
        (2, "Test3"),
        (None, "Test"),
        (None, "Test1"),
        (None, None),
    ]

    response = app.get("/example/page/null/multi/Test?sort(-id,-name)&page(size:3)")
    json_response = response.json()
    assert len(json_response["_data"]) == 8
    assert listdata(response, "id", "name") == [
        (0, "Test0"),
        (0, "Test1"),
        (0, None),
        (1, "Test2"),
        (2, "Test3"),
        (None, "Test"),
        (None, "Test1"),
        (None, None),
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_search", "spinta_set_meta_fields"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields"],
    ],
)
def test_join_with_base(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property   | type                 | ref     | prepare   | access
    datasets/basetest          |                      |         |           |
      |   |   |   |            |                      |         |           |
      |   |   | Place          |                      | id      |           |
      |   |   |   | id         | integer              |         |           | open
      |   |   |   | name       | string               |         |           | open
      |   |   |   | koord      | geometry(point,4326) |         |           | open
      |   |   |   |            |                      |         |           |
      |   | Place              |                      | name    |           |
      |   |   |   |            |                      |         |           |
      |   |   | Location       |                      | id      |           |
      |   |   |   | id         | integer              |         |           | open
      |   |   |   | name       |                      |         |           | open
      |   |   |   | koord      |                      |         |           | open
      |   |   |   | population | integer              |         |           | open
      |   |   |   | type       | string               |         |           | open
      |   |   |   |            | enum                 |         | "city"    |
      |   |   |   |            |                      |         | "country" |
      |   |   |   |            |                      |         |           |
      |   | Location           |                      | name    |           |
      |   |   | Test           |                      | id      |           |
      |   |   |   | id         | integer              |         |           | open
      |   |   |   | name       |                      |         |           | open
      |   |   |   |            |                      |         |           |
      |   |   | Country        |                      | id      |           |
      |   |   |   | id         | integer              |         |           | open
      |   |   |   | name       |                      |         |           | open
      |   |   |   | population |                      |         |           | open
      |   |   |   |            |                      |         |           |
      |   |   | City           |                      | id      |           |
      |   |   |   | id         | integer              |         |           | open
      |   |   |   | name       |                      |         |           | open
      |   |   |   | population |                      |         |           | open
      |   |   |   | koord      |                      |         |           | open
      |   |   |   | type       |                      |         |           | open
      |   |   |   | country    | ref                  | Country |           | open
      |   |   |   | testfk     | ref                  | Test    |           | open
      |   |   |   |            |                      |         |           |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)
    LTU = "d55e65c6-97c9-4cd3-99ff-ae34e268289b"
    VLN = "2074d66e-0dfd-4233-b1ec-199abc994d0c"
    TST = "2074d66e-0dfd-4233-b1ec-199abc994d0e"

    resp = app.post(
        "/datasets/basetest/Place",
        json={
            "_id": LTU,
            "id": 1,
            "name": "Lithuania",
        },
    )
    assert resp.status_code == 201

    resp = app.post(
        "/datasets/basetest/Location",
        json={
            "_id": LTU,
            "id": 1,
            "population": 2862380,
        },
    )
    assert resp.status_code == 201

    resp = app.post(
        "/datasets/basetest/Country",
        json={
            "_id": LTU,
            "id": 1,
        },
    )
    assert resp.status_code == 201

    resp = app.post(
        "/datasets/basetest/Place",
        json={"_id": VLN, "id": 2, "name": "Vilnius", "koord": "SRID=4326;POINT (54.68677 25.29067)"},
    )
    assert resp.status_code == 201

    resp = app.post(
        "/datasets/basetest/Location",
        json={
            "_id": VLN,
            "id": 2,
            "population": 625349,
        },
    )
    assert resp.status_code == 201

    resp = app.post(
        "/datasets/basetest/Place",
        json={"_id": TST, "id": 3, "name": "TestFK", "koord": "SRID=4326;POINT (54.68677 25.29067)"},
    )
    assert resp.status_code == 201

    resp = app.post(
        "/datasets/basetest/Location",
        json={
            "_id": TST,
            "id": 3,
            "population": 625349,
        },
    )
    assert resp.status_code == 201

    resp = app.post(
        "/datasets/basetest/Test",
        json={
            "_id": TST,
            "id": 3,
        },
    )
    assert resp.status_code == 201

    resp = app.post(
        "/datasets/basetest/City", json={"_id": VLN, "id": 2, "country": {"_id": LTU}, "testfk": {"_id": TST}}
    )
    assert resp.status_code == 201

    resp = app.get("/datasets/basetest/Location?select(_id,id,name,population,type)")
    assert resp.status_code == 200
    assert listdata(resp, "name", sort="name") == ["Lithuania", "TestFK", "Vilnius"]

    resp = app.get("/datasets/basetest/City?select(id,name,country.name,testfk.name)")
    assert resp.status_code == 200
    assert listdata(resp, "name", "country.name", "testfk.name")[0] == ("Vilnius", "Lithuania", "TestFK")

    resp = app.get("/datasets/basetest/City?select(_id,id,name,population,type,koord)")
    assert resp.status_code == 200
    assert listdata(resp, "name", "population", "koord", sort="name")[0] == (
        "Vilnius",
        625349,
        "POINT (54.68677 25.29067)",
    )


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_search", "spinta_set_meta_fields"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields"],
    ],
)
def test_invalid_inherit_check(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property   | type                 | ref     | prepare   | access
    datasets/invalid/base          |                      |         |           |
      |   |   |   |            |                      |         |           |
      |   |   | Place          |                      | id      |           |
      |   |   |   | id         | integer              |         |           | open
      |   |   |   | name       | string               |         |           | open
      |   |   |   | koord      | geometry(point,4326) |         |           | open
      |   |   |   |            |                      |         |           |
      |   | Place              |                      | name    |           |
      |   |   |   |            |                      |         |           |
      |   |   | Location       |                      | id      |           |
      |   |   |   | id         | integer              |         |           | open
      |   |   |   | name       |                      |         |           | open
      |   |   |   | population | integer              |         |           | open
      |   |   |   |            |                      |         |           |
      |   | Location           |                      | name    |           |
      |   |   |   |            |                      |         |           |
      |   |   | Country        |                      | id      |           |
      |   |   |   | id         | integer              |         |           | open
      |   |   |   | name       |                      |         |           | open
      |   |   |   | population |                      |         |           | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)
    LTU = "d55e65c6-97c9-4cd3-99ff-ae34e268289b"

    resp = app.post(
        "/datasets/invalid/base/Place",
        json={
            "_id": LTU,
            "id": 1,
            "name": "Lithuania",
        },
    )
    assert resp.status_code == 201

    resp = app.post(
        "/datasets/invalid/base/Location",
        json={
            "_id": LTU,
            "id": 1,
            "population": 2862380,
        },
    )
    assert resp.status_code == 201

    resp = app.post("/datasets/invalid/base/Country", json={"_id": LTU, "id": 1, "name": "Lietuva"})
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InheritPropertyValueMissmatch"]

    resp = app.post("/datasets/invalid/base/Country", json={"_id": LTU, "id": 1, "name": "Lithuania"})
    assert resp.status_code == 201


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_search", "spinta_set_meta_fields"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields"],
    ],
)
def test_checksum_result(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property   | type                 | ref      | prepare   | access | level
    datasets/result/checksum   |                      |          |           |        |
      |   |   | Country        |                      | id, name |           |        |
      |   |   |   | id         | integer              |          |           | open   |
      |   |   |   | name       | string               |          |           | open   |
      |   |   |   | population | integer              |          |           | open   |
      |   |   |   |            |                      |          |           |        |
      |   |   | City           |                      | id       |           |        |
      |   |   |   | id         | integer              |          |           | open   |
      |   |   |   | name       | string               |          |           | open   |
      |   |   |   | country    | ref                  | Country  |           | open   |
      |   |   |   | add        | object               |          |           | open   |
      |   |   |   | add.name   | string               |          |           | open   |
      |   |   |   | add.add    | object               |          |           | open   |
      |   |   |   | add.add.c  | ref                  | Country  |           | open   | 3
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)
    lt_id = str(uuid.uuid4())
    lv_id = str(uuid.uuid4())

    country_data = {
        lt_id: {"id": 0, "name": "Lithuania", "population": 1000},
        lv_id: {"id": 1, "name": "Latvia", "population": 500},
    }

    resp = app.post("/datasets/result/checksum/Country", json={"_id": lt_id, **country_data[lt_id]})
    assert resp.status_code == 201
    resp = app.post("/datasets/result/checksum/Country", json={"_id": lv_id, **country_data[lv_id]})
    assert resp.status_code == 201

    vilnius_id = str(uuid.uuid4())
    kaunas_id = str(uuid.uuid4())
    riga_id = str(uuid.uuid4())
    liepaja_id = str(uuid.uuid4())

    city_data = {
        vilnius_id: {
            "id": 0,
            "name": "Vilnius",
            "country": {"_id": lt_id},
            "add": {"name": "VLN", "add": {"c": {"id": 0, "name": "Lithuania"}}},
        },
        kaunas_id: {
            "id": 1,
            "name": "Kaunas",
            "country": {"_id": lt_id},
            "add": {"name": "KN", "add": {"c": {"id": 0, "name": "Lithuania"}}},
        },
        riga_id: {
            "id": 2,
            "name": "Riga",
            "country": {"_id": lv_id},
            "add": {"name": "RG", "add": {"c": {"id": 1, "name": "Latvia"}}},
        },
        liepaja_id: {
            "id": 3,
            "name": "Liepaja",
            "country": {"_id": lv_id},
            "add": {"name": "LP", "add": {"c": {"id": 1, "name": "Latvia"}}},
        },
    }

    resp = app.post("/datasets/result/checksum/City", json={"_id": vilnius_id, **city_data[vilnius_id]})
    assert resp.status_code == 201
    resp = app.post("/datasets/result/checksum/City", json={"_id": kaunas_id, **city_data[kaunas_id]})
    assert resp.status_code == 201
    resp = app.post("/datasets/result/checksum/City", json={"_id": riga_id, **city_data[riga_id]})
    assert resp.status_code == 201
    resp = app.post("/datasets/result/checksum/City", json={"_id": liepaja_id, **city_data[liepaja_id]})
    assert resp.status_code == 201

    resp = app.get("/datasets/result/checksum/Country?select(_id, id, checksum())")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "checksum()", sort="id", full=True) == [
        {"_id": lt_id, "id": 0, "checksum()": get_data_checksum(country_data[lt_id])},
        {"_id": lv_id, "id": 1, "checksum()": get_data_checksum(country_data[lv_id])},
    ]

    resp = app.get("/datasets/result/checksum/City?select(_id, id, checksum())")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "checksum()", sort="id", full=True) == [
        {"_id": vilnius_id, "id": 0, "checksum()": get_data_checksum(city_data[vilnius_id])},
        {"_id": kaunas_id, "id": 1, "checksum()": get_data_checksum(city_data[kaunas_id])},
        {"_id": riga_id, "id": 2, "checksum()": get_data_checksum(city_data[riga_id])},
        {"_id": liepaja_id, "id": 3, "checksum()": get_data_checksum(city_data[liepaja_id])},
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_search", "spinta_set_meta_fields"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields"],
    ],
)
def test_checksum_geometry(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property   | type                 | ref      | prepare   | access | level
    datasets/geometry/checksum |                      |          |           |        |
      |   |   | Country        |                      | id, name |           |        |
      |   |   |   | id         | integer              |          |           | open   |
      |   |   |   | name       | string               |          |           | open   |
      |   |   |   | poly       | geometry(polygon)    |          |           | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)
    lt_id = str(uuid.uuid4())
    lv_id = str(uuid.uuid4())

    country_data = {
        lt_id: {"id": 0, "name": "Lithuania", "poly": "POLYGON ((80 50, 50 50, 50 80, 80 80, 80 50))"},
        lv_id: {"id": 1, "name": "Latvia", "poly": "POLYGON ((20 50, 50 20, 20 20, 20 50))"},
    }

    resp = app.post("/datasets/geometry/checksum/Country", json={"_id": lt_id, **country_data[lt_id]})
    assert resp.status_code == 201
    resp = app.post("/datasets/geometry/checksum/Country", json={"_id": lv_id, **country_data[lv_id]})
    assert resp.status_code == 201

    resp = app.get("/datasets/geometry/checksum/Country?select(id, name, poly)&sort(id)").json()
    lt_checksum = get_data_checksum(resp["_data"][0])
    lv_checksum = get_data_checksum(resp["_data"][1])

    resp = app.get("/datasets/geometry/checksum/Country?select(_id, id, checksum())")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "checksum()", sort="id", full=True) == [
        {"_id": lt_id, "id": 0, "checksum()": lt_checksum},
        {"_id": lv_id, "id": 1, "checksum()": lv_checksum},
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_search", "spinta_set_meta_fields"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields"],
    ],
)
def test_geometry_manifest_flip_select(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property   | type                 | ref | prepare | access | level
    datasets/geometry/flip     |                      |     |         |        |
      |   |   | Country        |                      | id  |         |        |
      |   |   |   | id         | integer              |     |         | open   |
      |   |   |   | name       | string               |     |         | open   |
      |   |   |   | poly       | geometry(polygon)    |     | flip()  | open   |
      |   |   |   | geo_lt     | geometry(3346)       |     | flip()  | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)
    lt_id = str(uuid.uuid4())

    resp = app.post(
        "/datasets/geometry/flip/Country",
        json={
            "_id": lt_id,
            "id": 0,
            "name": "Lithuania",
            "poly": "POLYGON ((80 50, 50 50, 50 80, 80 80, 80 50))",
            "geo_lt": "POINT (5980000 200000)",
        },
    )
    assert resp.status_code == 201

    resp = app.get("/datasets/geometry/flip/Country?select(id, name, poly, geo_lt)")
    assert resp.status_code == 200
    assert listdata(resp, "id", "name", "poly", "geo_lt", full=True) == [
        {
            "id": 0,
            "name": "Lithuania",
            "poly": "POLYGON ((50 80, 50 50, 80 50, 80 80, 50 80))",
            "geo_lt": "POINT (200000 5980000)",
        },
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_search", "spinta_set_meta_fields"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields"],
    ],
)
def test_geometry_manifest_flip(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property   | type                 | ref | prepare | access | level
    datasets/geometry/flip     |                      |     |         |        |
      |   |   | Country        |                      | id  |         |        |
      |   |   |   | id         | integer              |     |         | open   |
      |   |   |   | name       | string               |     |         | open   |
      |   |   |   | poly       | geometry(polygon)    |     | flip()  | open   |
      |   |   |   | geo_lt     | geometry(3346)       |     | flip()  | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)
    lt_id = str(uuid.uuid4())

    resp = app.post(
        "/datasets/geometry/flip/Country",
        json={
            "_id": lt_id,
            "id": 0,
            "name": "Lithuania",
            "poly": "POLYGON ((80 50, 50 50, 50 80, 80 80, 80 50))",
            "geo_lt": "POINT (5980000 200000)",
        },
    )
    assert resp.status_code == 201

    resp = app.get("/datasets/geometry/flip/Country")
    assert resp.status_code == 200
    assert listdata(resp, "id", "name", "poly", "geo_lt", full=True) == [
        {
            "id": 0,
            "name": "Lithuania",
            "poly": "POLYGON ((50 80, 50 50, 80 50, 80 80, 50 80))",
            "geo_lt": "POINT (200000 5980000)",
        },
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_search", "spinta_set_meta_fields"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields"],
    ],
)
def test_geometry_select_flip(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property      | type                 | ref | prepare | access | level
    datasets/geometry/flip/normal |                      |     |         |        |
      |   |   | Country           |                      | id  |         |        |
      |   |   |   | id            | integer              |     |         | open   |
      |   |   |   | name          | string               |     |         | open   |
      |   |   |   | poly          | geometry(polygon)    |     |         | open   |
      |   |   |   | geo_lt        | geometry(3346)       |     |         | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)
    lt_id = str(uuid.uuid4())

    resp = app.post(
        "/datasets/geometry/flip/normal/Country",
        json={
            "_id": lt_id,
            "id": 0,
            "name": "Lithuania",
            "poly": "POLYGON ((80 50, 50 50, 50 80, 80 80, 80 50))",
            "geo_lt": "POINT (5980000 200000)",
        },
    )
    assert resp.status_code == 201

    resp = app.get(
        "/datasets/geometry/flip/normal/Country?select(id, name, flip(poly), flip(geo_lt), flip(flip(poly)), flip(flip(flip(poly))))"
    )
    assert resp.status_code == 200
    assert listdata(resp, full=True) == [
        {
            "id": 0,
            "name": "Lithuania",
            "flip(poly)": "POLYGON ((50 80, 50 50, 80 50, 80 80, 50 80))",
            "flip(flip(poly))": "POLYGON ((80 50, 50 50, 50 80, 80 80, 80 50))",
            "flip(flip(flip(poly)))": "POLYGON ((50 80, 50 50, 80 50, 80 80, 50 80))",
            "flip(geo_lt)": "POINT (200000 5980000)",
        },
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_search", "spinta_set_meta_fields"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields"],
    ],
)
def test_geometry_combined_flip(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property      | type                 | ref | prepare | access | level
    datasets/geometry/flip        |                      |     |         |        |
      |   |   | Country           |                      | id  |         |        |
      |   |   |   | id            | integer              |     |         | open   |
      |   |   |   | name          | string               |     |         | open   |
      |   |   |   | poly          | geometry(polygon)    |     | flip()  | open   |
      |   |   |   | geo_lt        | geometry(3346)       |     | flip()  | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)
    lt_id = str(uuid.uuid4())

    resp = app.post(
        "/datasets/geometry/flip/Country",
        json={
            "_id": lt_id,
            "id": 0,
            "name": "Lithuania",
            "poly": "POLYGON ((80 50, 50 50, 50 80, 80 80, 80 50))",
            "geo_lt": "POINT (5980000 200000)",
        },
    )
    assert resp.status_code == 201

    resp = app.get(
        "/datasets/geometry/flip/Country?select(poly, flip(poly), flip(flip(poly)), geo_lt, flip(geo_lt), flip(flip(geo_lt)))"
    )
    assert resp.status_code == 200
    assert listdata(resp, full=True) == [
        {
            "poly": "POLYGON ((50 80, 50 50, 80 50, 80 80, 50 80))",
            "flip(poly)": "POLYGON ((80 50, 50 50, 50 80, 80 80, 80 50))",
            "flip(flip(poly))": "POLYGON ((50 80, 50 50, 80 50, 80 80, 50 80))",
            "geo_lt": "POINT (200000 5980000)",
            "flip(geo_lt)": "POINT (5980000 200000)",
            "flip(flip(geo_lt))": "POINT (200000 5980000)",
        },
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_search", "spinta_set_meta_fields"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields"],
    ],
)
def test_geometry_manifest_flip_invalid_bbox(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property   | type                 | ref | prepare | access | level
    datasets/geometry/flip     |                      |     |         |        |
      |   |   | Country        |                      | id  |         |        |
      |   |   |   | id         | integer              |     |         | open   |
      |   |   |   | name       | string               |     |         | open   |
      |   |   |   | poly       | geometry(polygon)    |     | flip()  | open   |
      |   |   |   | geo_lt     | geometry(3346)       |     | flip()  | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)
    lt_id = str(uuid.uuid4())

    resp = app.post(
        "/datasets/geometry/flip/Country",
        json={
            "_id": lt_id,
            "id": 0,
            "name": "Lithuania",
            "poly": "POLYGON ((80 50, 50 50, 50 80, 80 80, 80 50))",
            "geo_lt": "POINT (200000 5980000)",
        },
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["CoordinatesOutOfRange"]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_search", "spinta_set_meta_fields"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields"],
    ],
)
def test_geometry_point(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property   | type     | ref | prepare                 | access | level
    datasets/geometry/point    |          |     |                         |        |
      |   |   | Country        |          | id  |                         |        |
      |   |   |   | id         | integer  |     |                         | open   |
      |   |   |   | name       | string   |     |                         | open   |
      |   |   |   | coord_x    | number   |     |                         | open   |
      |   |   |   | coord_y    | number   |     |                         | open   |
      |   |   |   | geo        | geometry |     | point(coord_x, coord_y) | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)
    lt_id = str(uuid.uuid4())

    resp = app.post(
        "/datasets/geometry/point/Country",
        json={"_id": lt_id, "id": 0, "name": "Lithuania", "coord_x": 10.5, "coord_y": 20.95},
    )
    assert resp.status_code == 201

    resp = app.get("/datasets/geometry/point/Country")
    assert resp.status_code == 200
    assert listdata(resp, "id", "name", "coord_x", "coord_y", "geo", full=True) == [
        {"id": 0, "name": "Lithuania", "coord_x": 10.5, "coord_y": 20.95, "geo": "POINT (10.5 20.95)"},
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_search", "spinta_set_meta_fields"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields"],
    ],
)
def test_filter_lithuanian_letters(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property   | type     | ref | prepare                 | access | level
    datasets/filters/chars     |          |     |                         |        |
      |   |   | City           |          | id  |                         |        |
      |   |   |   | id         | integer  |     |                         | open   |
      |   |   |   | name       | string   |     |                         | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)

    app.post(
        "/datasets/filters/chars/City",
        json={
            "id": 0,
            "name": "Šiauliai",
        },
    )
    app.post(
        "/datasets/filters/chars/City",
        json={
            "id": 1,
            "name": "Šilutė",
        },
    )
    app.post(
        "/datasets/filters/chars/City",
        json={
            "id": 2,
            "name": "Sydney",
        },
    )

    resp = app.get('/datasets/filters/chars/City?name.contains("Š")')
    assert resp.status_code == 200
    assert listdata(resp, "id", "name", full=True) == [
        {
            "id": 0,
            "name": "Šiauliai",
        },
        {
            "id": 1,
            "name": "Šilutė",
        },
    ]

    resp = app.get('/datasets/filters/chars/City?name.contains("ė")')
    assert resp.status_code == 200
    assert listdata(resp, "id", "name", full=True) == [
        {
            "id": 1,
            "name": "Šilutė",
        },
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_search", "spinta_set_meta_fields"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields"],
    ],
)
def test_swap_ufunc(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property   | type     | ref | prepare                 | access | level
    datasets/geometry/point    |          |     |                         |        |
      |   |   | Country        |          | id  |                         |        |
      |   |   |   | id         | integer  |     |                         | open   |
      |   |   |   | name       | string   |     |                         | open   |
      |   |   |   |            | enum     |     | swap('nan', '---')      | open   |
      |   |   |   |            |          |     | swap(null, '---')       | open   |
      |   |   |   |            |          |     | swap('', '---')         | open   |
      |   |   |   |            |          |     | swap('---', '--')       | open   |
      |   |   |   |            |          |     | '---'                   | open   |
      |   |   |   |            |          |     | '--'                    | open   |
      |   |   |   |            |          |     | 'test'                  | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)

    for _id, name in enumerate(("nan", "test", "", None)):
        resp = app.post("/datasets/geometry/point/Country", json={"id": _id, "name": name})
        assert resp.status_code == 201, resp.text

    resp = app.get("/datasets/geometry/point/Country")
    assert resp.status_code == 200, resp.text
    assert listdata(resp, "id", "name", full=True) == [
        {"id": 0, "name": "---"},
        {"id": 1, "name": "test"},
        {"id": 2, "name": "---"},
        {"id": 3, "name": "---"},
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_search", "spinta_set_meta_fields"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields"],
    ],
)
def test_replace_source_with_prepare(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property   | type     | ref | source | prepare | access | level
    datasets/geometry/point    |          |     |        |         |        |
      |   |   | Country        |          | id  |        |         |        |
      |   |   |   | id         | integer  |     |        |         | open   |
      |   |   |   | name       | string   |     |        |         | open   |
      |   |   |   |            | enum     |     |  1     | 'x1'    | open   |
      |   |   |   |            |          |     |  2     | 'x2'    | open   |
      |   |   |   |            |          |     |  3     | 'x3'    | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)

    for _id in range(1, 5):
        resp = app.post("/datasets/geometry/point/Country", json={"id": _id, "name": str(_id)})
        assert resp.status_code == 201, resp.text

    resp = app.post("/datasets/geometry/point/Country", json={"id": 6, "name": None})
    assert resp.status_code == 201, resp.text
    resp = app.post("/datasets/geometry/point/Country", json={"id": 7, "name": ""})
    assert resp.status_code == 201, resp.text

    resp = app.get("/datasets/geometry/point/Country")
    assert resp.status_code == 200, resp.text
    assert listdata(resp, "id", "name", full=True) == [
        {"id": 1, "name": "x1"},
        {"id": 2, "name": "x2"},
        {"id": 3, "name": "x3"},
        {"id": 4, "name": "4"},
        {"id": 6, "name": None},
        {"id": 7, "name": ""},
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_search", "spinta_set_meta_fields"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields"],
    ],
)
def test_null_under_prepare(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property   | type     | ref | source | prepare | access | level
    datasets/geometry/point    |          |     |        |         |        |
      |   |   | Country        |          | id  |        |         |        |
      |   |   |   | id         | integer  |     |        |         | open   |
      |   |   |   | name       | string   |     |        |         | open   |
      |   |   |   |            | enum     |     |        | null    | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)

    resp = app.post("/datasets/geometry/point/Country", json={"id": 1, "name": None})
    assert resp.status_code == 201, resp.text
    resp = app.post("/datasets/geometry/point/Country", json={"id": 2, "name": ""})
    assert resp.status_code == 201, resp.text

    resp = app.get("/datasets/geometry/point/Country")
    assert resp.status_code == 200, resp.text
    assert listdata(resp, "id", "name", full=True) == [
        {"id": 1, "name": None},
        {"id": 2, "name": ""},
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getone", "spinta_wipe", "spinta_search", "spinta_set_meta_fields", "spinta_move"],
        ["uapi:/:create", "uapi:/:getone", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields", "uapi:/:move"],
    ],
)
def test_getone_redirect(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property   | type     | ref | prepare                 | access | level
    datasets/redirect          |          |     |                         |        |
      |   |   | Country        |          | id  |                         |        |
      |   |   |   | id         | integer  |     |                         | open   |
      |   |   |   | name       | string   |     |                         | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)
    lt_id = str(uuid.uuid4())
    new_lt_id = str(uuid.uuid4())
    assert lt_id != new_lt_id

    resp = app.post("/datasets/redirect/Country", json={"_id": lt_id, "id": 0, "name": "Lithuania"})
    assert resp.status_code == 201
    resp = app.post("/datasets/redirect/Country", json={"_id": new_lt_id, "id": 1, "name": "Lithuania"})
    assert resp.status_code == 201

    resp = app.get(f"/datasets/redirect/Country/{lt_id}")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", full=True) == [
        {
            "_id": lt_id,
            "id": 0,
            "name": "Lithuania",
        },
    ]

    resp = app.request(
        "delete",
        f"/datasets/redirect/Country/{lt_id}/:move",
        json={"_id": new_lt_id, "_revision": resp.json()["_revision"]},
    )
    assert listdata(resp, "_id", "_same_as", full=True) == [
        {
            "_id": lt_id,
            "_same_as": new_lt_id,
        },
    ]
    assert resp.status_code == 200

    resp = app.get(f"/datasets/redirect/Country/{lt_id}")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", full=True) == [
        {
            "_id": new_lt_id,
            "id": 1,
            "name": "Lithuania",
        },
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        [
            "spinta_insert",
            "spinta_getone",
            "spinta_delete",
            "spinta_wipe",
            "spinta_search",
            "spinta_set_meta_fields",
            "spinta_move",
            "spinta_getall",
        ],
        [
            "uapi:/:create",
            "uapi:/:getone",
            "uapi:/:delete",
            "uapi:/:wipe",
            "uapi:/:search",
            "uapi:/:set_meta_fields",
            "uapi:/:move",
            "uapi:/:getall",
        ],
    ],
)
def test_getone_potential_redirect_loop(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    """
    Redirect loop potential case:
    DATA:
        ca76ec2a-f8ca-4cb5-87ae-d801e143844b
        f3ccada0-3104-4ab3-9fa4-d9a09f6ee55e

    REDIRECT MAPPING:
        EMPTY

    if we do these steps, there could potentially be redirect issues, if it's not handled properly:
    1.  DELETE "/Model/ca76ec2a-f8ca-4cb5-87ae-d801e143844b/:move" {"_id": "f3ccada0-3104-4ab3-9fa4-d9a09f6ee55e"}
        DATA:
            f3ccada0-3104-4ab3-9fa4-d9a09f6ee55e

        REDIRECT MAPPING:
            ca76ec2a-f8ca-4cb5-87ae-d801e143844b -> f3ccada0-3104-4ab3-9fa4-d9a09f6ee55e
    2.  POST "/Model" {"_id": "ca76ec2a-f8ca-4cb5-87ae-d801e143844b"}
        DATA:
            ca76ec2a-f8ca-4cb5-87ae-d801e143844b
            f3ccada0-3104-4ab3-9fa4-d9a09f6ee55e
    3.  DELETE "/Model/f3ccada0-3104-4ab3-9fa4-d9a09f6ee55e/:move" {"_id": "ca76ec2a-f8ca-4cb5-87ae-d801e143844b"}
        DATA:
            ca76ec2a-f8ca-4cb5-87ae-d801e143844b

        REDIRECT MAPPING (POTENTIALY):
            ca76ec2a-f8ca-4cb5-87ae-d801e143844b -> f3ccada0-3104-4ab3-9fa4-d9a09f6ee55e
            f3ccada0-3104-4ab3-9fa4-d9a09f6ee55e -> ca76ec2a-f8ca-4cb5-87ae-d801e143844b
    4.  DELETE "/Model/ca76ec2a-f8ca-4cb5-87ae-d801e143844b"
        DATA:
            EMPTY
    5.  GET "/Model/ca76ec2a-f8ca-4cb5-87ae-d801e143844b"
        GET "/Model/f3ccada0-3104-4ab3-9fa4-d9a09f6ee55e"

        This has potential to be redirect loop if redirect mapping is not handled correctly when data with existing
        redirect id is added back and/or data is deleted while it's being used in redirect table
    """

    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property   | type     | ref | prepare                 | access | level
    datasets/redirect          |          |     |                         |        |
      |   |   | Country        |          | id  |                         |        |
      |   |   |   | id         | integer  |     |                         | open   |
      |   |   |   | name       | string   |     |                         | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)
    lt_id = str(uuid.uuid4())
    new_lt_id = str(uuid.uuid4())
    assert lt_id != new_lt_id

    resp_old = app.post("/datasets/redirect/Country", json={"_id": lt_id, "id": 0, "name": "Lithuania"})
    assert resp_old.status_code == 201
    resp_new = app.post("/datasets/redirect/Country", json={"_id": new_lt_id, "id": 1, "name": "Lithuania"})
    assert resp_new.status_code == 201

    # Move lt to new_lt
    resp = app.request(
        "delete",
        f"/datasets/redirect/Country/{lt_id}/:move",
        json={"_id": new_lt_id, "_revision": resp_old.json()["_revision"]},
    )
    assert listdata(resp, "_id", "_same_as", full=True) == [
        {
            "_id": lt_id,
            "_same_as": new_lt_id,
        },
    ]
    assert resp.status_code == 200

    resp = app.get(f"/datasets/redirect/Country/{lt_id}")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", full=True) == [
        {
            "_id": new_lt_id,
            "id": 1,
            "name": "Lithuania",
        },
    ]

    # Create new lt entry
    resp = app.post("/datasets/redirect/Country", json={"_id": lt_id, "id": 0, "name": "Lithuania"})
    assert resp.status_code == 201

    # Now move new_lt to lt
    resp = app.request(
        "delete",
        f"/datasets/redirect/Country/{new_lt_id}/:move",
        json={"_id": lt_id, "_revision": resp_new.json()["_revision"]},
    )
    assert listdata(resp, "_id", "_same_as", full=True) == [
        {
            "_id": new_lt_id,
            "_same_as": lt_id,
        },
    ]
    assert resp.status_code == 200

    # Delete lt entry
    resp = app.delete(f"/datasets/redirect/Country/{lt_id}")
    assert resp.status_code == 204

    resp = app.get("/datasets/redirect/Country")
    assert resp.status_code == 200

    # Should be empty
    assert listdata(resp) == []

    # Should return errors, that item is not found, and not redirect loop
    resp = app.get(f"/datasets/redirect/Country/{lt_id}")
    assert resp.status_code == 404
    assert get_error_codes(resp.json()) == ["ItemDoesNotExist"]

    resp = app.get(f"/datasets/redirect/Country/{new_lt_id}")
    assert resp.status_code == 404
    assert get_error_codes(resp.json()) == ["ItemDoesNotExist"]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getone", "spinta_wipe", "spinta_search", "spinta_set_meta_fields", "spinta_move"],
        ["uapi:/:create", "uapi:/:getone", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields", "uapi:/:move"],
    ],
)
def test_getone_invalid_value_missing_redirect(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property   | type     | ref | prepare                 | access | level
    datasets/redirect          |          |     |                         |        |
      |   |   | Country        |          | id  |                         |        |
      |   |   |   | id         | integer  |     |                         | open   |
      |   |   |   | name       | string   |     |                         | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)
    lt_id = str(uuid.uuid4())

    country_redirect = get_table_identifier("datasets/redirect/Country/:redirect")
    manifest = context.get("store").manifest
    with manifest.backend.begin() as conn:
        conn.execute(f"""
            DROP TABLE IF EXISTS {country_redirect.pg_escaped_qualified_name};
        """)

    resp = app.get(f"/datasets/redirect/Country/{lt_id}")
    assert resp.status_code == 500
    assert get_error_codes(resp.json()) == ["RedirectFeatureMissing", "ItemDoesNotExist"]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_search", "spinta_set_meta_fields"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields"],
    ],
)
def test_filter_boolean_values(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property   | type     | ref | prepare                 | access | level
    datasets/filters/bools     |          |     |                         |        |
      |   |   | City           |          | id  |                         |        |
      |   |   |   | id         | integer  |     |                         | open   |
      |   |   |   | is_value   | boolean  |     |                         | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)

    app.post(
        "/datasets/filters/bools/City",
        json={"id": 0, "is_value": True},
    )
    app.post(
        "/datasets/filters/bools/City",
        json={"id": 1, "is_value": False},
    )
    app.post(
        "/datasets/filters/bools/City",
        json={"id": 2, "is_value": None},
    )

    resp = app.get("/datasets/filters/bools/City?is_value=true")
    assert resp.status_code == 200
    assert listdata(resp, "id", "is_value", full=True) == [{"id": 0, "is_value": True}]

    resp = app.get("/datasets/filters/bools/City?is_value=false")
    assert resp.status_code == 200
    assert listdata(resp, "id", "is_value", full=True) == [{"id": 1, "is_value": False}]

    resp = app.get("/datasets/filters/bools/City?is_value!=true")
    assert resp.status_code == 200
    assert listdata(resp, "id", "is_value", full=True) == [
        {"id": 1, "is_value": False},
        {"id": 2, "is_value": None},
    ]

    resp = app.get("/datasets/filters/bools/City?is_value!=false")
    assert resp.status_code == 200
    assert listdata(resp, "id", "is_value", full=True) == [
        {"id": 0, "is_value": True},
        {"id": 2, "is_value": None},
    ]

    resp = app.get("/datasets/filters/bools/City?is_value=null")
    assert resp.status_code == 200
    assert listdata(resp, "id", "is_value", full=True) == [{"id": 2, "is_value": None}]

    resp = app.get("/datasets/filters/bools/City?is_value!=null")
    assert resp.status_code == 200
    assert listdata(resp, "id", "is_value", full=True) == [
        {"id": 0, "is_value": True},
        {"id": 1, "is_value": False},
    ]
