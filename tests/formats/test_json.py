import datetime
import hashlib
import uuid
from pathlib import Path

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.core.config import RawConfig
import pytest
from _pytest.fixtures import FixtureRequest
from starlette.datastructures import Headers

from spinta.testing.client import create_test_client
from spinta.testing.data import pushdata, encode_page_values_manually
from spinta.testing.manifest import bootstrap_manifest


def sha1(s):
    return hashlib.sha1(s.encode()).hexdigest()


@pytest.mark.skip("datasets")
def test_export_json(app, mocker):
    mocker.patch(
        "spinta.backends.postgresql.dataset.utcnow", return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308)
    )

    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("country/:dataset/csv/:resource/countries", ["upsert", "getall", "search", "getone", "changes"])

    resp = app.post(
        "/country/:dataset/csv/:resource/countries",
        json={
            "_data": [
                {
                    "_op": "upsert",
                    "_type": "country/:dataset/csv/:resource/countries",
                    "_id": "69a33b149af7a7eeb25026c8cdc09187477ffe21",
                    "_where": '_id="69a33b149af7a7eeb25026c8cdc09187477ffe21"',
                    "code": "lt",
                    "title": "Lithuania",
                },
                {
                    "_op": "upsert",
                    "_type": "country/:dataset/csv/:resource/countries",
                    "_id": sha1("2"),
                    "_where": '_id="' + sha1("2") + '"',
                    "code": "lv",
                    "title": "LATVIA",
                },
                {
                    "_op": "upsert",
                    "_type": "country/:dataset/csv/:resource/countries",
                    "_id": sha1("2"),
                    "_where": '_id="' + sha1("2") + '"',
                    "code": "lv",
                    "title": "Latvia",
                },
            ]
        },
    )
    assert resp.status_code == 200, resp.json()
    data = resp.json()
    revs = [d["_revision"] for d in data["_data"]]

    assert app.get("/country/:dataset/csv/:resource/countries/:format/json?sort(+code)").json() == {
        "_data": [
            {
                "_type": "country/:dataset/csv/:resource/countries",
                "_id": "69a33b149af7a7eeb25026c8cdc09187477ffe21",
                "_revision": revs[0],
                "code": "lt",
                "title": "Lithuania",
            },
            {
                "_type": "country/:dataset/csv/:resource/countries",
                "_id": sha1("2"),
                "_revision": revs[2],
                "code": "lv",
                "title": "Latvia",
            },
        ],
    }

    assert app.get(
        "/country/69a33b149af7a7eeb25026c8cdc09187477ffe21/:dataset/csv/:resource/countries/:format/json"
    ).json() == {
        "_type": "country/:dataset/csv/:resource/countries",
        "_id": "69a33b149af7a7eeb25026c8cdc09187477ffe21",
        "_revision": revs[0],
        "title": "Lithuania",
        "code": "lt",
    }

    changes = app.get("/country/:dataset/csv/:resource/countries/:changes/:format/json").json()["_data"]
    assert changes == [
        {
            "_id": changes[0]["_id"],
            "_revision": changes[0]["_revision"],
            "_txn": changes[0]["_txn"],
            "_created": "2019-03-06T16:15:00.816308",
            "_op": "upsert",
            "_rid": "69a33b149af7a7eeb25026c8cdc09187477ffe21",
            "code": "lt",
            "title": "Lithuania",
        },
        {
            "_id": changes[1]["_id"],
            "_revision": changes[1]["_revision"],
            "_txn": changes[1]["_txn"],
            "_created": "2019-03-06T16:15:00.816308",
            "_op": "upsert",
            "_rid": sha1("2"),
            "code": "lv",
            "title": "LATVIA",
        },
        {
            "_id": changes[2]["_id"],
            "_revision": changes[2]["_revision"],
            "_txn": changes[2]["_txn"],
            "_created": "2019-03-06T16:15:00.816308",
            "_op": "upsert",
            "_rid": sha1("2"),
            "title": "Latvia",
        },
    ]

    assert app.get("country/:dataset/csv/:resource/countries/:changes/1000/:format/json").json() == {"_data": []}


def test_json_last_page(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref     | access
    example/json/page         |        |         |
      |   |   | City         |        | name    |
      |   |   |   | name     | string |         | open
    """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("example/json/page", ["insert", "search"])

    # Add data
    result = pushdata(app, "/example/json/page/City", {"name": "Vilnius"})
    pushdata(app, "/example/json/page/City", {"name": "Kaunas"})
    res = app.get("/example/json/page/City/:format/json?select(name,_page)").json()
    assert res == {
        "_data": [
            {"name": "Kaunas"},
            {"name": "Vilnius"},
        ],
        "_page": {"next": encode_page_values_manually({"name": "Vilnius", "_id": result["_id"]})},
    }


@pytest.mark.manifests("internal_sql", "csv")
def test_json_text(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type    | ref     | access  | level | uri
    example/json/text         |         |         |         |       | 
      |   |   |   |          | prefix  | rdf     |         |       | http://www.rdf.com
      |   |   |   |          |         | pav     |         |       | http://purl.org/pav/
      |   |   |   |          |         | dcat    |         |       | http://www.dcat.com
      |   |   |   |          |         | dct     |         |       | http://dct.com
      |   |   | Country      |         | name    |         |       | 
      |   |   |   | id       | integer |         |         |       |
      |   |   |   | name     | text    |         | open    | 3     |
      |   |   |   | name@en  | string  |         | open    |       |
      |   |   |   | name@lt  | string  |         | open    |       |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/json", ["insert", "getall", "search"])

    pushdata(app, "/example/json/text/Country", {"id": 0, "name": {"lt": "Lietuva", "en": "Lithuania", "C": "LT"}})
    pushdata(app, "/example/json/text/Country", {"id": 1, "name": {"lt": "Anglija", "en": "England", "C": "UK"}})

    resp = app.get(
        "/example/json/text/Country/:format/json?select(id,name)&sort(id)",
        headers=Headers(headers={"accept-language": "lt"}),
    ).json()["_data"]

    assert resp == [{"id": 0, "name": "Lietuva"}, {"id": 1, "name": "Anglija"}]


@pytest.mark.manifests("internal_sql", "csv")
def test_json_text_with_lang(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type    | ref     | access  | level | uri
    example/json/text/lang    |         |         |         |       | 
      |   |   |   |          | prefix  | rdf     |         |       | http://www.rdf.com
      |   |   |   |          |         | pav     |         |       | http://purl.org/pav/
      |   |   |   |          |         | dcat    |         |       | http://www.dcat.com
      |   |   |   |          |         | dct     |         |       | http://dct.com
      |   |   | Country      |         | name    |         |       | 
      |   |   |   | id       | integer |         |         |       |
      |   |   |   | name     | text    |         | open    | 3     |
      |   |   |   | name@en  | string  |         | open    |       |
      |   |   |   | name@lt  | string  |         | open    |       |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/json", ["insert", "getall", "search"])

    pushdata(app, "/example/json/text/lang/Country", {"id": 0, "name": {"lt": "Lietuva", "en": "Lithuania", "": "LT"}})
    pushdata(app, "/example/json/text/lang/Country", {"id": 1, "name": {"lt": "Anglija", "en": "England", "": "UK"}})

    resp = app.get(
        "/example/json/text/lang/Country/:format/json?lang(*)&select(id,name)&sort(id)",
        headers=Headers(headers={"accept-language": "lt"}),
    ).json()["_data"]

    assert resp == [
        {"id": 0, "name": {"": "LT", "en": "Lithuania", "lt": "Lietuva"}},
        {"id": 1, "name": {"": "UK", "en": "England", "lt": "Anglija"}},
    ]

    resp = app.get(
        "/example/json/text/lang/Country/:format/json?lang(en)&select(id,name)&sort(id)",
        headers=Headers(headers={"accept-language": "lt"}),
    ).json()["_data"]

    assert resp == [
        {
            "id": 0,
            "name": "Lithuania",
        },
        {
            "id": 1,
            "name": "England",
        },
    ]

    resp = app.get(
        "/example/json/text/lang/Country/:format/json?lang(en,lt)&select(id,name)&sort(id)",
        headers=Headers(headers={"accept-language": "lt"}),
    ).json()["_data"]

    assert resp == [
        {"id": 0, "name": {"en": "Lithuania", "lt": "Lietuva"}},
        {"id": 1, "name": {"en": "England", "lt": "Anglija"}},
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_json_changes_text(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type    | ref     | access  | level | uri
    example/json/text/changes |         |         |         |       | 
      |   |   |   |          | prefix  | rdf     |         |       | http://www.rdf.com
      |   |   |   |          |         | pav     |         |       | http://purl.org/pav/
      |   |   |   |          |         | dcat    |         |       | http://www.dcat.com
      |   |   |   |          |         | dct     |         |       | http://dct.com
      |   |   | Country      |         | name    |         |       | 
      |   |   |   | id       | integer |         |         |       |
      |   |   |   | name     | text    |         | open    | 3     |
      |   |   |   | name@en  | string  |         | open    |       |
      |   |   |   | name@lt  | string  |         | open    |       |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/json", ["insert", "getall", "search", "changes"])

    pushdata(
        app, "/example/json/text/changes/Country", {"id": 0, "name": {"lt": "Lietuva", "en": "Lithuania", "C": "LT"}}
    )
    pushdata(
        app, "/example/json/text/changes/Country", {"id": 1, "name": {"lt": "Anglija", "en": "England", "C": "UK"}}
    )

    resp = app.get(
        "/example/json/text/changes/Country/:changes/-10/:format/json?select(id,name)",
        headers=Headers(headers={"accept-language": "lt"}),
    ).json()["_data"]

    assert resp == [
        {"id": 0, "name": {"": "LT", "en": "Lithuania", "lt": "Lietuva"}},
        {"id": 1, "name": {"": "UK", "en": "England", "lt": "Anglija"}},
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_json_empty(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type    | ref     | access  | level | uri
    example/json/empty       |         |         |         |       | 
      |   |   |   |          | prefix  | rdf     |         |       | http://www.rdf.com
      |   |   |   |          |         | pav     |         |       | http://purl.org/pav/
      |   |   |   |          |         | dcat    |         |       | http://www.dcat.com
      |   |   |   |          |         | dct     |         |       | http://dct.com
      |   |   | Country      |         | name    |         |       | 
      |   |   |   | id       | integer |         |         |       |
      |   |   |   | name     | string  |         | open    | 3     |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/json", ["insert", "getall", "search", "changes"])

    resp = app.get("/example/json/empty/Country/:format/json?select(id,name)").json()

    assert resp == {"_data": []}


@pytest.mark.manifests("internal_sql", "csv")
def test_json_changes_corrupt_data(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property     | type    | ref     | access  | level | uri
    example/json/changes/corrupt |         |         |         |       | 
      |   |   | City             |         | name    | open    |       | 
      |   |   |   | id           | integer |         |         |       |
      |   |   |   | name         | string  |         |         |       |
      |   |   |   | country      | ref     | Country |         |       |
      |   |   |   | country.test | string  |         |         |       |
      |   |   |   | obj          | object  |         |         |       |
      |   |   |   | obj.test     | string  |         |         |       |
      |   |   | Country          |         | name    | open    |       | 
      |   |   |   | id           | integer |         |         |       |
      |   |   |   | name         | string  |         |         |       |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context, scope=["spinta_set_meta_fields"])
    app.authmodel("example/json", ["insert", "getall", "search", "changes"])
    country_id = str(uuid.uuid4())
    city_id = str(uuid.uuid4())
    pushdata(app, "/example/json/changes/corrupt/Country", {"_id": country_id, "id": 0, "name": "Lietuva"})
    pushdata(
        app,
        "/example/json/changes/corrupt/City",
        {
            "_id": city_id,
            "id": 0,
            "name": "Vilnius",
            "country": {"_id": country_id, "test": "t_lt"},
            "obj": {"test": "t_obj"},
        },
    )

    resp = app.get("/example/json/changes/corrupt/City/:changes/-10/:format/json")
    data = resp.json()["_data"][0]
    # Exclude reserved properties
    value = {key: value for key, value in data.items() if not key.startswith("_")}
    assert list(value.keys()) == ["id", "name", "country", "obj"]
    assert value["id"] == 0
    assert value["name"] == "Vilnius"
    assert value["country"] == {"_id": country_id, "test": "t_lt"}
    assert value["obj"] == {"test": "t_obj"}

    # Corrupt changelog data
    store = context.get("store")
    backend: PostgreSQL = store.manifest.backend
    model = commands.get_model(context, store.manifest, "example/json/changes/corrupt/City")
    with backend.begin() as transaction:
        table = backend.get_table(model, TableType.CHANGELOG)
        transaction.execute(
            table.update()
            .values(
                data={
                    "id": 0,
                    "name": "Vilnius",
                    "new": "new",
                    "country": {"_id": country_id, "testas": "testas"},
                    "obj": {"test": "t_obj_updated", "nested": {"test": "test"}},
                }
            )
            .where(table.c._rid == city_id)
        )

    resp = app.get("/example/json/changes/corrupt/City/:changes/-10/:format/json")
    data = resp.json()["_data"][0]
    # Exclude reserved properties
    value = {key: value for key, value in data.items() if not key.startswith("_")}
    assert list(value.keys()) == ["id", "name", "country", "obj"]

    assert value["id"] == 0
    assert value["name"] == "Vilnius"
    assert value["country"] == {"_id": country_id}
    assert value["obj"] == {"test": "t_obj_updated"}
