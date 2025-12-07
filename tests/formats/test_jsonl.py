import datetime
import json
import operator
import hashlib
import uuid
from pathlib import Path
from starlette.datastructures import Headers

import pytest
from _pytest.fixtures import FixtureRequest

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import pushdata, encode_page_values_manually
from spinta.testing.manifest import bootstrap_manifest


def sha1(s):
    return hashlib.sha1(s.encode()).hexdigest()


@pytest.mark.skip("datasets")
def test_export_json(app, mocker):
    mocker.patch(
        "spinta.backends.postgresql.sqlalchemy.utcnow", return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308)
    )

    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("country/:dataset/csv/:resource/countries", ["upsert", "getall"])

    resp = app.post(
        "/country/:dataset/csv/:resource/countries",
        json={
            "_data": [
                {
                    "_op": "upsert",
                    "_type": "country/:dataset/csv/:resource/countries",
                    "_id": sha1("1"),
                    "_where": '_id="' + sha1("1") + '"',
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
    data = resp.json()["_data"]
    revs = [d["_revision"] for d in data]

    resp = app.get("country/:dataset/csv/:resource/countries/:format/jsonl")
    data = sorted([json.loads(line) for line in resp.text.splitlines()], key=operator.itemgetter("code"))
    assert data == [
        {
            "code": "lt",
            "_id": sha1("1"),
            "_revision": revs[0],
            "title": "Lithuania",
            "_type": "country/:dataset/csv/:resource/countries",
        },
        {
            "code": "lv",
            "_id": sha1("2"),
            "_revision": revs[2],
            "title": "Latvia",
            "_type": "country/:dataset/csv/:resource/countries",
        },
    ]


@pytest.mark.skip("datasets")
def test_export_jsonl_with_all(app):
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("continent/:dataset/dependencies/:resource/continents", ["insert", "getall"])
    app.authmodel("country/:dataset/dependencies/:resource/continents", ["insert", "getall"])
    app.authmodel("capital/:dataset/dependencies/:resource/continents", ["insert", "getall"])

    resp = app.post(
        "/",
        json={
            "_data": [
                {
                    "_type": "continent/:dataset/dependencies/:resource/continents",
                    "_op": "insert",
                    "_id": sha1("1"),
                    "title": "Europe",
                },
                {
                    "_type": "country/:dataset/dependencies/:resource/continents",
                    "_op": "insert",
                    "_id": sha1("2"),
                    "title": "Lithuania",
                    "continent": sha1("1"),
                },
                {
                    "_type": "capital/:dataset/dependencies/:resource/continents",
                    "_op": "insert",
                    "_id": sha1("3"),
                    "title": "Vilnius",
                    "country": sha1("2"),
                },
            ]
        },
    )
    assert resp.status_code == 200, resp.json()

    resp = app.get("/:all/:dataset/dependencies/:resource/continents?format(jsonl)")
    assert resp.status_code == 200, resp.json()
    data = [json.loads(d) for d in resp.text.splitlines()]
    assert sorted(((d["_id"], d["title"]) for d in data), key=lambda x: x[1]) == [
        (sha1("1"), "Europe"),
        (sha1("2"), "Lithuania"),
        (sha1("3"), "Vilnius"),
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_jsonl_last_page(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref     | access
    example/jsonl/page         |        |         |
      |   |   | City         |        | name    |
      |   |   |   | name     | string |         | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("example/jsonl/page", ["insert", "search"])

    # Add data
    result = pushdata(app, "/example/jsonl/page/City", {"name": "Vilnius"})
    pushdata(app, "/example/jsonl/page/City", {"name": "Kaunas"})
    res = app.get("/example/jsonl/page/City/:format/jsonl?select(name,_page)")
    data = [json.loads(d) for d in res.text.splitlines()]
    assert data == [
        {"name": "Kaunas"},
        {"_page": {"next": encode_page_values_manually({"name": "Vilnius", "_id": result["_id"]})}, "name": "Vilnius"},
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_jsonl_getone(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref     | access
    example/jsonl/one        |        |         |
      |   |   | City         |        | name    |
      |   |   |   | name     | string |         | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("example/jsonl/one", ["insert", "search", "getone"])

    # Add data
    result = pushdata(app, "/example/jsonl/one/City", {"name": "Vilnius"})
    pushdata(app, "/example/jsonl/one/City", {"name": "Kaunas"})
    res = app.get(f"/example/jsonl/one/City/{result['_id']}/:format/jsonl")
    data = [json.loads(d) for d in res.text.splitlines()]
    assert data == [
        {"_id": result["_id"], "_revision": result["_revision"], "_type": "example/jsonl/one/City", "name": "Vilnius"},
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_jsonl_text(
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
    example/jsonl/text         |         |         |         |       | 
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
    app.authmodel("example/jsonl", ["insert", "getall", "search"])

    pushdata(app, "/example/jsonl/text/Country", {"id": 0, "name": {"lt": "Lietuva", "en": "Lithuania", "": "LT"}})
    pushdata(app, "/example/jsonl/text/Country", {"id": 1, "name": {"lt": "Anglija", "en": "England", "": "UK"}})

    resp = app.get(
        "/example/jsonl/text/Country/:format/jsonl?select(id,name)&sort(id)",
        headers=Headers(headers={"accept-language": "lt"}),
    )
    data = [json.loads(d) for d in resp.text.splitlines()]

    assert data == [{"id": 0, "name": "Lietuva"}, {"id": 1, "name": "Anglija"}]


@pytest.mark.manifests("internal_sql", "csv")
def test_jsonl_text_with_lang(
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
    example/jsonl/text/lang    |         |         |         |       | 
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
    app.authmodel("example/jsonl", ["insert", "getall", "search"])

    pushdata(app, "/example/jsonl/text/lang/Country", {"id": 0, "name": {"lt": "Lietuva", "en": "Lithuania", "": "LT"}})
    pushdata(app, "/example/jsonl/text/lang/Country", {"id": 1, "name": {"lt": "Anglija", "en": "England", "": "UK"}})

    resp = app.get(
        "/example/jsonl/text/lang/Country/:format/jsonl?lang(*)&select(id,name)&sort(id)",
        headers=Headers(headers={"accept-language": "lt"}),
    )
    data = [json.loads(d) for d in resp.text.splitlines()]

    assert data == [
        {"id": 0, "name": {"": "LT", "en": "Lithuania", "lt": "Lietuva"}},
        {"id": 1, "name": {"": "UK", "en": "England", "lt": "Anglija"}},
    ]

    resp = app.get(
        "/example/jsonl/text/lang/Country/:format/jsonl?lang(en)&select(id,name)&sort(id)",
        headers=Headers(headers={"accept-language": "lt"}),
    )
    data = [json.loads(d) for d in resp.text.splitlines()]

    assert data == [
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
        "/example/jsonl/text/lang/Country/:format/jsonl?lang(en,lt)&select(id,name)&sort(id)",
        headers=Headers(headers={"accept-language": "lt"}),
    )
    data = [json.loads(d) for d in resp.text.splitlines()]

    assert data == [
        {"id": 0, "name": {"en": "Lithuania", "lt": "Lietuva"}},
        {"id": 1, "name": {"en": "England", "lt": "Anglija"}},
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_jsonl_changes_text(
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
    example/jsonl/text/changes |         |         |         |       | 
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
    app.authmodel("example/jsonl", ["insert", "getall", "search", "changes"])

    pushdata(
        app, "/example/jsonl/text/changes/Country", {"id": 0, "name": {"lt": "Lietuva", "en": "Lithuania", "C": "LT"}}
    )
    pushdata(
        app, "/example/jsonl/text/changes/Country", {"id": 1, "name": {"lt": "Anglija", "en": "England", "C": "UK"}}
    )

    resp = app.get(
        "/example/jsonl/text/changes/Country/:changes/-10/:format/jsonl?select(id,name)",
        headers=Headers(headers={"accept-language": "lt"}),
    )
    data = [json.loads(d) for d in resp.text.splitlines()]

    assert data == [
        {"id": 0, "name": {"": "LT", "en": "Lithuania", "lt": "Lietuva"}},
        {"id": 1, "name": {"": "UK", "en": "England", "lt": "Anglija"}},
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_jsonl_empty(
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
    example/jsonl/empty      |         |         |         |       | 
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
    app.authmodel("example/jsonl", ["insert", "getall", "search", "changes"])

    resp = app.get("/example/jsonl/empty/Country/:format/jsonl?select(id,name)")
    data = [json.loads(d) for d in resp.text.splitlines()]

    assert data == []


@pytest.mark.manifests("internal_sql", "csv")
def test_jsonl_changes_corrupt_data(
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
    example/jsonl/changes/corrupt |         |         |         |       | 
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
    app.authmodel("example/jsonl", ["insert", "getall", "search", "changes"])
    country_id = str(uuid.uuid4())
    city_id = str(uuid.uuid4())
    pushdata(app, "/example/jsonl/changes/corrupt/Country", {"_id": country_id, "id": 0, "name": "Lietuva"})
    pushdata(
        app,
        "/example/jsonl/changes/corrupt/City",
        {
            "_id": city_id,
            "id": 0,
            "name": "Vilnius",
            "country": {"_id": country_id, "test": "t_lt"},
            "obj": {"test": "t_obj"},
        },
    )

    resp = app.get("/example/jsonl/changes/corrupt/City/:changes/-10/:format/jsonl")
    data = [json.loads(d) for d in resp.text.splitlines()][0]
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
    model = commands.get_model(context, store.manifest, "example/jsonl/changes/corrupt/City")
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

    resp = app.get("/example/jsonl/changes/corrupt/City/:changes/-10/:format/jsonl")
    data = [json.loads(d) for d in resp.text.splitlines()][0]
    # Exclude reserved properties
    value = {key: value for key, value in data.items() if not key.startswith("_")}
    assert list(value.keys()) == ["id", "name", "country", "obj"]

    assert value["id"] == 0
    assert value["name"] == "Vilnius"
    assert value["country"] == {"_id": country_id}
    assert value["obj"] == {"test": "t_obj_updated"}
