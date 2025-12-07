import base64
import datetime
import hashlib
import uuid
from pathlib import Path
from starlette.datastructures import Headers
import pytest
from _pytest.fixtures import FixtureRequest

from spinta import commands
from spinta.auth import AdminToken
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.core.enums import Action
from spinta.components import UrlParams
from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest_and_context
from spinta.testing.data import pushdata
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.client import create_test_client
from spinta.testing.request import render_data


def sha1(s):
    return hashlib.sha1(s.encode()).hexdigest()


@pytest.mark.skip("datasets")
def test_export_ascii(app, mocker):
    mocker.patch(
        "spinta.backends.postgresql.dataset.utcnow",
        return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308),
    )

    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("country/:dataset/csv/:resource/countries", ["upsert", "getall", "search", "changes"])

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
    assert app.get(
        "/country/:dataset/csv/:resource/countries/:format/ascii?select(code,title)&sort(+code)&format(colwidth(42))"
    ).text == ("----  ---------\ncode  title    \nlt    Lithuania\nlv    Latvia\n----  ---------\n")

    resp = app.get("/country/:dataset/csv/:resource/countries/:changes")
    changes = resp.json()["_data"]
    changes = [{k: str(v) for k, v in row.items()} for row in changes]
    res = app.get("country/:dataset/csv/:resource/countries/:changes/:format/ascii?format(colwidth(42))").text
    lines = res.splitlines()
    cols = lines[0].split()
    data = [dict(zip(cols, [v.strip() for v in row.split()])) for row in lines[2:]]
    data = [{k: v for k, v in row.items() if v != "None"} for row in data]
    assert data == changes


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.asyncio
async def test_export_multiple_types(manifest_type: str, tmp_path: Path, rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property | type    | ref   | access
    example                  |         |       |
      |   |   | A            |         | value |
      |   |   |   | value    | integer |       | open
      |   |   | B            |         | value |
      |   |   |   | value    | integer |       | open
      |   |   | C            |         | value |
      |   |   |   | value    | integer |       | open

    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    rows = [
        {"_type": "example/A", "value": 1},
        {"_type": "example/A", "value": 2},
        {"_type": "example/A", "value": 3},
        {"_type": "example/B", "value": 1},
        {"_type": "example/B", "value": 2},
        {"_type": "example/B", "value": 3},
        {"_type": "example/C", "value": 1},
        {"_type": "example/C", "value": 2},
        {"_type": "example/C", "value": 3},
    ]
    context.set("auth.token", AdminToken())
    config = context.get("config")
    exporter = config.exporters["ascii"]
    ns = commands.get_namespace(context, manifest, "")
    params = UrlParams()
    assert "".join(exporter(context, ns, Action.GETALL, params, rows)) == (
        "\n"
        "\n"
        "Table: example/A\n"
        "---------  ---  ---------  ----------  -----\n"
        "_type      _id  _revision  _page.next  value\n"
        "example/A  ∅    ∅          ∅           1\n"
        "example/A  ∅    ∅          ∅           2\n"
        "example/A  ∅    ∅          ∅           3\n"
        "---------  ---  ---------  ----------  -----\n"
        "\n"
        "\n"
        "Table: example/B\n"
        "---------  ---  ---------  ----------  -----\n"
        "_type      _id  _revision  _page.next  value\n"
        "example/B  ∅    ∅          ∅           1\n"
        "example/B  ∅    ∅          ∅           2\n"
        "example/B  ∅    ∅          ∅           3\n"
        "---------  ---  ---------  ----------  -----\n"
        "\n"
        "\n"
        "Table: example/C\n"
        "---------  ---  ---------  ----------  -----\n"
        "_type      _id  _revision  _page.next  value\n"
        "example/C  ∅    ∅          ∅           1\n"
        "example/C  ∅    ∅          ∅           2\n"
        "example/C  ∅    ∅          ∅           3\n"
        "---------  ---  ---------  ----------  -----\n"
    )


@pytest.mark.skip("datasets")
def test_export_ascii_params(app, mocker):
    app.authmodel("country/:dataset/csv/:resource/countries", ["insert", "search"])
    resp = app.post(
        "/country/:dataset/csv/:resource/countries",
        json={
            "_data": [
                {
                    "_op": "insert",
                    "_type": "country/:dataset/csv/:resource/countries",
                    "code": "lt",
                    "title": "Lithuania",
                },
                {
                    "_op": "insert",
                    "_type": "country/:dataset/csv/:resource/countries",
                    "code": "lv",
                    "title": "Latvia",
                },
            ]
        },
    )
    assert resp.status_code == 200, resp.json()
    assert app.get(
        "/country/:dataset/csv/:resource/countries/:format/ascii?select(code,title)&sort(+code)&format(width(50))"
    ).text == ("----  ---------\ncode  title    \nlt    Lithuania\nlv    Latvia\n----  ---------\n")


@pytest.mark.manifests("internal_sql", "csv")
def test_ascii_ref_dtype(
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
    example/ascii/ref        |        |         |
      |   |   | Country      |        | name    |
      |   |   |   | name     | string |         | open
      |   |   | City         |        | name    |
      |   |   |   | name     | string |         | open
      |   |   |   | country  | ref    | Country | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/ascii/ref", ["insert", "search"])

    # Add data
    country = pushdata(app, "/example/ascii/ref/Country", {"name": "Lithuania"})
    pushdata(
        app,
        "/example/ascii/ref/City",
        {
            "name": "Vilnius",
            "country": {"_id": country["_id"]},
        },
    )

    assert app.get("/example/ascii/ref/City/:format/ascii?select(name, country)").text == (
        "-------  ------------------------------------\n"
        "name     country._id                         \n"
        f"Vilnius  {country['_id']}\n"
        "-------  ------------------------------------\n"
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_ascii_file_dtype(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref  | access
    example/ascii/file       |        |      |
      |   |   | Country      |        | name |
      |   |   |   | name     | string |      | open
      |   |   |   | flag     | file   |      | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/ascii/file", ["insert", "search"])

    # Add data
    pushdata(
        app,
        "/example/ascii/file/Country",
        {
            "name": "Lithuania",
            "flag": {
                "_id": "file.txt",
                "_content_type": "text/plain",
                "_content": base64.b64encode(b"DATA").decode(),
            },
        },
    )

    assert app.get("/example/ascii/file/Country/:format/ascii?select(name, flag)").text == (
        "---------  --------  ------------------\n"
        "name       flag._id  flag._content_type\n"
        "Lithuania  file.txt  text/plain\n"
        "---------  --------  ------------------\n"
    )


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.asyncio
async def test_ascii_getone(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property | type   | ref  | access
    example                  |        |      |
      |   |   | City         |        | name |
      |   |   |   | name     | string |      | open
    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )

    _id = "19e4f199-93c5-40e5-b04e-a575e81ac373"
    result = render_data(
        context,
        manifest,
        f"example/City/{_id}",
        None,
        accept="text/plain",
        data={
            "_type": "example/City",
            "_id": _id,
            "_revision": "b6197bb7-3592-4cdb-a61c-5a618f44950c",
            "name": "Vilnius",
        },
    )
    result = "".join([x async for x in result.body_iterator]).splitlines()
    assert result == [
        "",
        "",
        "Table: example/City",
        "------------  ------------------------------------  ------------------------------------  ----------  -------",
        "_type         _id                                   _revision                             _page.next  name   ",
        "example/City  19e4f199-93c5-40e5-b04e-a575e81ac373  b6197bb7-3592-4cdb-a61c-5a618f44950c  ∅           Vilnius",
        "------------  ------------------------------------  ------------------------------------  ----------  -------",
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_ascii_params(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property   | type    | ref  | access
    example/ascii/params       |         |      |
      |   |   | Country        |         | name |
      |   |   |   | name       | string  |      | open
      |   |   |   | capital    | string  |      | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/ascii/params", ["insert", "search"])

    # Add data
    pushdata(app, "/example/ascii/params/Country", {"name": "Lithuania", "capital": "Vilnius"})

    assert app.get("/example/ascii/params/Country/:format/ascii?select(name,capital)&format(width(15))").text == (
        "---------  ...\nname       ...\nLithuania  ...\n---------  ...\n"
    )

    assert app.get("/example/ascii/params/Country/:format/ascii?select(name,capital)&format(colwidth(7))").text == (
        "-------  -------\nname     capital\nLithuan  Vilnius\\\nia\n-------  -------\n"
    )

    assert app.get("/example/ascii/params/Country/:format/ascii?select(name,capital)&format(vlen(7))").text == (
        "----------  -------\nname        capital\nLithuan...  Vilnius\n----------  -------\n"
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_ascii_multiline(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property   | type    | ref  | access
    example/ascii/params       |         |      |
      |   |   | Country        |         | name |
      |   |   |   | name       | string  |      | open
      |   |   |   | capital    | string  |      | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/ascii/params", ["insert", "search"])

    # Add data
    pushdata(
        app,
        "/example/ascii/params/Country",
        {"name": "Lithuania", "capital": "Current capital - Vilnius.\nPrevious - Kernave."},
    )

    assert app.get("/example/ascii/params/Country/:format/ascii?select(name,capital)&format(colwidth(20))").text == (
        "---------  ----------------\n"
        "name       capital         \n"
        "Lithuania  Current capital \\\n"
        "           - Vilnius.      \\\n"
        "           Previous - Kerna\\\n"
        "           ve.\n"
        "---------  ----------------\n"
    )


@pytest.mark.asyncio
async def test_ascii_check_last_page(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property   | type    | ref  | access
        example/ascii/page         |         |      |
          |   |   | Country        |         | name |
          |   |   |   | name       | string  |      | open
          |   |   |   | capital    | string  |      | open
        """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("example/ascii/page", ["insert", "search", "getall"])
    lithuania_id = "8d3404c7-9371-403f-aaab-6266b57a4b38"
    england_id = "aff76319-17f6-4ad2-a448-45262d9536b8"
    # Add data
    results = pushdata(
        app,
        "/example/ascii/page/Country",
        [
            {"_op": "insert", "_id": lithuania_id, "name": "Lithuania", "capital": "Vilnius"},
            {"_op": "insert", "_id": england_id, "name": "England", "capital": "London"},
        ],
    )
    assert app.get("/example/ascii/page/Country/:format/ascii").text == (
        "\n"
        "\n"
        "Table: example/ascii/page/Country\n"
        "--------------------------  ------------------------------------  ------------------------------------  ------------------------------------------  --------  -------\n"
        "_type                       _id                                   _revision                             _page.next                                  name      capital\n"
        f"example/ascii/page/Country  aff76319-17f6-4ad2-a448-45262d9536b8  {results[1]['_revision']}  ∅                                           England   London\n"
        f"example/ascii/page/Country  8d3404c7-9371-403f-aaab-6266b57a4b38  {results[0]['_revision']}  WyJMaXRodWFuaWEiLCAiOGQzNDA0YzctOTM3MS00MD  Lithuani  Vilnius\\\n"
        "                                                                                                        NmLWFhYWItNjI2NmI1N2E0YjM4Il0=              a\n"
        "--------------------------  ------------------------------------  ------------------------------------  ------------------------------------------  --------  -------\n"
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_ascii_text(
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
    example/ascii/text         |         |         |         |       | 
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
    app.authmodel("example/ascii", ["insert", "getall", "search"])

    pushdata(app, "/example/ascii/text/Country", {"id": 0, "name": {"lt": "Lietuva", "en": "Lithuania", "C": "LT"}})
    pushdata(app, "/example/ascii/text/Country", {"id": 1, "name": {"lt": "Anglija", "en": "England", "C": "UK"}})

    res = app.get(
        "/example/ascii/text/Country/:format/ascii?select(id,name)&sort(id)",
        headers=Headers(headers={"accept-language": "lt"}),
    ).text
    assert res == ("--  -------\nid  name   \n0   Lietuva\n1   Anglija\n--  -------\n")


@pytest.mark.manifests("internal_sql", "csv")
def test_ascii_text_with_lang(
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
    example/ascii/text/lang    |         |         |         |       | 
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
    app.authmodel("example/ascii", ["insert", "getall", "search"])

    pushdata(
        app, "/example/ascii/text/lang/Country", {"id": 0, "name": {"lt": "Lietuva", "en": "Lithuania", "C": "LT"}}
    )
    pushdata(app, "/example/ascii/text/lang/Country", {"id": 1, "name": {"lt": "Anglija", "en": "England", "C": "UK"}})

    res = app.get(
        "/example/ascii/text/lang/Country/:format/ascii?lang(*)&select(id,name)&sort(id)",
        headers=Headers(headers={"accept-language": "lt"}),
    ).text
    assert res == (
        "--  ------  ---------  -------\n"
        "id  name@C  name@en    name@lt\n"
        "0   LT      Lithuania  Lietuva\n"
        "1   UK      England    Anglija\n"
        "--  ------  ---------  -------\n"
    )

    res = app.get(
        "/example/ascii/text/lang/Country/:format/ascii?lang(en)&select(id,name)&sort(id)",
        headers=Headers(headers={"accept-language": "lt"}),
    ).text
    assert res == ("--  ---------\nid  name     \n0   Lithuania\n1   England\n--  ---------\n")

    res = app.get(
        "/example/ascii/text/lang/Country/:format/ascii?lang(en,lt)&select(id,name)&sort(id)",
        headers=Headers(headers={"accept-language": "lt"}),
    ).text
    assert res == (
        "--  ---------  -------\n"
        "id  name@en    name@lt\n"
        "0   Lithuania  Lietuva\n"
        "1   England    Anglija\n"
        "--  ---------  -------\n"
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_ascii_changes_text(
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
    example/ascii/text/changes |         |         |         |       | 
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
    app.authmodel("example/ascii", ["insert", "getall", "search", "changes"])

    pushdata(
        app, "/example/ascii/text/changes/Country", {"id": 0, "name": {"lt": "Lietuva", "en": "Lithuania", "C": "LT"}}
    )
    pushdata(
        app, "/example/ascii/text/changes/Country", {"id": 1, "name": {"lt": "Anglija", "en": "England", "C": "UK"}}
    )

    resp = app.get(
        "/example/ascii/text/changes/Country/:changes/-10/:format/ascii?select(id,name)",
        headers=Headers(headers={"accept-language": "lt"}),
    ).text

    assert resp == (
        "--  ------  ---------  -------\n"
        "id  name@C  name@en    name@lt\n"
        "0   LT      Lithuania  Lietuva\n"
        "1   UK      England    Anglija\n"
        "--  ------  ---------  -------\n"
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_ascii_empty(
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
    example/ascii/empty      |         |         |         |       | 
      |   |   |   |          | prefix  | rdf     |         |       | http://www.rdf.com
      |   |   |   |          |         | pav     |         |       | http://purl.org/pav/
      |   |   |   |          |         | dcat    |         |       | http://www.dcat.com
      |   |   |   |          |         | dct     |         |       | http://dct.com
      |   |   | Country      |         | name    |         |       | 
      |   |   |   | id       | integer |         |         |       |
      |   |   |   | name     | string  |         |         |       |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/ascii", ["insert", "getall", "search", "changes"])
    resp = app.get("/example/ascii/empty/Country/:format/ascii?select(id,name)").text
    assert resp == ""


@pytest.mark.manifests("internal_sql", "csv")
def test_ascii_changes_corrupt_data(
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
    example/ascii/changes/corrupt |         |         |         |       | 
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
    app.authmodel("example/ascii", ["insert", "getall", "search", "changes"])
    country_id = str(uuid.uuid4())
    city_id = str(uuid.uuid4())
    pushdata(app, "/example/ascii/changes/corrupt/Country", {"_id": country_id, "id": 0, "name": "Lietuva"})
    pushdata(
        app,
        "/example/ascii/changes/corrupt/City",
        {
            "_id": city_id,
            "id": 0,
            "name": "Vilnius",
            "country": {"_id": country_id, "test": "t_lt"},
            "obj": {"test": "t_obj"},
        },
    )

    resp = app.get(
        "/example/ascii/changes/corrupt/City/:changes/-10/:format/ascii?select(_id, id, name, country, obj)"
    ).text
    assert resp == (
        "------------------------------------  --  -------  ------------------------------------  ------------  --------\n"
        "_id                                   id  name     country._id                           country.test  obj.test\n"
        f"{city_id}  0   Vilnius  {country_id}  t_lt          t_obj\n"
        "------------------------------------  --  -------  ------------------------------------  ------------  --------\n"
    )

    # Corrupt changelog data
    store = context.get("store")
    backend: PostgreSQL = store.manifest.backend
    model = commands.get_model(context, store.manifest, "example/ascii/changes/corrupt/City")
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

    resp = app.get(
        "/example/ascii/changes/corrupt/City/:changes/-10/:format/ascii?select(_id, id, name, country, obj)"
    ).text
    assert resp == (
        "------------------------------------  --  -------  ------------------------------------  ------------  -------------\n"
        "_id                                   id  name     country._id                           country.test  obj.test     \n"
        f"{city_id}  0   Vilnius  {country_id}  ∅             t_obj_updated\n"
        "------------------------------------  --  -------  ------------------------------------  ------------  -------------\n"
    )
