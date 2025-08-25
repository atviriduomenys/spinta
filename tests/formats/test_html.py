import base64
import hashlib
import uuid
from pathlib import Path

from lxml import html
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple

import pytest
from _pytest.fixtures import FixtureRequest
from starlette.requests import Request
from starlette.datastructures import Headers

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.core.enums import Action
from spinta.components import Config
from spinta.components import Context
from spinta.components import Namespace
from spinta.components import Store
from spinta.components import UrlParams
from spinta.components import Version
from spinta.core.config import RawConfig
from spinta.formats.html.commands import _LimitIter
from spinta.formats.html.components import Cell
from spinta.formats.html.components import Color
from spinta.formats.html.components import Html
from spinta.formats.html.helpers import CurrentLocation
from spinta.formats.html.helpers import get_current_location
from spinta.formats.html.helpers import short_id
from spinta.testing.client import TestClient
from spinta.testing.client import TestClientResponse
from spinta.testing.client import create_test_client
from spinta.testing.data import pushdata
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.manifest import load_manifest_and_context
from spinta.testing.request import render_data
from spinta.utils.data import take


def _get_data_table(context: dict):
    return [tuple(context["header"])] + [tuple(cell["value"] for cell in row) for row in context["data"]]


def sha1(s):
    return hashlib.sha1(s.encode()).hexdigest()


@pytest.mark.skip("datasets")
def test_select_with_joins(app):
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("continent/:dataset/dependencies/:resource/continents", ["insert"])
    app.authmodel("country/:dataset/dependencies/:resource/continents", ["insert"])
    app.authmodel("capital/:dataset/dependencies/:resource/continents", ["insert", "search"])

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

    resp = app.get(
        "/capital/:dataset/dependencies/:resource/continents?"
        "select(_id,title,country.title,country.continent.title)&"
        "format(html)"
    )
    assert _get_data_table(resp.context) == [
        ("_id", "title", "country.title", "country.continent.title"),
        (sha1("3")[:8], "Vilnius", "Lithuania", "Europe"),
    ]


def test_limit_in_links(app):
    app.authmodel(
        "Country",
        [
            "search",
        ],
    )
    resp = app.get("/Country/:format/html?limit(1)")
    assert resp.context["formats"][0] == ("CSV", "/Country/:format/csv?limit(1)")


def _get_current_loc(context: Context, path: str):
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "path_params": {"path": path},
            "headers": {},
        }
    )
    params = commands.prepare(context, UrlParams(), Version(), request)
    if isinstance(params.model, Namespace):
        store: Store = context.get("store")
        model = commands.get_model(context, store.manifest, "_ns")
    else:
        model = params.model
    config: Config = context.get("config")
    return get_current_location(context, config, model, params)


@pytest.fixture(scope="module")
def context_current_location(rc: RawConfig) -> Context:
    return bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref | title               | description
    datasets                 | ns     |     | All datasets        | All external datasets.
    datasets/gov             | ns     |     | Government datasets | All external government datasets.
                             |        |     |                     |
    datasets/gov/vpt/new     |        |     | New data            | Data from a new database.
      | resource             |        |     |                     |
      |   |   | City         |        |     | Cities              | All cities.
      |   |   |   | name     | string |     | City name           | Name of a city.
    """,
    )


@pytest.mark.parametrize(
    "path,result",
    [
        (
            "/",
            [
                ("🏠", "/"),
            ],
        ),
        (
            "/datasets",
            [
                ("🏠", "/"),
                ("All datasets", None),
            ],
        ),
        (
            "/datasets/gov",
            [
                ("🏠", "/"),
                ("All datasets", "/datasets"),
                ("Government datasets", None),
            ],
        ),
        (
            "/datasets/gov/vpt",
            [
                ("🏠", "/"),
                ("All datasets", "/datasets"),
                ("Government datasets", "/datasets/gov"),
                ("vpt", None),
            ],
        ),
        (
            "/datasets/gov/vpt/new",
            [
                ("🏠", "/"),
                ("All datasets", "/datasets"),
                ("Government datasets", "/datasets/gov"),
                ("vpt", "/datasets/gov/vpt"),
                ("New data", None),
            ],
        ),
        (
            "/datasets/gov/vpt/new/City",
            [
                ("🏠", "/"),
                ("All datasets", "/datasets"),
                ("Government datasets", "/datasets/gov"),
                ("vpt", "/datasets/gov/vpt"),
                ("New data", "/datasets/gov/vpt/new"),
                ("Cities", None),
                ("Changes", "/datasets/gov/vpt/new/City/:changes/-10"),
            ],
        ),
        (
            "/datasets/gov/vpt/new/City/0edc2281-f372-44a7-b0f8-e8d06ad0ce08",
            [
                ("🏠", "/"),
                ("All datasets", "/datasets"),
                ("Government datasets", "/datasets/gov"),
                ("vpt", "/datasets/gov/vpt"),
                ("New data", "/datasets/gov/vpt/new"),
                ("Cities", "/datasets/gov/vpt/new/City"),
                ("0edc2281", None),
                ("Changes", "/datasets/gov/vpt/new/City/0edc2281-f372-44a7-b0f8-e8d06ad0ce08/:changes/-10"),
            ],
        ),
    ],
)
def test_current_location(
    context_current_location: Context,
    path: str,
    result: CurrentLocation,
):
    context = context_current_location
    assert _get_current_loc(context, path) == result


@pytest.fixture(scope="module")
def context_current_location_with_root(rc: RawConfig):
    rc = rc.fork(
        {
            "root": "datasets/gov/vpt",
        }
    )
    return bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref | title               | description
    datasets                 | ns     |     | All datasets        | All external datasets.
    datasets/gov             | ns     |     | Government datasets | All external government datasets.
                             |        |     |                     |
    datasets/gov/vpt/new     |        |     | New data            | Data from a new database.
      | resource             |        |     |                     |
      |   |   | City         |        |     | Cities              | All cities.
      |   |   |   | name     | string |     | City name           | Name of a city.
    datasets/gov/vpt/old     |        |     | New data            | Data from a new database.
      | resource             |        |     |                     |
      |   |   | Country      |        |     | Countries           | All countries.
      |   |   |   | name     | string |     | Country name        | Name of a country.
    """,
    )


@pytest.mark.parametrize(
    "path,result",
    [
        (
            "/",
            [
                ("🏠", "/"),
            ],
        ),
        (
            "/datasets",
            [
                ("🏠", "/"),
            ],
        ),
        (
            "/datasets/gov",
            [
                ("🏠", "/"),
            ],
        ),
        (
            "/datasets/gov/vpt",
            [
                ("🏠", "/"),
            ],
        ),
        (
            "/datasets/gov/vpt/new",
            [
                ("🏠", "/"),
                ("New data", None),
            ],
        ),
        (
            "/datasets/gov/vpt/new/City",
            [
                ("🏠", "/"),
                ("New data", "/datasets/gov/vpt/new"),
                ("Cities", None),
                ("Changes", "/datasets/gov/vpt/new/City/:changes/-10"),
            ],
        ),
        (
            "/datasets/gov/vpt/new/City/0edc2281-f372-44a7-b0f8-e8d06ad0ce08",
            [
                ("🏠", "/"),
                ("New data", "/datasets/gov/vpt/new"),
                ("Cities", "/datasets/gov/vpt/new/City"),
                ("0edc2281", None),
                ("Changes", "/datasets/gov/vpt/new/City/0edc2281-f372-44a7-b0f8-e8d06ad0ce08/:changes/-10"),
            ],
        ),
    ],
)
def test_current_location_with_root(
    context_current_location_with_root: Context,
    path: str,
    result: CurrentLocation,
):
    context = context_current_location_with_root
    assert _get_current_loc(context, path) == result


def _prep_file_type(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
) -> Tuple[TestClient, str]:
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | access
    example/html/file        |        |
      | resource             |        |
      |   |   | Country      |        |
      |   |   |   | name     | string | open
      |   |   |   | flag     | image  | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel(
        "example/html/file",
        [
            "insert",
            "getall",
            "getone",
            "changes",
            "search",
        ],
    )

    # Add a file
    resp = app.post(
        "/example/html/file/Country",
        json={
            "name": "Lithuania",
            "flag": {
                "_id": "flag.png",
                "_content_type": "image/png",
                "_content": base64.b64encode(b"IMAGE").decode(),
            },
        },
    )
    assert resp.status_code == 201, resp.json()
    data = resp.json()
    _id = data["_id"]

    return app, _id


def _table(data: List[List[Cell]]) -> List[List[Dict[str, Any]]]:
    return [[cell.as_dict() for cell in row] for row in data]


def _table_with_header(
    resp: TestClientResponse,
) -> List[Dict[str, Dict[str, Any]]]:
    header = resp.context["header"]
    return [{key: cell.as_dict() for key, cell in zip(header, row)} for row in resp.context["data"]]


@pytest.mark.manifests("internal_sql", "csv")
def test_file_type_list(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    app, _id = _prep_file_type(manifest_type, tmp_path, rc, postgresql, request)
    resp = app.get(
        "/example/html/file/Country",
        headers={
            "Accept": "text/html",
        },
    )
    assert _table(resp.context["data"]) == [
        [
            {
                "link": f"/example/html/file/Country/{_id}",
                "value": short_id(_id),
            },
            {
                "value": "Lithuania",
            },
            {
                "link": f"/example/html/file/Country/{_id}/flag",
                "value": "flag.png",
            },
            {
                "value": "image/png",
            },
        ]
    ]


def _row(row: List[Tuple[str, Cell]]) -> List[Tuple[str, Dict[str, Any]]]:
    return [(name, cell.as_dict()) for name, cell in row]


@pytest.mark.manifests("internal_sql", "csv")
def test_file_type_details(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    app, _id = _prep_file_type(manifest_type, tmp_path, rc, postgresql, request)
    resp = app.get(
        f"/example/html/file/Country/{_id}",
        headers={
            "Accept": "text/html",
        },
    )
    assert list(map(take, _table_with_header(resp))) == [
        {
            "name": {"value": "Lithuania"},
            "flag._id": {
                "value": "flag.png",
                "link": f"/example/html/file/Country/{_id}/flag",
            },
            "flag._content_type": {"value": "image/png"},
        },
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_file_type_changes(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    app, _id = _prep_file_type(manifest_type, tmp_path, rc, postgresql, request)
    resp = app.get(
        "/example/html/file/Country/:changes",
        headers={
            "Accept": "text/html",
        },
    )
    assert _table(resp.context["data"])[0][6:] == [
        {"color": "#f5f5f5", "value": ""},
        {
            "value": "Lithuania",
        },
        {
            "link": f"/example/html/file/Country/{_id}/flag",
            "value": "flag.png",
        },
        {
            "value": "image/png",
        },
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_file_type_changes_single_object(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    app, _id = _prep_file_type(manifest_type, tmp_path, rc, postgresql, request)
    resp = app.get(
        f"/example/html/file/Country/{_id}/:changes",
        headers={
            "Accept": "text/html",
        },
    )
    assert _table(resp.context["data"])[0][6:] == [
        {"color": "#f5f5f5", "value": ""},
        {
            "value": "Lithuania",
        },
        {
            "value": "flag.png",
            "link": f"/example/html/file/Country/{_id}/flag",
        },
        {
            "value": "image/png",
        },
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_file_type_no_pk(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    app, _id = _prep_file_type(manifest_type, tmp_path, rc, postgresql, request)
    resp = app.get(
        "/example/html/file/Country?select(name, flag)",
        headers={
            "Accept": "text/html",
        },
    )
    assert _table(resp.context["data"]) == [
        [
            {
                "value": "Lithuania",
            },
            {
                "value": "flag.png",
                "link": f"/example/html/file/Country/{_id}/flag",
            },
            {
                "value": "image/png",
            },
        ]
    ]


@pytest.mark.parametrize(
    "limit, exhausted, result",
    [
        (0, False, []),
        (1, False, [1]),
        (2, False, [1, 2]),
        (3, True, [1, 2, 3]),
        (4, True, [1, 2, 3]),
        (5, True, [1, 2, 3]),
    ],
)
def test_limit_iter(limit, exhausted, result):
    it = _LimitIter(limit, iter([1, 2, 3]))
    assert list(it) == result
    assert it.exhausted is exhausted


@pytest.mark.manifests("internal_sql", "csv")
def test_prepare_ref_for_response(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | Country        |         | name    |
      |   |   |   | name       | string  |         | open
      |   |   | City           |         | name    |
      |   |   |   | name       | string  |         | open
      |   |   |   | country    | ref     | Country | open
    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    fmt = Html()
    value = {"_id": "c634dbd8-416f-457d-8bda-5a6c35bbd5d6"}
    cell = Cell("c634dbd8", link="/example/Country/c634dbd8-416f-457d-8bda-5a6c35bbd5d6")
    dtype = commands.get_model(context, manifest, "example/City").properties["country"].dtype
    result = commands.prepare_dtype_for_response(
        context,
        fmt,
        dtype,
        value,
        data={},
        action=Action.GETALL,
        select=None,
    )
    assert list(result) == ["_id"]
    assert isinstance(result["_id"], Cell)
    assert result["_id"].value == cell.value
    assert result["_id"].color == cell.color
    assert result["_id"].link == cell.link


@pytest.mark.manifests("internal_sql", "csv")
def test_prepare_ref_for_response_empty(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | Country        |         | name    |
      |   |   |   | name       | string  |         | open
      |   |   | City           |         | name    |
      |   |   |   | name       | string  |         | open
      |   |   |   | country    | ref     | Country | open
    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    fmt = Html()
    value = None
    cell = Cell("", link=None, color=Color.null)
    dtype = commands.get_model(context, manifest, "example/City").properties["country"].dtype
    result = commands.prepare_dtype_for_response(
        context,
        fmt,
        dtype,
        value,
        data={},
        action=Action.GETALL,
        select=None,
    )
    assert result == cell
    assert result.value == cell.value
    assert result.color == cell.color
    assert result.link == cell.link


@pytest.mark.manifests("internal_sql", "csv")
def test_select_id(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | City           |         | name    |
      |   |   |   | name       | string  |         | open
    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    result = render_data(
        context,
        manifest,
        "example/City",
        "select(_id)",
        accept="text/html",
        data={
            "_id": "19e4f199-93c5-40e5-b04e-a575e81ac373",
            "_revision": "b6197bb7-3592-4cdb-a61c-5a618f44950c",
        },
    )
    assert result == {
        "_id": Cell(
            value="19e4f199",
            link="/example/City/19e4f199-93c5-40e5-b04e-a575e81ac373",
            color=None,
        ),
    }


@pytest.mark.manifests("internal_sql", "csv")
def test_select_join(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    context, manifest = load_manifest_and_context(
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
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    result = render_data(
        context,
        manifest,
        "example/City",
        "select(_id, country.name)",
        accept="text/html",
        data={
            "_id": "19e4f199-93c5-40e5-b04e-a575e81ac373",
            "_revision": "b6197bb7-3592-4cdb-a61c-5a618f44950c",
            "country": {"name": "Lithuania"},
        },
    )
    assert result == {
        "_id": Cell(
            value="19e4f199",
            link="/example/City/19e4f199-93c5-40e5-b04e-a575e81ac373",
            color=None,
        ),
        "country.name": Cell(value="Lithuania", link=None, color=None),
    }


@pytest.mark.manifests("internal_sql", "csv")
def test_select_join_multiple_props(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    context, manifest = load_manifest_and_context(
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
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    result = render_data(
        context,
        manifest,
        "example/City",
        "select(_id, country._id, country.name)",
        accept="text/html",
        data={
            "_id": "19e4f199-93c5-40e5-b04e-a575e81ac373",
            "_revision": "b6197bb7-3592-4cdb-a61c-5a618f44950c",
            "country": {
                "_id": "262f6c72-4284-4d26-b9b0-e282bfe46a46",
                "name": "Lithuania",
            },
        },
    )
    assert result == {
        "_id": Cell(
            value="19e4f199",
            link="/example/City/19e4f199-93c5-40e5-b04e-a575e81ac373",
            color=None,
        ),
        "country._id": Cell(
            value="262f6c72",
            link="/example/Country/262f6c72-4284-4d26-b9b0-e282bfe46a46",
            color=None,
        ),
        "country.name": Cell(value="Lithuania", link=None, color=None),
    }


@pytest.mark.manifests("internal_sql", "csv")
def test_recursive_refs(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property | type    | ref      | access
    example                  |         |          |
      |   |   | Category     |         |          |
      |   |   |   | name     | string  |          | open
      |   |   |   | parent   | ref     | Category | open
    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    result = render_data(
        context,
        manifest,
        "example/Category/262f6c72-4284-4d26-b9b0-e282bfe46a46",
        query=None,
        accept="text/html",
        data={
            "_id": "262f6c72-4284-4d26-b9b0-e282bfe46a46",
            "_revision": "b6197bb7-3592-4cdb-a61c-5a618f44950c",
            "_type": "example/Category",
            "_page": b"encoded",
            "name": "Leaf",
            "parent": {
                "_id": "19e4f199-93c5-40e5-b04e-a575e81ac373",
            },
        },
    )
    assert result == {
        "_id": Cell(
            value="262f6c72",
            link="/example/Category/262f6c72-4284-4d26-b9b0-e282bfe46a46",
            color=None,
        ),
        "_revision": Cell(
            value="b6197bb7-3592-4cdb-a61c-5a618f44950c",
            link=None,
            color=None,
        ),
        "_type": Cell(
            value="example/Category",
            link=None,
            color=None,
        ),
        "name": Cell(value="Leaf", link=None, color=None),
        "parent._id": Cell(
            value="19e4f199",
            link="/example/Category/19e4f199-93c5-40e5-b04e-a575e81ac373",
            color=None,
        ),
    }


@pytest.mark.manifests("internal_sql", "csv")
def test_show_single_object(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
) -> Tuple[TestClient, str]:
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property    | type    | ref     | access
        example/html                |         |         |
          |   |   | City            |         | id      |
          |   |   |   | id          | integer |         | open
          |   |   |   | name        | string  |         | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel(
        "example/html/City",
        [
            "insert",
            "getall",
            "getone",
            "changes",
            "search",
        ],
    )
    record_id = "6b1b4150-2aae-47b2-b28f-e750a28536e5"
    # Add a file
    resp = app.post(
        "example/html/City",
        json={
            "_id": record_id,
            "id": 1,
            "name": "Vilnius",
        },
    )

    assert resp.status_code == 201

    resp_for_rec = app.get("example/html/City/{0}".format(record_id))

    resp_html_tree_single_object_id = html.fromstring(resp_for_rec.content)
    result = resp_html_tree_single_object_id.xpath("//table//tbody//tr//th")
    assert resp_html_tree_single_object_id != []

    resp = app.get(
        f"example/html/City/{record_id}/:changes",
        headers={
            "Accept": "text/html",
        },
    )

    resp_html_tree = html.fromstring(resp.content)

    result = resp_html_tree.xpath("//table//thead")
    assert result != []


def test_show_external_ref(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
) -> Tuple[TestClient, str]:
    context, manifest = load_manifest_and_context(
        rc,
        """
        d | r | b | m | property    | type    | ref     | access | level
        example/show                |         |         |        |
          |   |   | Country         |         |         |        |
          |   |   |   | id          | integer |         | open   |
          |   |   |   | name        | string  |         | open   |
          |   |   | City            |         |         |        |
          |   |   |   | id          | integer |         | open   |
          |   |   |   | name        | string  |         | open   |
          |   |   |   | country     | ref     | Country | open   | 0
    """,
    )
    result = render_data(
        context,
        manifest,
        "example/show/City/262f6c72-4284-4d26-b9b0-e282bfe46a46",
        query=None,
        accept="text/html",
        data={
            "_id": "262f6c72-4284-4d26-b9b0-e282bfe46a46",
            "_revision": "b6197bb7-3592-4cdb-a61c-5a618f44950c",
            "_type": "example/show/City",
            "_page": b"encoded",
            "id": "0",
            "name": "Vilnius",
            "country": {
                "_id": None,
            },
        },
    )
    assert result == {
        "_id": Cell(
            value="262f6c72",
            link="/example/show/City/262f6c72-4284-4d26-b9b0-e282bfe46a46",
            color=None,
        ),
        "_revision": Cell(
            value="b6197bb7-3592-4cdb-a61c-5a618f44950c",
            link=None,
            color=None,
        ),
        "_type": Cell(
            value="example/show/City",
            link=None,
            color=None,
        ),
        "id": Cell(value="0", link=None, color=None),
        "name": Cell(value="Vilnius", link=None, color=None),
        "country._id": Cell(
            value="",
            link=None,
            color=Color.null,
        ),
    }


@pytest.mark.manifests("internal_sql", "csv")
def test_html_text(
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
    example/html/text         |         |         |         |       | 
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
    app.authmodel("example/html", ["insert", "getall", "search"])

    pushdata(app, "/example/html/text/Country", {"id": 0, "name": {"lt": "Lietuva", "en": "Lithuania", "C": "LT"}})
    pushdata(app, "/example/html/text/Country", {"id": 1, "name": {"lt": "Anglija", "en": "England", "C": "UK"}})

    resp = app.get(
        "/example/html/text/Country/:format/html?select(id,name)&sort(id)",
        headers=Headers(headers={"accept-language": "lt"}),
    )

    assert _table_with_header(resp) == [
        {"id": {"value": 0}, "name": {"value": "Lietuva"}},
        {"id": {"value": 1}, "name": {"value": "Anglija"}},
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_html_text_with_lang(
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
    example/html/text/lang    |         |         |         |       | 
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
    app.authmodel("example/html", ["insert", "getall", "search"])

    pushdata(app, "/example/html/text/lang/Country", {"id": 0, "name": {"lt": "Lietuva", "en": "Lithuania", "C": "LT"}})
    pushdata(app, "/example/html/text/lang/Country", {"id": 1, "name": {"lt": "Anglija", "en": "England", "C": "UK"}})

    resp = app.get(
        "/example/html/text/lang/Country/:format/html?lang(*)&select(id,name)&sort(id)",
        headers=Headers(headers={"accept-language": "lt"}),
    )

    assert _table_with_header(resp) == [
        {
            "id": {"value": 0},
            "name@C": {"value": "LT"},
            "name@en": {"value": "Lithuania"},
            "name@lt": {"value": "Lietuva"},
        },
        {
            "id": {"value": 1},
            "name@C": {"value": "UK"},
            "name@en": {"value": "England"},
            "name@lt": {"value": "Anglija"},
        },
    ]

    resp = app.get(
        "/example/html/text/lang/Country/:format/html?lang(en)&select(id,name)&sort(id)",
        headers=Headers(headers={"accept-language": "lt"}),
    )

    assert _table_with_header(resp) == [
        {
            "id": {"value": 0},
            "name": {"value": "Lithuania"},
        },
        {
            "id": {"value": 1},
            "name": {"value": "England"},
        },
    ]

    resp = app.get(
        "/example/html/text/lang/Country/:format/html?lang(en,lt)&select(id,name)&sort(id)",
        headers=Headers(headers={"accept-language": "lt"}),
    )

    assert _table_with_header(resp) == [
        {"id": {"value": 0}, "name@en": {"value": "Lithuania"}, "name@lt": {"value": "Lietuva"}},
        {"id": {"value": 1}, "name@en": {"value": "England"}, "name@lt": {"value": "Anglija"}},
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_html_changes_text(
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
    example/html/text/changes |         |         |         |       | 
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
    app.authmodel("example/html", ["insert", "getall", "search", "changes"])

    pushdata(
        app, "/example/html/text/changes/Country", {"id": 0, "name": {"lt": "Lietuva", "en": "Lithuania", "C": "LT"}}
    )
    pushdata(
        app, "/example/html/text/changes/Country", {"id": 1, "name": {"lt": "Anglija", "en": "England", "C": "UK"}}
    )

    resp = app.get(
        "/example/html/text/changes/Country/:changes/-10/:format/html?select(id,name)",
        headers=Headers(headers={"accept-language": "lt"}),
    )

    assert _table_with_header(resp) == [
        {
            "id": {"value": 0},
            "name@C": {"value": "LT"},
            "name@en": {"value": "Lithuania"},
            "name@lt": {"value": "Lietuva"},
        },
        {
            "id": {"value": 1},
            "name@C": {"value": "UK"},
            "name@en": {"value": "England"},
            "name@lt": {"value": "Anglija"},
        },
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_html_empty(
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
    example/html/empty       |         |         |         |       | 
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
    app.authmodel("example/html", ["insert", "getall", "search", "changes"])

    resp = app.get("/example/html/empty/Country/:format/html?select(id,name)")
    assert resp.context["header"] == ["id", "name"]
    assert resp.context["data"] == []


@pytest.mark.manifests("internal_sql", "csv")
def test_html_changes_text_one(
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
    example/html/text/changes |         |         |         |       | 
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
    app.authmodel("example/html", ["insert", "getall", "search", "changes"])

    pushdata(app, "/example/html/text/changes/Country", {"id": 0, "name": {"en": "Lietuva"}})
    pushdata(
        app, "/example/html/text/changes/Country", {"id": 1, "name": {"lt": "Anglija", "en": "England", "C": "UK"}}
    )

    resp = app.get(
        "/example/html/text/changes/Country/:changes/-10/:format/html",
        headers=Headers(headers={"accept-language": "lt"}),
    )

    table = _table_with_header(resp)
    first = table[0]
    second = table[1]

    # This is hacky, but changes does not support full AST query parsing
    assert first["id"] == {"value": 0}
    assert first["name@en"] == {"value": "Lietuva"}
    assert first["name@lt"] == {"color": Color.null.value, "value": ""}
    assert first["name@C"] == {"color": Color.null.value, "value": ""}
    assert second["id"] == {"value": 1}
    assert second["name@en"] == {"value": "England"}
    assert second["name@lt"] == {"value": "Anglija"}
    assert second["name@C"] == {"value": "UK"}


@pytest.mark.manifests("internal_sql", "csv")
def test_html_changes_corrupt_data(
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
    example/html/changes/corrupt |         |         |         |       | 
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
    app.authmodel("example/html", ["insert", "getall", "search", "changes"])
    country_id = str(uuid.uuid4())
    city_id = str(uuid.uuid4())
    pushdata(app, "/example/html/changes/corrupt/Country", {"_id": country_id, "id": 0, "name": "Lietuva"})
    pushdata(
        app,
        "/example/html/changes/corrupt/City",
        {
            "_id": city_id,
            "id": 0,
            "name": "Vilnius",
            "country": {"_id": country_id, "test": "t_lt"},
            "obj": {"test": "t_obj"},
        },
    )

    resp = app.get("/example/html/changes/corrupt/City/:changes/-10/:format/html")
    table = _table_with_header(resp)
    data = table[0]
    # Exclude reserved properties
    value = {key: value for key, value in data.items() if not key.startswith("_")}
    assert list(value.keys()) == ["id", "name", "country._id", "country.test", "obj.test"]
    assert value["id"] == {"value": 0}
    assert value["name"] == {"value": "Vilnius"}
    assert value["country._id"] == {
        "value": country_id[:8],
        "link": "/example/html/changes/corrupt/Country/" + country_id,
    }
    assert value["country.test"] == {"value": "t_lt"}
    assert value["obj.test"] == {"value": "t_obj"}

    # Corrupt changelog data
    store = context.get("store")
    backend: PostgreSQL = store.manifest.backend
    model = commands.get_model(context, store.manifest, "example/html/changes/corrupt/City")
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

    resp = app.get("/example/html/changes/corrupt/City/:changes/-10/:format/html")
    table = _table_with_header(resp)
    data = table[0]
    # Exclude reserved properties
    value = {key: value for key, value in data.items() if not key.startswith("_")}
    assert list(value.keys()) == ["id", "name", "country._id", "country.test", "obj.test"]

    assert value["id"] == {"value": 0}
    assert value["name"] == {"value": "Vilnius"}
    assert value["country._id"] == {
        "value": country_id[:8],
        "link": "/example/html/changes/corrupt/Country/" + country_id,
    }
    assert value["country.test"] == {"color": Color.null.value, "value": ""}
    assert value["obj.test"] == {"value": "t_obj_updated"}
