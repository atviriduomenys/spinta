from pathlib import Path

from pytest import FixtureRequest
import pytest
from spinta.core.config import RawConfig
from spinta.formats.html.components import Cell
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.manifest import load_manifest_and_context
from spinta.testing.request import render_data
from starlette.datastructures import Headers

from spinta.testing.utils import error


@pytest.mark.manifests("internal_sql", "csv")
def test_text(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property      | type
    backends/postgres/dtypes/text |
      |   |   | Country           |
      |   |   |   | name@lt       | string
      |   |   |   | name@en       | string
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authmodel("backends/postgres/dtypes/text/Country", ["insert", "getall", "search"])

    # Write data
    resp = app.post("/backends/postgres/dtypes/text/Country", json={"name": {"lt": "Lietuva", "en": "Lithuania"}})
    assert resp.status_code == 201

    # Read data
    resp = app.get("/backends/postgres/dtypes/text/Country?select(name@en, name@lt)")
    assert listdata(resp, full=True) == [{"name@en": "Lithuania", "name@lt": "Lietuva"}]

    listdata(resp, full=True)


@pytest.mark.manifests("internal_sql", "csv")
def test_text_patch(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property      | type
    backends/postgres/dtypes/text |
      |   |   | Country           |
      |   |   |   | name@lt       | string
      |   |   |   | name@en       | string
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authmodel("backends/postgres/dtypes/text/Country", ["insert", "patch", "getall", "search"])

    # Write data
    resp = app.post("/backends/postgres/dtypes/text/Country", json={"name": {"lt": "Lietuva", "en": "Lithuania"}})

    assert resp.status_code == 201

    # Patch data
    data = resp.json()
    pk = data["_id"]
    rev = data["_revision"]
    resp = app.patch(
        f"/backends/postgres/dtypes/text/Country/{pk}",
        json={"_revision": rev, "name": {"lt": "Latvija", "en": "Latvia"}},
    )
    assert resp.status_code == 200

    # Read data
    resp = app.get("/backends/postgres/dtypes/text/Country?select(name@lt, name@en)")
    assert listdata(resp, full=True) == [{"name@lt": "Latvija", "name@en": "Latvia"}]


@pytest.mark.manifests("internal_sql", "csv")
def test_html(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property | type   | access
    example                  |        |
      |   |   | Country      |        |
      |   |   |   | name@lt  | string | open
      |   |   |   | name@en  | string | open
    """,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
    )
    result = render_data(
        context,
        manifest,
        "example/Country",
        query="select(_id,name@lt,name@en)",
        accept="text/html",
        data={
            "_id": "262f6c72-4284-4d26-b9b0-e282bfe46a46",
            "_revision": "b6197bb7-3592-4cdb-a61c-5a618f44950c",
            "name": {
                "lt": "Lietuva",
                "en": "Lithuania",
            },
        },
    )
    assert result == {
        "_id": Cell(
            value="262f6c72",
            link="/example/Country/262f6c72-4284-4d26-b9b0-e282bfe46a46",
            color=None,
        ),
        "name@lt": Cell(value="Lietuva", link=None, color=None),
        "name@en": Cell(value="Lithuania", link=None, color=None),
    }


@pytest.mark.manifests("internal_sql", "csv")
def test_text_change_log(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property      | type
    backends/postgres/dtypes/text |
      |   |   | Country           |
      |   |   |   | name@lt       | string
      |   |   |   | name@en       | string
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    model = "backends/postgres/dtypes/text/Country"
    app = create_test_client(context)
    app.authmodel(
        "backends/postgres/dtypes/text/Country",
        [
            "insert",
            "update",
            "delete",
            "changes",
        ],
    )
    resp = app.post(model, json={"name": {"lt": "Lietuva", "en": "Lithuania"}})
    assert resp.status_code == 201

    data = resp.json()
    id_ = data["_id"]

    resp_changes = app.get(f"/{model}/{id_}/:changes")

    assert len(resp_changes.json()["_data"]) == 1
    assert resp_changes.json()["_data"][-1]["_op"] == "insert"
    assert resp_changes.json()["_data"][-1]["name"] == data["name"]

    resp = app.delete(f"/{model}/{id_}")

    assert resp.status_code == 204

    resp_changes = app.get(f"/{model}/{id_}/:changes")

    assert len(resp_changes.json()["_data"]) == 2
    assert resp_changes.json()["_data"][1]["_op"] == "delete"


@pytest.mark.manifests("internal_sql", "csv")
def test_text_select_by_prop(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
        id | d | r | b | m | property | type   | ref | source | prepare | level | access | uri | title | description
           | types/text               |        |     |        |         |       |        |     |       |
           |                          |        |     |        |         |       |        |     |       |
           |   |   |   | Country      |        |     |        |         |       |        |     |       |
           |   |   |   |   | name     | string |     |        |         | 3     | open   |     |       |
           |                          |        |     |        |         |       |        |     |       |
           |   |   |   | Country1     |        |     |        |         |       |        |     |       |
           |   |   |   |   | name@lt  | string   |     |        |         | 3     | open   |     |       |
           |   |   |   |   | name@en  | string   |     |        |         | 3     | open   |     |       |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    model = "types/text/Country1"
    app = create_test_client(context)
    app.authmodel("types/text/Country1", ["insert", "update", "delete", "changes", "search"])
    resp = app.post(f"/{model}", json={"name": {"lt": "Lietuva", "en": "Lithuania"}})

    assert resp.status_code == 201

    select_by_prop = app.get(f"/{model}/?select(name@lt)")
    assert select_by_prop.status_code == 200
    assert len(select_by_prop.json()["_data"]) == 1
    sort_by_prop = app.get(f"/{model}/?sort(name@lt)")
    assert sort_by_prop.status_code == 200


@pytest.mark.manifests("internal_sql", "csv")
def test_text_post_wrong_property_with_text(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property      | type
    backends/postgres/dtypes/text |
      |   |   | Country           |
      |   |   |   | name@lt       | string
      |   |   |   | name@en       | string
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    model = "backends/postgres/dtypes/text/Country"
    app = create_test_client(context)
    app.authmodel(
        "backends/postgres/dtypes/text/Country",
        [
            "insert",
            "update",
            "delete",
            "changes",
        ],
    )

    resp = app.post(f"/{model}", json={"name": "lietuva", "name@en": "lithuania"})
    resp = app.post(
        f"/{model}",
        json={
            "_type": model,
            "title": "lithuania",
        },
    )

    assert resp.status_code != 200


@pytest.mark.manifests("internal_sql", "csv")
def test_text_accept_language(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
        id | d | r | b | m | property | type     | ref | source | prepare | level | access | uri | title | description
           | types/text/accept               |          |     |        |         |       |        |     |       |
           |   |   |   | Country      |          |     |        |         |       |        |     |       |
           |   |   |   |   | name@lt  | string   |     |        |         | 3     | open   |     |       |
           |   |   |   |   | name@en  | string   |     |        |         | 3     | open   |     |       |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    model = "types/text/accept/Country"
    app = create_test_client(context)
    app.authmodel("types/text/accept/Country", ["insert", "update", "delete", "changes", "search"])
    resp = app.post(f"/{model}", json={"name": {"lt": "Lietuva", "en": "Lithuania"}})

    assert resp.status_code == 201

    select_by_prop = app.get(f"/{model}/?select(name)", headers=Headers(headers={"accept-language": "en"}))
    assert select_by_prop.status_code == 200
    assert len(select_by_prop.json()["_data"]) == 1
    assert select_by_prop.json()["_data"] == [{"name": "Lithuania"}]

    select_by_prop = app.get(f"/{model}/?select(name)", headers=Headers(headers={"accept-language": "lt"}))
    assert select_by_prop.status_code == 200
    assert len(select_by_prop.json()["_data"]) == 1
    assert select_by_prop.json()["_data"] == [{"name": "Lietuva"}]


@pytest.mark.manifests("internal_sql", "csv")
def test_text_content_language(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
        id | d | r | b | m | property | type     | ref | source | prepare | level | access | uri | title | description
           | types/text/content               |          |     |        |         |       |        |     |       |
           |   |   |   | Country      |          |     |        |         |       |        |     |       |
           |   |   |   |   | name@lt  | string   |     |        |         | 3     | open   |     |       |
           |   |   |   |   | name@en  | string   |     |        |         | 3     | open   |     |       |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    model = "types/text/content/Country"
    app = create_test_client(context)
    app.authmodel("types/text/content/Country", ["insert", "update", "delete", "changes", "search"])
    resp = app.post(f"/{model}", json={"name": "Lithuania"}, headers=Headers(headers={"content-language": "en"}))

    assert resp.status_code == 201

    select_by_prop = app.get(f"/{model}/?select(name@en,name@lt)")
    assert select_by_prop.status_code == 200
    assert len(select_by_prop.json()["_data"]) == 1
    assert select_by_prop.json()["_data"] == [{"name": {"lt": None, "en": "Lithuania"}}]


@pytest.mark.manifests("internal_sql", "csv")
def test_text_unknown_language(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
        id | d | r | b | m | property | type     | ref | source | prepare | level | access | uri | title | description
           | types/text/content       |          |     |        |         |       |        |     |       |
           |   |   |   | Country      |          |     |        |         |       |        |     |       |
           |   |   |   |   | name     | text     |     |        |         | 3     | open   |     |       |
           |   |   |   |   | name@lt  | string   |     |        |         |       | open   |     |       |
           |   |   |   |   | name@en  | string   |     |        |         |       | open   |     |       |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    model = "types/text/content/Country"
    app = create_test_client(context)
    app.authmodel("types/text/content/Country", ["insert", "update", "delete", "changes", "search"])
    resp = app.post(f"/{model}", json={"name": {"": "LT", "lt": "Lietuva"}})

    assert resp.status_code == 201

    select_by_prop = app.get(f"/{model}/?select(name@C)")
    assert select_by_prop.status_code == 200
    assert len(select_by_prop.json()["_data"]) == 1
    assert select_by_prop.json()["_data"] == [{"name": {"": "LT"}}]


@pytest.mark.manifests("internal_sql", "csv")
def test_text_unknown_language_invalid(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
        id | d | r | b | m | property | type     | ref | source | prepare | level | access | uri | title | description
           | types/text/content               |          |     |        |         |       |        |     |       |
           |   |   |   | Country      |          |     |        |         |       |        |     |       |
           |   |   |   |   | name     | text     |     |        |         | 4     | open   |     |       |
           |   |   |   |   | name@lt  | string   |     |        |         |       | open   |     |       |
           |   |   |   |   | name@en  | string   |     |        |         |       | open   |     |       |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    model = "types/text/content/Country"
    app = create_test_client(context)
    app.authmodel("types/text/content/Country", ["insert", "update", "delete", "changes", "search"])
    resp = app.post(f"/{model}", json={"name": {"C": "LT", "lt": "Lietuva"}})
    assert resp.status_code == 400
    assert error(resp) == "LangNotDeclared"
