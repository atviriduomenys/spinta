"""Custom scopes granted in the access token limit the rows and columns a client gets (spinta#2111)."""

from pathlib import Path

import pytest
import sqlalchemy as sa

from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.client import TestClient, create_client, create_test_client
from spinta.testing.data import listdata
from spinta.testing.datasets import Sqlite
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.utils import error

MANIFEST = """
d | r | b | m | property      | type   | ref     | source       | prepare           | access
example                       |        |         |              |                   |
  | resource                  | sql    |         |              |                   |
  |   |   | Country           |        | code    | COUNTRY      |                   |
  |   |   |   | code          | string |         | CODE         |                   | protected
  |   |   |   | title         | string |         | TITLE        |                   | protected
  |   |   | City              |        |         | CITY         |                   |
                              | scope  | ltu     |              | country_code='lt' |
                              | scope  | names   |              | select(name)      |
  |   |   |   | name          | string |         | NAME         |                   | protected
  |   |   |   | country_code  | string |         | COUNTRY_CODE |                   | protected
  |   |   |   | country       | ref    | Country | COUNTRY_CODE |                   | protected
"""


def _scopes(*names_and_actions: str) -> list[str]:
    return [f"uapi:/example/City/{name}" for name in names_and_actions]


@pytest.fixture
def app(context: Context, rc: RawConfig, tmp_path: Path, sqlite: Sqlite) -> TestClient:
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(MANIFEST))
    sqlite.init(
        {
            "COUNTRY": [sa.Column("CODE", sa.Text), sa.Column("TITLE", sa.Text)],
            "CITY": [sa.Column("NAME", sa.Text), sa.Column("COUNTRY_CODE", sa.Text)],
        }
    )
    sqlite.write("COUNTRY", [{"CODE": "lt", "TITLE": "Lithuania"}, {"CODE": "lv", "TITLE": "Latvia"}])
    sqlite.write(
        "CITY",
        [
            {"NAME": "Vilnius", "COUNTRY_CODE": "lt"},
            {"NAME": "Kaunas", "COUNTRY_CODE": "lt"},
            {"NAME": "Riga", "COUNTRY_CODE": "lv"},
        ],
    )
    # Serve protected data, so only the token's scopes grant access.
    return create_client(rc.fork({"access": "protected"}), tmp_path, sqlite)


def test_single_scope_in_token_limits_rows(app: TestClient):
    app.authorize(_scopes("@ltu/:getall"), strict_set=True)

    resp = app.get("/example/City")

    assert resp.status_code == 200, resp.text
    assert sorted(listdata(resp, "name")) == ["Kaunas", "Vilnius"]


def test_user_filter_cannot_widen_the_scope(app: TestClient):
    app.authorize(_scopes("@ltu/:getall", "@ltu/:search"), strict_set=True)

    assert listdata(app.get('/example/City?country_code="lv"'), "name") == []
    assert listdata(app.get('/example/City?name="Vilnius"'), "name") == ["Vilnius"]


def test_count_respects_the_scope(app: TestClient):
    app.authorize(_scopes("@ltu/:getall", "@ltu/:search"), strict_set=True)

    resp = app.get("/example/City?count()")

    assert resp.status_code == 200, resp.text
    assert listdata(resp, "count()") == [2]


def test_column_scope_in_token_limits_columns(app: TestClient):
    app.authorize(_scopes("@names/:getall", "@names/:search"), strict_set=True)

    resp = app.get("/example/City")

    assert resp.status_code == 200, resp.text
    assert all(set(row) - {"_type", "_id", "_revision"} == {"name"} for row in resp.json()["_data"])
    # As with a scope named in the URL (spinta#1937), properties outside the scope do not exist for the client.
    assert error(app.get("/example/City?select(country_code)")) == "PropertyNotFound"
    assert error(app.get('/example/City?country_code="lt"')) == "PropertiesNotFound"
    assert error(app.get("/example/City?sort(country_code)")) == "PropertiesNotFound"


def test_model_scope_in_token_is_not_limited(app: TestClient):
    app.authorize(["uapi:/example/City/:getall"], strict_set=True)

    resp = app.get("/example/City")

    assert sorted(listdata(resp, "name")) == ["Kaunas", "Riga", "Vilnius"]


def test_several_scopes_in_token_require_choosing_one(app: TestClient):
    app.authorize(_scopes("@ltu/:getall", "@names/:getall"), strict_set=True)

    assert app.get("/example/City").status_code == 400

    resp = app.get("/example/City/@ltu")
    assert resp.status_code == 200, resp.text
    assert sorted(listdata(resp, "name")) == ["Kaunas", "Vilnius"]


def test_scope_not_in_token_is_forbidden(app: TestClient):
    app.authorize(_scopes("@ltu/:getall"), strict_set=True)

    assert app.get("/example/City/@names").status_code == 403


def test_getone_outside_the_scope_is_not_found(app: TestClient):
    app.authorize(["uapi:/example/City/:getall"], strict_set=True)
    ids = {row["name"]: row["_id"] for row in app.get("/example/City").json()["_data"]}
    app.authorize(_scopes("@ltu/:getone"), strict_set=True)

    assert app.get(f"/example/City/{ids['Vilnius']}").status_code == 200
    assert app.get(f"/example/City/{ids['Riga']}").status_code == 404


def test_output_formats_respect_the_scope(app: TestClient):
    app.authorize(_scopes("@ltu/:getall"), strict_set=True)

    resp = app.get("/example/City/:format/csv")

    assert resp.status_code == 200, resp.text
    assert "Vilnius" in resp.text and "Kaunas" in resp.text
    assert "Riga" not in resp.text


def test_limit_counts_within_the_scope(app: TestClient):
    app.authorize(_scopes("@ltu/:search"), strict_set=True)

    resp = app.get("/example/City?sort(name)&limit(5)")

    assert resp.status_code == 200, resp.text
    assert listdata(resp, "name") == ["Kaunas", "Vilnius"]


def test_getone_returns_only_the_scope_columns(app: TestClient):
    app.authorize(["uapi:/example/City/:getall"], strict_set=True)
    vilnius = next(row["_id"] for row in app.get("/example/City").json()["_data"] if row["name"] == "Vilnius")
    app.authorize(_scopes("@names/:getone"), strict_set=True)

    resp = app.get(f"/example/City/{vilnius}")

    assert resp.status_code == 200, resp.text
    assert set(resp.json()) - {"_type", "_id", "_revision"} == {"name"}


def test_expand_does_not_reveal_a_model_the_token_does_not_grant(app: TestClient):
    app.authorize(_scopes("@ltu/:getall"), strict_set=True)

    resp = app.get("/example/City?expand(country)")

    assert "Lithuania" not in resp.text


INTERNAL = """
d | r | b | m | property      | type   | ref | prepare           | access
example/internal              |        |     |                   |
  |   |   | City              |        |     |                   |
                              | scope  | ltu | country_code='lt' |
  |   |   |   | name          | string |     |                   | protected
  |   |   |   | country_code  | string |     |                   | protected
  |   |   |   | info          | object |     |                   | protected
  |   |   |   | info.note     | string |     |                   | protected
"""


@pytest.fixture
def internal_app(rc: RawConfig, tmp_path: Path, postgresql: str, request: pytest.FixtureRequest) -> TestClient:
    context = bootstrap_manifest(
        rc.fork({"access": "protected"}), INTERNAL, backend=postgresql, tmp_path=tmp_path, request=request
    )
    app = create_test_client(context)
    app.authmodel("example/internal/City", ["insert"])
    for name, code in (("Vilnius", "lt"), ("Riga", "lv")):
        resp = app.post("/example/internal/City", json={"name": name, "country_code": code, "info": {"note": name}})
        assert resp.status_code == 201, resp.text
    return app


def test_internal_backend_limits_rows_by_scope(internal_app: TestClient):
    internal_app.authorize(["uapi:/example/internal/City/@ltu/:getall"], strict_set=True)

    assert listdata(internal_app.get("/example/internal/City"), "name") == ["Vilnius"]


def test_changes_are_refused_for_scope_only_access(internal_app: TestClient):
    internal_app.authorize(
        ["uapi:/example/internal/City/@ltu/:changes", "uapi:/example/internal/City/@ltu/:getall"], strict_set=True
    )

    assert internal_app.get("/example/internal/City/:changes").status_code == 403


def test_writes_are_refused_for_scope_only_access(internal_app: TestClient):
    internal_app.authorize(["uapi:/example/internal/City/@ltu/:insert"], strict_set=True)

    resp = internal_app.post("/example/internal/City", json={"name": "Kaunas", "country_code": "lt"})

    assert resp.status_code == 403


def test_subresource_of_an_object_outside_the_scope_is_not_found(internal_app: TestClient):
    internal_app.authorize(["uapi:/example/internal/City/:getall"], strict_set=True)
    ids = {row["name"]: row["_id"] for row in internal_app.get("/example/internal/City").json()["_data"]}
    internal_app.authorize(["uapi:/example/internal/City/@ltu/:getone"], strict_set=True)

    assert internal_app.get(f"/example/internal/City/{ids['Vilnius']}/info").status_code == 200
    assert internal_app.get(f"/example/internal/City/{ids['Riga']}/info").status_code == 404


def test_count_is_allowed_under_a_column_scope(app: TestClient):
    # count() reads no column, so a scope that selects columns does not forbid it.
    app.authorize(_scopes("@names/:getall", "@names/:search"), strict_set=True)

    resp = app.get("/example/City?count()")

    assert resp.status_code == 200, resp.text
    assert listdata(resp, "count()") == [3]


def test_count_is_allowed_under_a_column_scope_named_in_the_url(app: TestClient):
    app.authorize(["uapi:/example/City/:getall", "uapi:/example/City/:search"], strict_set=True)

    resp = app.get("/example/City/@names?count()")

    assert resp.status_code == 200, resp.text
    assert listdata(resp, "count()") == [3]


def test_function_over_a_column_outside_the_scope_is_refused(app: TestClient):
    app.authorize(_scopes("@names/:getall", "@names/:search"), strict_set=True)

    assert error(app.get("/example/City?select(max(country_code))")) == "PropertyNotFound"
