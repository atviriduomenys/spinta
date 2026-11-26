"""Forms of `scope.prepare` found while testing spinta#1922: array items (`tags[]`) and base model properties (#2112)."""

from pathlib import Path

import pytest

from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.tabular.helpers import striptable
from spinta.spyna import parse, unparse
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.client import TestClient, create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.tabular import create_tabular_manifest

ARRAY = """
d | r | b | m | property | type    | ref    | prepare      | access
example/arrays           |         |        |              |
  |   |   | City         |         |        |              |
                         | scope   | tagged | tags[] = 'x' |
  |   |   |   | name     | string  |        |              | protected
  |   |   |   | tags[]   | string  |        |              | protected
"""

BASE = """
d | r | b | m | property | type    | ref     | prepare          | access
example/bases            |         |         |                  |
  |   |   | Place        |         |         |                  |
  |   |   |   | name     | string  |         |                  | protected
  |   | Place            |         |         |                  |
  |   |   | City         |         |         |                  |
                         | scope   | vilnius | name = 'Vilnius' |
  |   |   |   | name     | inherit |         |                  | protected
  |   |   |   | code     | string  |         |                  | protected
"""


def _check(context: Context, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path, manifest: str):
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(manifest))
    return cli.invoke(rc, ["check", tmp_path / "manifest.csv"], fail=False)


def test_array_items_round_trip():
    assert unparse(parse("tags[] = 'x'")) == "tags[]='x'"


def test_check_accepts_array_items(context: Context, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    result = _check(context, rc, cli, tmp_path, ARRAY)

    assert result.exit_code == 0, result.exception


def test_check_rejects_a_malformed_bracket_formula(
    context: Context, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    result = _check(context, rc, cli, tmp_path, ARRAY.replace("tags[] = 'x'", "missing[] = 'x'"))

    assert result.exit_code != 0
    assert "missing" in str(result.exception)
    assert not isinstance(result.exception, AttributeError)


def test_check_accepts_base_model_properties(context: Context, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    # `name` is only on the base model: the specification says a model inherits every base field.
    result = _check(
        context,
        rc,
        cli,
        tmp_path,
        BASE.replace("  |   |   |   | name     | inherit |         |                  | protected\n", ""),
    )

    assert result.exit_code == 0, result.exception


@pytest.fixture
def app(rc: RawConfig, tmp_path: Path, postgresql: str, request: pytest.FixtureRequest):
    def create(manifest: str) -> TestClient:
        context = bootstrap_manifest(
            rc.fork({"access": "protected"}), manifest, backend=postgresql, tmp_path=tmp_path, request=request
        )
        return create_test_client(context)

    return create


def test_scope_with_array_items_filters_rows(app):
    client = app(ARRAY)
    client.authmodel("example/arrays/City", ["insert", "getall", "search"])
    for name, tags in (("Vilnius", ["x", "y"]), ("Riga", ["y"])):
        assert client.post("/example/arrays/City", json={"name": name, "tags": tags}).status_code == 201

    resp = client.get("/example/arrays/City/@tagged")

    assert resp.status_code == 200, resp.text
    assert listdata(resp, "name") == ["Vilnius"]


def test_scope_with_base_model_property_filters_rows(app):
    client = app(BASE)
    client.authmodel("example/bases/Place", ["insert", "getall", "search"])
    client.authmodel("example/bases/City", ["insert", "getall", "search"])
    # A base model's object and its model's object share the same `_id`.
    client.authorize(["uapi:/:set_meta_fields"])
    for name, code in (("Vilnius", "vno"), ("Riga", "rix")):
        place = client.post("/example/bases/Place", json={"name": name}).json()
        resp = client.post("/example/bases/City", json={"_id": place["_id"], "code": code})
        assert resp.status_code == 201, resp.text

    resp = client.get("/example/bases/City/@vilnius")

    assert resp.status_code == 200, resp.text
    assert listdata(resp, "code") == ["vno"]


def test_inherited_property_can_be_filtered(app):
    client = app(BASE)
    client.authmodel("example/bases/Place", ["insert", "getall", "search"])
    client.authmodel("example/bases/City", ["insert", "getall", "search"])
    client.authorize(["uapi:/:set_meta_fields"])
    for name, code in (("Vilnius", "vno"), ("Riga", "rix")):
        place = client.post("/example/bases/Place", json={"name": name}).json()
        assert client.post("/example/bases/City", json={"_id": place["_id"], "code": code}).status_code == 201

    resp = client.get('/example/bases/City?name="Riga"')

    assert resp.status_code == 200, resp.text
    assert listdata(resp, "code") == ["rix"]
