from pathlib import Path

from _pytest.fixtures import FixtureRequest

from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata, send
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.utils import get_error_codes
import pytest


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_changes", "spinta_patch"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:changes", "uapi:/:patch"]
    ]
)
def test_ref_change_assignment(
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
    d | r | b | m | property | type   | ref                           | source       | level | access
    datasets/ref             |        |                               |              |       |
      |   |   | Country      |        | code                          |              |       |
      |   |   |   | code     | string |                               |              |       | open
      |   |   |   | name     | string |                               |              |       | open
      |   |   | City         |        |                               |              |       |
      |   |   |   | name     | string |                               |              |       | open
      |   |   |   | country  | ref    | Country                       |              | 4     | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)

    city_model = "datasets/ref/City"
    country_model = "datasets/ref/Country"
    lt = send(app, country_model, "insert", {"code": "LT", "name": "Lithuania"})
    lv = send(app, country_model, "insert", {"code": "LV", "name": "Latvia"})

    vln = send(app, city_model, "insert", {"name": "Vilnius", "country": {"_id": lv.id}})

    result = app.get(city_model)
    assert listdata(result, "name", "country") == [("Vilnius", {"_id": lv.id})]

    send(app, city_model, "patch", vln, {"country": {"_id": lt.id}})

    result = app.get(city_model)
    assert listdata(result, "name", "country") == [("Vilnius", {"_id": lt.id})]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_changes", "spinta_patch"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:changes", "uapi:/:patch"]
    ]
)
def test_ref_unassign(
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
    d | r | b | m | property | type   | ref                           | source       | level | access
    datasets/ref             |        |                               |              |       |
      |   |   | Country      |        | code                          |              |       |
      |   |   |   | code     | string |                               |              |       | open
      |   |   |   | name     | string |                               |              |       | open
      |   |   | City         |        |                               |              |       |
      |   |   |   | name     | string |                               |              |       | open
      |   |   |   | country  | ref    | Country                       |              | 4     | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)

    city_model = "datasets/ref/City"
    country_model = "datasets/ref/Country"
    lt = send(app, country_model, "insert", {"code": "LT", "name": "Lithuania"})

    vln = send(app, city_model, "insert", {"name": "Vilnius", "country": {"_id": lt.id}})

    result = app.get(city_model)
    assert listdata(result, "name", "country") == [("Vilnius", {"_id": lt.id})]

    send(app, city_model, "patch", vln, {"country": None})

    result = app.get(city_model)
    assert listdata(result, "name", "country") == [("Vilnius", None)]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_changes", "spinta_patch"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:changes", "uapi:/:patch"]
    ]
)
def test_ref_unassign_incorrect(
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
    d | r | b | m | property | type   | ref                           | source       | level | access
    datasets/ref             |        |                               |              |       |
      |   |   | Country      |        | code                          |              |       |
      |   |   |   | code     | string |                               |              |       | open
      |   |   |   | name     | string |                               |              |       | open
      |   |   | City         |        |                               |              |       |
      |   |   |   | name     | string |                               |              |       | open
      |   |   |   | country  | ref    | Country                       |              | 4     | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)

    city_model = "datasets/ref/City"
    country_model = "datasets/ref/Country"
    lt = send(app, country_model, "insert", {"code": "LT", "name": "Lithuania"})

    vln = send(app, city_model, "insert", {"name": "Vilnius", "country": {"_id": lt.id}})

    result = app.get(city_model)
    assert listdata(result, "name", "country") == [("Vilnius", {"_id": lt.id})]
    resp = app.patch(f"{city_model}/{vln.id}", json={"_revision": vln.rev, "country": {"_id": None}})

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["DirectRefValueUnassignment"]
