from pathlib import Path

import xml.etree.ElementTree as ET

from _pytest.fixtures import FixtureRequest

from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata, send
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.manifest import load_manifest
from spinta.testing.utils import get_error_codes
import pytest


@pytest.mark.manifests("internal_sql", "csv")
def test_load(manifest_type, tmp_path: Path, rc: RawConfig):
    table = """
    d | r | b | m | property   | type   | ref                  | source       | level | access
    dataset/one                |        |                      |              |       |
      | external               | sql    |                      | sqlite://    |       |
                               |        |                      |              |       |
      |   |   | Country        |        | code                 |              |       |
      |   |   |   | code       | string |                      |              |       | open
      |   |   |   | name       | string |                      |              |       | open
    dataset/two                |        |                      |              |       |
                               |        |                      |              |       |
      |   |   | City           |        |                      |              |       |
      |   |   |   | name       | string |                      |              |       | open
      |   |   |   | country    | ref    | /dataset/one/Country |              | 3     | open
    """
    manifest = load_manifest(rc, table, manifest_type=manifest_type, tmp_path=tmp_path)
    assert manifest == table


@pytest.mark.manifests("internal_sql", "csv")
def test_external_ref(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref                           | source       | level | access
    datasets/externalref     |        |                               |              |       |
      | external             | sql    |                               | sqlite://    |       |
      |   |   | Country      |        | code                          |              |       |
      |   |   |   | code     | string |                               |              |       | open
      |   |   |   | name     | string |                               |              |       | open
    datasets/internal        |        |                               |              |       |
      |   |   | City         |        |                               |              |       |
      |   |   |   | name     | string |                               |              |       | open
      |   |   |   | country  | ref    | /datasets/externalref/Country |              | 3     | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authmodel("datasets/internal/City", ["insert", "getall", "search"])

    resp = app.post(
        "/datasets/internal/City",
        json={
            "country": {"code": "lt"},
            "name": "Vilnius",
        },
    )
    assert resp.status_code == 201

    resp = app.get("/datasets/internal/City")
    assert listdata(resp, full=True) == [{"country.code": "lt", "name": "Vilnius"}]

    resp = app.get("/datasets/internal/City?select(country.code)")
    assert listdata(resp, full=True) == [{"country.code": "lt"}]


@pytest.mark.manifests("internal_sql", "csv")
def test_external_ref_without_primary_key(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref                           | source       | level | access
    datasets/externalref     |        |                               |              |       |
      | external             | sql    |                               | sqlite://    |       |
      |   |   | Country      |        |                               |              |       |
      |   |   |   | code     | string |                               |              |       | open
      |   |   |   | name     | string |                               |              |       | open
    datasets/internal/pk     |        |                               |              |       |
      |   |   | City         |        |                               |              |       |
      |   |   |   | name     | string |                               |              |       | open
      |   |   |   | country  | ref    | /datasets/externalref/Country |              | 3     | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authmodel("datasets/internal/pk/City", ["insert", "getall", "search"])

    _id = "4d741843-4e94-4890-81d9-5af7c5b5989a"
    resp = app.post(
        "/datasets/internal/pk/City",
        json={
            "country": {"_id": _id},
            "name": "Vilnius",
        },
    )
    assert resp.status_code == 201

    resp = app.get("/datasets/internal/pk/City")
    assert listdata(resp, full=True) == [{"country._id": _id, "name": "Vilnius"}]

    resp = app.get("/datasets/internal/pk/City?select(country)")
    assert listdata(resp, full=True) == [{"country._id": _id}]


@pytest.mark.manifests("internal_sql", "csv")
def test_external_ref_with_explicit_key(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref                               | source       | level | access
    datasets/external/ref    |        |                                   |              |       |
      | external             | sql    |                                   | sqlite://    |       |
      |   |   | Country      |        |                                   |              |       |
      |   |   |   | id       | integer|                                   |              |       | open
      |   |   |   | name     | string |                                   |              |       | open
    datasets/explicit/ref    |        |                                   |              |       |
      |   |   | City         |        |                                   |              |       |
      |   |   |   | name     | string |                                   |              |       | open
      |   |   |   | country  | ref    | /datasets/external/ref/Country[id]|              | 3     | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authmodel("datasets/explicit/ref/City", ["insert", "getall", "search"])

    resp = app.post(
        "/datasets/explicit/ref/City",
        json={
            "country": {"_id": 1},
            "name": "Vilnius",
        },
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["FieldNotInResource"]

    resp = app.post(
        "/datasets/explicit/ref/City",
        json={
            "country": {"id": 1},
            "name": "Vilnius",
        },
    )
    assert resp.status_code == 201

    resp = app.get("/datasets/explicit/ref/City")
    assert listdata(resp, full=True) == [{"country.id": 1, "name": "Vilnius"}]

    resp = app.get("/datasets/explicit/ref/City?select(country)")
    assert listdata(resp, full=True) == [{"country.id": 1}]


@pytest.mark.manifests("internal_sql", "csv")
def test_external_ref_unassign(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref                           | source       | level | access
    datasets/external/ref/m  |        |                               |              |       |
      |   |   | Country      |        | code, name                    |              |       |
      |   |   |   | code     | string |                               |              |       | open
      |   |   |   | name     | string |                               |              |       | open
      |   |   | City         |        |                               |              |       |
      |   |   |   | name     | string |                               |              |       | open
      |   |   |   | country  | ref    | Country                       |              | 3     | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(["spinta_insert", "spinta_getall", "spinta_changes", "spinta_patch"])

    city_model = "datasets/external/ref/m/City"
    country_model = "datasets/external/ref/m/Country"
    send(app, country_model, "insert", {"code": "LT", "name": "Lithuania"})

    vln = send(app, city_model, "insert", {"name": "Vilnius", "country": {"code": "LT", "name": "Lithuania"}})

    result = app.get(city_model)
    assert listdata(result, "name", "country") == [("Vilnius", {"code": "LT", "name": "Lithuania"})]

    send(app, city_model, "patch", vln, {"country": None})

    result = app.get(city_model)
    assert listdata(result, "name", "country") == [("Vilnius", None)]


@pytest.mark.manifests("internal_sql", "csv")
def test_external_ref_unassign_invalid(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref                           | source       | level | access
    datasets/external/ref/m  |        |                               |              |       |
      |   |   | Country      |        | code, name                    |              |       |
      |   |   |   | code     | string |                               |              |       | open
      |   |   |   | name     | string |                               |              |       | open
      |   |   | City         |        |                               |              |       |
      |   |   |   | name     | string |                               |              |       | open
      |   |   |   | country  | ref    | Country                       |              | 3     | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(["spinta_insert", "spinta_getall", "spinta_changes", "spinta_patch"])

    city_model = "datasets/external/ref/m/City"
    country_model = "datasets/external/ref/m/Country"
    send(app, country_model, "insert", {"code": "LT", "name": "Lithuania"})

    vln = send(app, city_model, "insert", {"name": "Vilnius", "country": {"code": "LT", "name": "Lithuania"}})

    result = app.get(city_model)
    assert listdata(result, "name", "country") == [("Vilnius", {"code": "LT", "name": "Lithuania"})]

    resp = app.patch(f"{city_model}/{vln.id}", json={"_revision": vln.rev, "country": {"code": None, "name": None}})
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["DirectRefValueUnassignment"]


@pytest.mark.manifests("internal_sql", "csv")
def test_external_ref_unassign_invalid_no_pk(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref                           | source       | level | access
    datasets/external/ref/n   |        |                               |              |       |
      |   |   | Country      |        |                               |              |       |
      |   |   |   | code     | string |                               |              |       | open
      |   |   |   | name     | string |                               |              |       | open
      |   |   | City         |        |                               |              |       |
      |   |   |   | name     | string |                               |              |       | open
      |   |   |   | country  | ref    | Country                       |              | 3     | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(["spinta_insert", "spinta_getall", "spinta_changes", "spinta_patch"])

    city_model = "datasets/external/ref/n/City"
    country_model = "datasets/external/ref/n/Country"
    lt = send(app, country_model, "insert", {"code": "LT", "name": "Lithuania"})

    vln = send(app, city_model, "insert", {"name": "Vilnius", "country": {"_id": lt.id}})

    result = app.get(city_model)
    assert listdata(result, "name", "country") == [("Vilnius", {"_id": lt.id})]

    resp = app.patch(
        f"{city_model}/{vln.id}",
        json={
            "_revision": vln.rev,
            "country": {
                "_id": None,
            },
        },
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["DirectRefValueUnassignment"]


@pytest.mark.skip(reason="format support not implemented")
def test_external_ref_format_rdf(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref                           | source       | level | access
    datasets/externalref     |        |                               |              |       |
      | external             | sql    |                               | sqlite://    |       |
      |   |   | Country      |        | code                          |              |       |
      |   |   |   | code     | string |                               |              |       | open
      |   |   |   | name     | string |                               |              |       | open
    datasets/internal        |        |                               |              |       |
      |   |   | City         |        |                               |              |       |
      |   |   |   | name     | string |                               |              |       | open
      |   |   |   | country  | ref    | /datasets/externalref/Country |              | 3     | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authmodel("datasets/internal/City", ["insert", "getall", "search"])

    resp = app.post(
        "/datasets/internal/City",
        json={
            "country": {"code": "lt"},
            "name": "Vilnius",
        },
    )
    assert resp.status_code == 201

    resp = app.get("/datasets/internal/City/:format/rdf")
    assert resp.status_code == 200

    namespaces = {
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "pav": "http://purl.org/pav/",
        "": "https://testserver/",
    }
    root = ET.fromstring(resp.text)
    rdf_description = root.find("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description")
    id_value = rdf_description.attrib["{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about"].split("/")[-1]
    version_value = rdf_description.attrib["{http://purl.org/pav/}version"]
    page_value = rdf_description.find("_page", namespaces).text

    assert (
        resp.text == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns="https://testserver/">\n'
        f'<rdf:Description rdf:about="/datasets/internal/City/{id_value}" rdf:type="datasets/internal/City" pav:version="{version_value}">\n '
        f" <_page>{page_value}</_page>\n "
        f" <name>Vilnius</name>\n "
        f" <country>\n"
        f'    <rdf:Description rdf:type="datasets/externalref/Country">\n'
        f"      <country>lt</country>\n"
        f"    </rdf:Description>\n"
        f"  </country>\n"
        f"</rdf:Description>\n"
        f"</rdf:RDF>\n"
    )
