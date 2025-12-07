import base64

import pytest

import xml.etree.ElementTree as ET

from spinta.testing.client import create_test_client
from spinta.testing.manifest import bootstrap_manifest
from _pytest.fixtures import FixtureRequest


@pytest.mark.models(
    "backends/postgres/dtypes/Binary",
)
def test_insert(model, app):
    data = base64.b64encode(b"data").decode("ascii")
    app.authmodel(model, ["insert"])
    resp = app.post(f"/{model}", json={"blob": data})
    assert resp.status_code == 201, resp.json()
    assert resp.json()["blob"] == data


@pytest.mark.models(
    "backends/postgres/dtypes/Binary",
)
def test_upsert(model, app):
    data = base64.b64encode(b"data").decode("ascii")
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel(model, ["upsert"])
    if ":dataset/" in model:
        pk = "844b08602aeffbf0d12dbfd5f2e861c7501ed2cb"
    else:
        pk = "9ea9cf88-68f6-4753-b9e6-ce3d40ba1861"
    resp = app.post(
        f"/{model}",
        json={
            "_op": "upsert",
            "_where": f'_id="{pk}"',
            "_id": pk,
            "blob": data,
        },
    )
    assert resp.status_code == 201, resp.json()
    assert resp.json()["_id"] == pk
    assert resp.json()["blob"] == data

    resp = app.post(
        f"/{model}",
        json={
            "_op": "upsert",
            "_where": f'_id="{pk}"',
            "_id": pk,
            "blob": data,
        },
    )
    assert resp.status_code == 200, resp.json()
    assert resp.json()["_id"] == pk
    assert "blob" not in resp.json()


@pytest.mark.models(
    "datasets/dtypes/Binary",
)
def test_getone(model, app):
    data = base64.b64encode(b"data").decode("ascii")
    app.authmodel(model, ["insert", "getone"])
    resp = app.post(f"/{model}", json={"blob": data})
    assert resp.status_code == 201, resp.json()
    assert resp.json()["blob"] == data

    pk = resp.json()["_id"]
    resp = app.get(f"/{model}/{pk}")
    assert resp.status_code == 200, resp.json()
    assert resp.json()["blob"] == data


@pytest.mark.models(
    "datasets/dtypes/Binary",
)
def test_getall(model, app):
    data = base64.b64encode(b"data").decode("ascii")
    app.authmodel(model, ["insert", "getall"])
    resp = app.post(f"/{model}", json={"blob": data})
    assert resp.status_code == 201, resp.json()
    assert resp.json()["blob"] == data

    resp = app.get(f"/{model}")
    assert resp.status_code == 200, resp.json()
    assert resp.json()["_data"][0]["blob"] == data


@pytest.mark.manifests("internal_sql", "csv")
def test_rdf(manifest_type: str, tmp_path, rc, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property    | type    | ref      | access | level
    example/dtypes/binary/rdf   |         |          |        |
                                |         |          |        |
      |   |   | Item            |         | id, blob |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | blob        | binary  |          | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authmodel("example/dtypes/binary/rdf", ["insert", "getone", "getall"])

    data = base64.b64encode(b"data").decode("ascii")
    resp = app.post("example/dtypes/binary/rdf/Item", json={"blob": data})
    assert resp.status_code == 201, resp.json()
    assert resp.json()["blob"] == data

    resp = app.get("example/dtypes/binary/rdf/Item/:format/rdf/")
    assert resp.status_code == 200, resp.json()

    root = ET.fromstring(resp.text)
    rdf_description = root.find("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description")
    version_value = rdf_description.attrib["{http://purl.org/pav/}version"]
    page_value = rdf_description.find("{https://testserver/}_page").text
    item_id = rdf_description.attrib["{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about"].split("/")[-1]

    assert (
        resp.text == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns="https://testserver/">\n'
        f'<rdf:Description rdf:about="/example/dtypes/binary/rdf/Item/{item_id}" rdf:type="example/dtypes/binary/rdf/Item" pav:version="{version_value}">\n'
        f"  <_page>{page_value}</_page>\n"
        f"  <blob>{data}</blob>\n"
        f"</rdf:Description>\n"
        f"</rdf:RDF>\n"
    )
