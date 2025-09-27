import uuid
from pathlib import Path

from pytest import FixtureRequest

from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.utils import get_error_codes, get_error_context
import pytest


@pytest.mark.manifests("internal_sql", "csv")
def test_insert_with_required_property(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    dataset = "type/required/insert"
    context = bootstrap_manifest(
        rc,
        f"""
    d | r | b | m | property          | type            | ref
    {dataset}                         |                 |
      |   |   | City                  |                 |
      |   |   |   | name              | string required |
      |   |   |   | description       | string          |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authmodel(f"{dataset}/City", ["insert", "getall"])

    resp = app.post(
        f"/{dataset}/City",
        json={
            "description": "Test city",
        },
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["RequiredProperty"]
    assert get_error_context(resp.json(), "RequiredProperty", ["property", "model"]) == {
        "property": "name",
        "model": f"{dataset}/City",
    }
    resp = app.post(
        f"/{dataset}/City",
        json={
            "name": "City",
            "description": "Test city",
        },
    )
    assert resp.status_code == 201
    resp = app.get(f"/{dataset}/City")
    assert listdata(resp, full=True) == [
        {
            "name": "City",
            "description": "Test city",
        }
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_update_with_required_property(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    dataset = "type/required/update"
    context = bootstrap_manifest(
        rc,
        f"""
    d | r | b | m | property          | type            | ref
    {dataset}                         |                 |
      |   |   | City                  |                 |
      |   |   |   | name              | string required |
      |   |   |   | description       | string          |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authmodel(
        f"{dataset}/City",
        [
            "insert",
            "update",
            "getall",
        ],
    )

    resp = app.post(
        f"/{dataset}/City",
        json={
            "name": "City",
            "description": "Test city",
        },
    )
    city = resp.json()

    resp = app.put(
        f"/{dataset}/City/{city['_id']}",
        json={
            "_revision": city["_revision"],
            "description": "Test city updated",
        },
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["RequiredProperty"]
    assert get_error_context(resp.json(), "RequiredProperty", ["property", "model"]) == {
        "property": "name",
        "model": f"{dataset}/City",
    }
    resp = app.put(
        f"/{dataset}/City/{city['_id']}",
        json={
            "_revision": city["_revision"],
            "name": "City",
            "description": "Test city updated",
        },
    )
    assert resp.status_code == 200
    resp = app.get(f"/{dataset}/City")
    assert listdata(resp, full=True) == [
        {
            "name": "City",
            "description": "Test city updated",
        }
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_patch_with_required_property(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    dataset = "type/required/patch"
    context = bootstrap_manifest(
        rc,
        f"""
    d | r | b | m | property          | type            | ref
    {dataset}                         |                 |
      |   |   | City                  |                 |
      |   |   |   | name              | string required |
      |   |   |   | description       | string          |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authmodel(
        f"{dataset}/City",
        [
            "insert",
            "patch",
            "getall",
        ],
    )

    resp = app.post(
        f"/{dataset}/City",
        json={
            "name": "City",
            "description": "Test city",
        },
    )
    city = resp.json()

    resp = app.patch(
        f"/{dataset}/City/{city['_id']}",
        json={
            "_revision": city["_revision"],
            "name": None,
            "description": "Test city updated",
        },
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["RequiredProperty"]
    assert get_error_context(resp.json(), "RequiredProperty", ["property", "model"]) == {
        "property": "name",
        "model": f"{dataset}/City",
    }

    resp = app.patch(
        f"/{dataset}/City/{city['_id']}",
        json={
            "_revision": city["_revision"],
            "description": "Test city updated",
        },
    )
    assert resp.status_code == 200
    resp = app.get(f"/{dataset}/City")
    assert listdata(resp, full=True) == [
        {
            "name": "City",
            "description": "Test city updated",
        }
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_upsert_insert_with_required_property(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    dataset = "type/required/upsert"
    context = bootstrap_manifest(
        rc,
        f"""
    d | r | b | m | property          | type            | ref
    {dataset}                         |                 |
      |   |   | City                  |                 |
      |   |   |   | name              | string required |
      |   |   |   | description       | string          |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authmodel(f"{dataset}/City", ["insert", "upsert", "getall"])

    resp = app.post(
        f"/{dataset}/City",
        json={
            "_op": "upsert",
            "_where": f"_id='{str(uuid.uuid4())}'",
            "description": "Test city",
        },
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["RequiredProperty"]
    assert get_error_context(resp.json(), "RequiredProperty", ["property", "model"]) == {
        "property": "name",
        "model": f"{dataset}/City",
    }
    resp = app.post(
        f"/{dataset}/City",
        json={
            "_op": "upsert",
            "_where": f"_id='{str(uuid.uuid4())}'",
            "name": "City",
            "description": "Test city",
        },
    )
    assert resp.status_code == 201
    resp = app.get(f"/{dataset}/City")
    assert listdata(resp, full=True) == [
        {
            "name": "City",
            "description": "Test city",
        }
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_upsert_patch_with_required_property(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    dataset = "type/required/upsert/patch"
    context = bootstrap_manifest(
        rc,
        f"""
    d | r | b | m | property          | type            | ref
    {dataset}                         |                 |
      |   |   | City                  |                 |
      |   |   |   | name              | string required |
      |   |   |   | description       | string          |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authmodel(
        f"{dataset}/City",
        [
            "insert",
            "upsert",
            "getall",
        ],
    )

    resp = app.post(
        f"/{dataset}/City",
        json={
            "name": "City",
            "description": "Test city",
        },
    )
    city = resp.json()

    resp = app.post(
        f"/{dataset}/City",
        json={
            "_op": "upsert",
            "_where": f"_id='{city['_id']}'",
            "description": "Test city",
        },
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["RequiredProperty"]
    assert get_error_context(resp.json(), "RequiredProperty", ["property", "model"]) == {
        "property": "name",
        "model": f"{dataset}/City",
    }
    resp = app.post(
        f"/{dataset}/City",
        json={
            "_op": "upsert",
            "_where": f"_id='{city['_id']}'",
            "name": "City",
            "description": "Test city updated",
        },
    )
    assert resp.status_code == 200
    resp = app.get(f"/{dataset}/City")
    assert listdata(resp, full=True) == [
        {
            "name": "City",
            "description": "Test city updated",
        }
    ]
