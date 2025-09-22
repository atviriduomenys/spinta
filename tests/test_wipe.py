from pathlib import Path
from typing import List
from typing import Tuple

import pytest
from _pytest.fixtures import FixtureRequest

from spinta.core.config import RawConfig
from spinta.testing.client import TestClient, create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.utils import get_error_codes
from spinta.utils.schema import NA


def _excluding(
    name: str,
    value: str,
    data: List[Tuple[str, str]],
) -> List[Tuple[str, str]]:
    return [(_type, status) for _type, status in data if not (_type == name and status == value)]


@pytest.mark.parametrize(
    "scope", [["spinta_insert", "spinta_getall", "spinta_wipe"], ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe"]]
)
def test_wipe_all(
    app,
    scope: list,
):
    app.authorize(scope)

    # Create some data in different models
    resp = app.post(
        "/",
        json={
            "_data": [
                {"_op": "insert", "_type": "Report", "status": "ok"},
                {"_op": "insert", "_type": "backends/mongo/Report", "status": "ok"},
                {"_op": "insert", "_type": "backends/postgres/Report", "status": "ok"},
            ]
        },
    )
    assert resp.status_code == 200, resp.json()

    # Get data from all models
    resp = app.get("/:all")
    assert listdata(resp, "_type", "status") == [
        ("Report", "ok"),
        ("_txn", NA),
        ("backends/mongo/Report", "ok"),
        ("backends/postgres/Report", "ok"),
    ]

    # Wipe all data
    resp = app.delete("/:wipe")
    assert resp.status_code == 200, resp.json()

    # Check what data again
    resp = app.get("/:all")
    assert resp.status_code == 200, resp.json()
    assert len(resp.json()["_data"]) == 0


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
@pytest.mark.parametrize(
    "scope", [["spinta_insert", "spinta_getall", "spinta_wipe"], ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe"]]
)
def test_wipe_model(model, app, scope):
    app.authorize(scope)

    # Create some data in different models
    resp = app.post(
        "/",
        json={
            "_data": [
                {"_op": "insert", "_type": "Report", "status": "ok"},
                {"_op": "insert", "_type": "backends/mongo/Report", "status": "ok"},
                {"_op": "insert", "_type": "backends/postgres/Report", "status": "ok"},
            ]
        },
    )
    assert resp.status_code == 200, resp.json()

    # Get data from all models
    resp = app.get("/:all")
    assert listdata(resp, "_type", "status") == [
        ("Report", "ok"),
        ("_txn", NA),
        ("backends/mongo/Report", "ok"),
        ("backends/postgres/Report", "ok"),
    ]

    # Wipe model data
    resp = app.delete(f"/{model}/:wipe")
    assert resp.status_code == 200, resp.json()
    resp = app.delete("/_txn/:wipe")
    assert resp.status_code == 200, resp.json()

    # Check the data again
    resp = app.get("/:all")
    assert listdata(resp, "_type", "status") == _excluding(
        model,
        "ok",
        [
            ("Report", "ok"),
            ("backends/mongo/Report", "ok"),
            ("backends/postgres/Report", "ok"),
        ],
    )


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
@pytest.mark.parametrize(
    "scope", [["spinta_insert", "spinta_getall", "spinta_wipe"], ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe"]]
)
def test_wipe_row(
    model: str,
    app: TestClient,
    scope: list,
):
    app.authorize(scope)

    # Create some data in different models
    resp = app.post(
        "/",
        json={
            "_data": [
                {"_op": "insert", "_type": "Report", "status": "ok"},
                {"_op": "insert", "_type": "backends/mongo/Report", "status": "ok"},
                {"_op": "insert", "_type": "backends/mongo/Report", "status": "nb"},
                {"_op": "insert", "_type": "backends/postgres/Report", "status": "ok"},
                {"_op": "insert", "_type": "backends/postgres/Report", "status": "nb"},
            ]
        },
    )
    _id_idx = {
        "backends/mongo/Report": 1,
        "backends/postgres/Report": 3,
    }
    _id = listdata(resp, "_id")[_id_idx[model]]

    # Get data from all models
    resp = app.get("/:all")
    assert listdata(resp, "_type", "status") == [
        ("Report", "ok"),
        ("_txn", NA),
        ("backends/mongo/Report", "nb"),
        ("backends/mongo/Report", "ok"),
        ("backends/postgres/Report", "nb"),
        ("backends/postgres/Report", "ok"),
    ]

    # Wipe model row data
    with pytest.raises(NotImplementedError):
        resp = app.delete(f"/{model}/{_id}/:wipe")
    assert resp.status_code == 200, resp.json()
    resp = app.delete("/_txn/:wipe")
    assert resp.status_code == 200, resp.json()

    # Check the data again.
    resp = app.get("/:all")
    assert listdata(resp, "_type", "status") == [
        ("Report", "ok"),
        ("backends/mongo/Report", "nb"),
        ("backends/mongo/Report", "ok"),
        ("backends/postgres/Report", "nb"),
        ("backends/postgres/Report", "ok"),
    ]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
@pytest.mark.parametrize(
    "scope", [["spinta_insert", "spinta_getall", "spinta_delete"], ["uapi:/:create", "uapi:/:getall", "uapi:/:delete"]]
)
def test_wipe_check_scope(
    model,
    app,
    scope: list,
):
    app.authorize(scope)
    resp = app.delete(f"/{model}/:wipe")
    assert resp.status_code == 403


@pytest.mark.parametrize(
    "scope", [["spinta_insert", "spinta_getall", "spinta_delete"], ["uapi:/:create", "uapi:/:getall", "uapi:/:delete"]]
)
def test_wipe_check_ns_scope(
    app,
    scope: list,
):
    app.authorize(scope)
    resp = app.delete("/:wipe")
    assert resp.status_code == 403


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
@pytest.mark.parametrize("scope", [["spinta_wipe"], ["uapi:/:wipe"]])
def test_wipe_in_batch(
    model,
    app,
    scope: list,
):
    app.authorize(scope)
    resp = app.post("/", json={"_data": [{"_op": "wipe", "_type": model}]})
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["UnknownAction"]


@pytest.mark.parametrize(
    "scope", [["spinta_insert", "spinta_getall", "spinta_delete"], ["uapi:/:create", "uapi:/:getall", "uapi:/:delete"]]
)
def test_wipe_all_access(
    app: TestClient,
    scope: list,
):
    app.authorize(scope)

    # Create some data in different models.
    resp = app.post(
        "/",
        json={
            "_data": [
                {"_op": "insert", "_type": "Report", "status": "ok"},
                {"_op": "insert", "_type": "backends/mongo/Report", "status": "ok"},
                {"_op": "insert", "_type": "backends/postgres/Report", "status": "ok"},
            ]
        },
    )
    assert resp.status_code == 200, resp.json()

    # Get data from all models.
    resp = app.get("/:all")
    assert listdata(resp, "_type", "status") == [
        ("Report", "ok"),
        ("_txn", NA),
        ("backends/mongo/Report", "ok"),
        ("backends/postgres/Report", "ok"),
    ]

    # Wipe all data
    resp = app.delete("/:wipe")
    assert resp.status_code == 403  # Forbidden

    # Check the data again.
    resp = app.get("/:all")
    assert listdata(resp, "_type", "status") == [
        ("Report", "ok"),
        ("_txn", NA),
        ("backends/mongo/Report", "ok"),
        ("backends/postgres/Report", "ok"),
    ]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
@pytest.mark.parametrize(
    "scope", [["spinta_insert", "spinta_getall", "spinta_delete"], ["uapi:/:create", "uapi:/:getall", "uapi:/:delete"]]
)
def test_wipe_model_access(
    model,
    app,
    scope: list,
):
    app.authorize(scope)

    # Create some data in different models
    resp = app.post(
        "/",
        json={
            "_data": [
                {"_op": "insert", "_type": "Report", "status": "ok"},
                {"_op": "insert", "_type": "backends/mongo/Report", "status": "ok"},
                {"_op": "insert", "_type": "backends/postgres/Report", "status": "ok"},
            ]
        },
    )
    assert resp.status_code == 200, resp.json()

    # Get data from all models
    resp = app.get("/:all")
    assert listdata(resp, "_type", "status") == [
        ("Report", "ok"),
        ("_txn", NA),
        ("backends/mongo/Report", "ok"),
        ("backends/postgres/Report", "ok"),
    ]

    # Wipe model data
    resp = app.delete(f"/{model}/:wipe")
    assert resp.status_code == 403, resp.json()

    # Check what data again
    resp = app.get("/:all")
    assert listdata(resp, "_type", "status") == [
        ("Report", "ok"),
        ("_txn", NA),
        ("backends/mongo/Report", "ok"),
        ("backends/postgres/Report", "ok"),
    ]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
@pytest.mark.parametrize(
    "scope", [["spinta_insert", "spinta_getall", "spinta_delete"], ["uapi:/:create", "uapi:/:getall", "uapi:/:delete"]]
)
def test_wipe_row_access(
    model,
    app,
    scope: list,
):
    app.authorize(scope)

    # Create some data in different models
    resp = app.post(
        "/",
        json={
            "_data": [
                {"_op": "insert", "_type": "Report", "status": "ok"},
                {"_op": "insert", "_type": "backends/mongo/Report", "status": "ok"},
                {"_op": "insert", "_type": "backends/postgres/Report", "status": "ok"},
            ]
        },
    )
    ids = dict(listdata(resp, "_type", "_id"))
    _id = ids[model]

    # Get data from all models
    resp = app.get("/:all")
    assert listdata(resp, "_type", "status") == [
        ("Report", "ok"),
        ("_txn", NA),
        ("backends/mongo/Report", "ok"),
        ("backends/postgres/Report", "ok"),
    ]

    # Wipe model row data
    with pytest.raises(NotImplementedError):
        resp = app.delete(f"/{model}/{_id}/:wipe")
        assert resp.status_code == 403, resp.json()

    # Check what data again
    resp = app.get("/:all")
    assert listdata(resp, "_type", "status") == [
        ("Report", "ok"),
        ("_txn", NA),
        ("backends/mongo/Report", "ok"),
        ("backends/postgres/Report", "ok"),
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope", [["spinta_insert", "spinta_getall", "spinta_wipe"], ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe"]]
)
def test_wipe_with_long_names(
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
    d | r | b | m | property                 | type     | ref
    backends/postgres/very/long/name/models  |          |
      |   |   | ModelWithVeryVeryVeryLongName|          |
      |   |   |   | status                   | string   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        full_load=True,
        request=request,
    )
    with context:
        app = create_test_client(context)
        app.authorize(scope)

        # Create some data
        resp = app.post(
            "/",
            json={
                "_data": [
                    {
                        "_op": "insert",
                        "_type": "backends/postgres/very/long/name/models/ModelWithVeryVeryVeryLongName",
                        "status": "ok",
                    },
                ]
            },
        )
        assert resp.status_code == 200, resp.json()

        # Get data from all models
        resp = app.get("/:all")
        assert listdata(resp, "_type", "status") == [
            ("_txn", NA),
            ("backends/postgres/very/long/name/models/ModelWithVeryVeryVeryLongName", "ok"),
        ]

        # Wipe all data
        resp = app.delete("/:wipe")
        assert resp.status_code == 200, resp.json()

        # Check what data again
        resp = app.get("/:all")
        assert resp.status_code == 200, resp.json()
        assert len(resp.json()["_data"]) == 0
