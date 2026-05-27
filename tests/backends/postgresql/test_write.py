from pathlib import Path

import pytest
from _pytest.fixtures import FixtureRequest

from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.utils import get_error_codes, get_error_context


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_search", "spinta_set_meta_fields"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields"],
    ],
)
def test_insert_string_enum_values(
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
    d | r | b | m | property | type    | ref | source |prepare    | access | level
    datasets/insert/enums    |         |     |        |           |        |
      |   |   | City         |         | id  |        |           |        |
      |   |   |   | id       | integer |     |        |           | open   |
      |   |   |   | name     | string  |     |        |           | open   |
      |   |   |   |          | enum    |     | 1      | "TEST1"   | open   |
      |   |   |   |          |         |     |        | "TEST2"   | open   |
      |   |   |   |          |         |     | TEST3  |           | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)

    resp = app.post(
        "/datasets/insert/enums/City",
        json={"id": 0, "name": "1"},
    )
    assert resp.status_code == 201
    resp = app.post(
        "/datasets/insert/enums/City",
        json={"id": 1, "name": "TEST2"},
    )
    assert resp.status_code == 201
    resp = app.post(
        "/datasets/insert/enums/City",
        json={"id": 2, "name": "TEST3"},
    )
    assert resp.status_code == 201

    resp = app.get("/datasets/insert/enums/City")
    assert resp.status_code == 200
    assert listdata(resp, "id", "name", full=True) == [
        {"id": 0, "name": "TEST1"},
        {"id": 1, "name": "TEST2"},
        {"id": 2, "name": "TEST3"},
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_search", "spinta_set_meta_fields"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields"],
    ],
)
def test_insert_string_enum_invalid_values(
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
    d | r | b | m | property | type    | ref | source |prepare    | access | level
    datasets/insert/enums    |         |     |        |           |        |
      |   |   | City         |         | id  |        |           |        |
      |   |   |   | id       | integer |     |        |           | open   |
      |   |   |   | name     | string  |     |        |           | open   |
      |   |   |   |          | enum    |     | 1      | "TEST1"   | open   |
      |   |   |   |          |         |     |        | "TEST2"   | open   |
      |   |   |   |          |         |     | TEST3  |           | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)

    resp = app.post(
        "/datasets/insert/enums/City",
        json={"id": 0, "name": "TEST1"},
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["ValueNotInEnum"]
    assert get_error_context(resp.json(), "ValueNotInEnum", ["value"]) == {
        "value": "TEST1",
    }

    resp = app.post(
        "/datasets/insert/enums/City",
        json={"id": 1, "name": "2"},
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["ValueNotInEnum"]
    assert get_error_context(resp.json(), "ValueNotInEnum", ["value"]) == {
        "value": "2",
    }

    resp = app.get("/datasets/insert/enums/City")
    assert resp.status_code == 200
    assert listdata(resp, "id", "name", full=True) == []


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_search", "spinta_set_meta_fields"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields"],
    ],
)
def test_insert_integer_enum_values(
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
    d | r | b | m | property | type    | ref | source |prepare | access | level
    datasets/insert/enums    |         |     |        |        |        |
      |   |   | City         |         | id  |        |        |        |
      |   |   |   | id       | integer |     |        |        | open   |
      |   |   |   |          | enum    |     | 1      | 100    | open   |
      |   |   |   |          |         |     |        | 101    | open   |
      |   |   |   | name     | string  |     |        |        | open   |

    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)

    resp = app.post(
        "/datasets/insert/enums/City",
        json={"id": 1},
    )
    assert resp.status_code == 201
    resp = app.post(
        "/datasets/insert/enums/City",
        json={"id": 101},
    )
    assert resp.status_code == 201

    resp = app.get("/datasets/insert/enums/City")
    assert resp.status_code == 200
    assert listdata(resp, "id", "name", full=True) == [
        {"id": 100, "name": None},
        {"id": 101, "name": None},
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_search", "spinta_set_meta_fields"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:search", "uapi:/:set_meta_fields"],
    ],
)
def test_insert_integer_enum_invalid_values(
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
    d | r | b | m | property | type    | ref | source |prepare    | access | level
    datasets/insert/enums    |         |     |        |        |        |
      |   |   | City         |         | id  |        |        |        |
      |   |   |   | id       | integer |     |        |        | open   |
      |   |   |   |          | enum    |     | 1      | 100    | open   |
      |   |   |   |          |         |     |        | 101    | open   |
      |   |   |   | name     | string  |     |        |        | open   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)

    resp = app.post(
        "/datasets/insert/enums/City",
        json={"id": 100},
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["ValueNotInEnum"]
    assert get_error_context(resp.json(), "ValueNotInEnum", ["value"]) == {
        "value": 100,
    }

    resp = app.post(
        "/datasets/insert/enums/City",
        json={"id": 2},
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["ValueNotInEnum"]
    assert get_error_context(resp.json(), "ValueNotInEnum", ["value"]) == {
        "value": 2,
    }

    resp = app.get("/datasets/insert/enums/City")
    assert resp.status_code == 200
    assert listdata(resp, "id", "name", full=True) == []
