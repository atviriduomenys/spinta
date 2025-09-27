from pathlib import Path
import sqlalchemy as sa
import uuid
import json

from pytest import FixtureRequest
import pytest

from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client, create_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.manifest import prepare_manifest
from spinta.testing.datasets import create_sqlite_db
from spinta.testing.datasets import Sqlite
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.utils import get_error_codes
from spinta.manifests.tabular.helpers import striptable
from spinta.core.enums import Mode


@pytest.fixture(scope="module")
def uuid_db():
    with create_sqlite_db(
        {
            "test_uuid": [
                sa.Column("id", sa.String, primary_key=True, default=int),
                sa.Column("guid", sa.String, default=lambda: str(uuid.uuid4())),
            ],
        }
    ) as db:
        db.write(
            "test_uuid",
            [
                {"id": 1, "guid": "5394173a-7750-4dab-81ba-95c807e04f72"},
                {"id": 2, "guid": "9c6aa93a-352b-4d36-a694-356aa99dfab2"},
                {"id": 3, "guid": "6314ae6d-ac99-4fba-a121-c692ecac19d8"},
            ],
        )
        yield db


@pytest.mark.manifests("internal_sql", "csv")
def test_insert(manifest_type: str, tmp_path: Path, rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property      | type
        backends/postgres/dtypes/uuid |
          |   |   | Entity            |
          |   |   |   | id            | uuid
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("backends/postgres/dtypes/uuid/Entity", ["insert", "getall", "search"])
    entity_id = str(uuid.uuid4())
    resp = app.post("/backends/postgres/dtypes/uuid/Entity", json={"id": entity_id})
    assert resp.status_code == 201


@pytest.mark.manifests("internal_sql", "csv")
def test_read_data(manifest_type: str, tmp_path: Path, rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property      | type
        backends/postgres/dtypes/uuid |
          |   |   | Entity            |
          |   |   |   | id            | uuid
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("backends/postgres/dtypes/uuid/Entity", ["insert", "getall", "search"])
    entity_id = str(uuid.uuid4())
    app.post("/backends/postgres/dtypes/uuid/Entity", json={"id": entity_id})
    resp = app.get("/backends/postgres/dtypes/uuid/Entity?select(id)")
    assert listdata(resp, full=True) == [{"id": entity_id}]


@pytest.mark.manifests("internal_sql", "csv")
def test_filter_eq(manifest_type: str, tmp_path: Path, rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property      | type
        backends/postgres/dtypes/uuid |
          |   |   | Entity            |
          |   |   |   | id            | uuid
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("backends/postgres/dtypes/uuid/Entity", ["insert", "getall", "search"])
    entity_ids = [str(uuid.uuid4()) for _ in range(3)]
    app.post(
        "/backends/postgres/dtypes/uuid/Entity",
        json={
            "_data": [
                {"_op": "insert", "id": entity_ids[0]},
                {"_op": "insert", "id": entity_ids[1]},
                {"_op": "insert", "id": entity_ids[2]},
            ]
        },
    )
    resp = app.get(f'/backends/postgres/dtypes/uuid/Entity?id="{entity_ids[0]}"')
    assert resp.status_code == 200, f"Failed: {resp.url}"
    assert listdata(resp, "id") == [(entity_ids[0])]


@pytest.mark.manifests("internal_sql", "csv")
def test_filter_ne(manifest_type: str, tmp_path: Path, rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property      | type
        backends/postgres/dtypes/uuid |
          |   |   | Entity            |
          |   |   |   | id            | uuid
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("backends/postgres/dtypes/uuid/Entity", ["insert", "getall", "search"])
    entity_ids = [str(uuid.uuid4()) for _ in range(3)]
    app.post(
        "/backends/postgres/dtypes/uuid/Entity",
        json={
            "_data": [
                {"_op": "insert", "id": entity_ids[0]},
                {"_op": "insert", "id": entity_ids[1]},
                {"_op": "insert", "id": entity_ids[2]},
            ]
        },
    )
    resp = app.get(f'/backends/postgres/dtypes/uuid/Entity?id!="{entity_ids[0]}"')
    assert resp.status_code == 200, f"Failed: {resp.url}"
    assert sorted(listdata(resp, "id")) == sorted([(entity_ids[1]), (entity_ids[2])])


@pytest.mark.manifests("internal_sql", "csv")
def test_filter_lt(manifest_type: str, tmp_path: Path, rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property      | type
        backends/postgres/dtypes/uuid |
          |   |   | Entity            |
          |   |   |   | id            | uuid
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("backends/postgres/dtypes/uuid/Entity", ["insert", "getall", "search"])
    app.post(
        "/backends/postgres/dtypes/uuid/Entity",
        json={
            "_data": [
                {"_op": "insert", "id": "d1917f5c-7671-443a-8bdd-55ec0f233856"},
                {"_op": "insert", "id": "1ecfcfb2-7dea-464d-a44c-74193130d15d"},
                {"_op": "insert", "id": "1ad35c76-5bb5-49bf-84c6-cd588e8ad2a8"},
                {"_op": "insert", "id": "2e80bf28-aed6-4db4-9e5f-a60e84a5fd20"},
            ]
        },
    )
    resp = app.get('/backends/postgres/dtypes/uuid/Entity?id<"2e80bf28-aed6-4db4-9e5f-a60e84a5fd20"')
    assert resp.status_code == 200, f"Failed: {resp.url}"
    assert sorted(listdata(resp, "id")) == sorted(
        ["1ecfcfb2-7dea-464d-a44c-74193130d15d", "1ad35c76-5bb5-49bf-84c6-cd588e8ad2a8"]
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_filter_contains(manifest_type: str, tmp_path: Path, rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property      | type
        backends/postgres/dtypes/uuid |
          |   |   | Entity            |
          |   |   |   | id            | uuid
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("backends/postgres/dtypes/uuid/Entity", ["insert", "getall", "search"])
    app.post(
        "/backends/postgres/dtypes/uuid/Entity",
        json={
            "_data": [
                {"_op": "insert", "id": "d1917f5c-7671-443a-8bdd-55ec0f233856"},
                {"_op": "insert", "id": "1ecfcfb2-7dea-464d-a44c-74193130d15d"},
            ]
        },
    )
    resp = app.get('/backends/postgres/dtypes/uuid/Entity?id.contains("7671")')
    assert resp.status_code == 200, f"Failed: {resp.url}"
    assert listdata(resp, "id") == ["d1917f5c-7671-443a-8bdd-55ec0f233856"]


@pytest.mark.manifests("internal_sql", "csv")
def test_filter_sort(manifest_type: str, tmp_path: Path, rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property      | type
        backends/postgres/dtypes/uuid |
          |   |   | Entity            |
          |   |   |   | id            | uuid
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("backends/postgres/dtypes/uuid/Entity", ["insert", "getall", "search"])
    entity_ids = [str(uuid.uuid4()) for _ in range(3)]
    app.post(
        "/backends/postgres/dtypes/uuid/Entity",
        json={
            "_data": [
                {"_op": "insert", "id": entity_ids[0]},
                {"_op": "insert", "id": entity_ids[1]},
                {"_op": "insert", "id": entity_ids[2]},
            ]
        },
    )
    resp = app.get("/backends/postgres/dtypes/uuid/Entity?sort(id)")
    assert resp.status_code == 200, f"Failed: {resp.url}"
    assert listdata(resp, "id") == sorted([(entity_ids[0]), (entity_ids[1]), (entity_ids[2])])


@pytest.mark.manifests("internal_sql", "csv")
def test_format_csv(manifest_type: str, tmp_path: Path, rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property      | type
        backends/postgres/dtypes/uuid |
          |   |   | Entity            |
          |   |   |   | id            | uuid
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("backends/postgres/dtypes/uuid/Entity", ["insert", "getall", "search"])
    entity_id = str(uuid.uuid4())
    app.post("/backends/postgres/dtypes/uuid/Entity", json={"id": entity_id})

    resp = app.get("/backends/postgres/dtypes/uuid/Entity/:format/csv?select(id)")
    assert resp.status_code == 200, f"CSV format failed: {resp.text}"
    assert resp.text == f"id\r\n{entity_id}\r\n"


@pytest.mark.manifests("internal_sql", "csv")
def test_format_json(manifest_type: str, tmp_path: Path, rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property      | type
        backends/postgres/dtypes/uuid |
          |   |   | Entity            |
          |   |   |   | id            | uuid
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("backends/postgres/dtypes/uuid/Entity", ["insert", "getall", "search"])
    entity_id = str(uuid.uuid4())
    app.post("/backends/postgres/dtypes/uuid/Entity", json={"id": entity_id})

    resp = app.get("/backends/postgres/dtypes/uuid/Entity/:format/json?select(id)")
    assert resp.status_code == 200, f"JSON format failed: {resp.text}"
    assert resp.text == f'{{"_data":[{{"id":"{entity_id}"}}]}}'


@pytest.mark.manifests("internal_sql", "csv")
def test_format_jsonl(manifest_type: str, tmp_path: Path, rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property      | type
        backends/postgres/dtypes/uuid |
          |   |   | Entity            |
          |   |   |   | id            | uuid
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("backends/postgres/dtypes/uuid/Entity", ["insert", "getall", "search"])
    entity_id = str(uuid.uuid4())
    app.post("/backends/postgres/dtypes/uuid/Entity", json={"id": entity_id})

    resp = app.get("/backends/postgres/dtypes/uuid/Entity/:format/jsonl?select(id)")
    assert resp.status_code == 200, f"JSONL format failed: {resp.text}"
    assert resp.text == f'{{"id":"{entity_id}"}}\n'


@pytest.mark.manifests("internal_sql", "csv")
def test_format_ascii(manifest_type: str, tmp_path: Path, rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property      | type
        backends/postgres/dtypes/uuid |
          |   |   | Entity            |
          |   |   |   | id            | uuid
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("backends/postgres/dtypes/uuid/Entity", ["insert", "getall", "search"])
    entity_id = str(uuid.uuid4())
    app.post("/backends/postgres/dtypes/uuid/Entity", json={"id": entity_id})

    resp = app.get("/backends/postgres/dtypes/uuid/Entity/:format/ascii?select(id)")
    assert resp.status_code == 200, f"ASCII format failed: {resp.text}"
    assert (
        resp.text == "------------------------------------\n"
        "id                                  \n"
        f"{entity_id}\n"
        "------------------------------------\n"
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_format_rdf(manifest_type: str, tmp_path: Path, rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property      | type
        backends/postgres/dtypes/uuid |
          |   |   | Entity            |
          |   |   |   | id            | uuid
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("backends/postgres/dtypes/uuid/Entity", ["insert", "getall", "search"])
    entity_id = str(uuid.uuid4())
    app.post("/backends/postgres/dtypes/uuid/Entity", json={"id": entity_id})

    resp = app.get("/backends/postgres/dtypes/uuid/Entity/:format/rdf?select(id)")
    assert resp.status_code == 200, f"RDF format failed: {resp.text}"
    assert (
        resp.text == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns="https://testserver/">\n'
        f'<rdf:Description rdf:type="backends/postgres/dtypes/uuid/Entity">\n '
        f" <id>{entity_id}</id>\n"
        f"</rdf:Description>\n"
        f"</rdf:RDF>\n"
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_invalid_uuid(manifest_type: str, tmp_path: Path, rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property      | type
        backends/postgres/dtypes/uuid |
          |   |   | Entity            |
          |   |   |   | id            | uuid
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("backends/postgres/dtypes/uuid/Entity", ["insert", "getall", "search"])
    invalid_uuid = "invalid-uuid"
    resp = app.post("/backends/postgres/dtypes/uuid/Entity", json={"id": invalid_uuid})
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]


def test_uuid_sql(context, rc: RawConfig, tmp_path: Path, uuid_db: Sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
id | d | r | b | m | property  | type    | ref | source    | prepare | level | access | uri | title | description
   | datasets/uuid/example     |         |     |           |         |       |        |     |       |
   |   | data                  | sql     |     |           |         |       |        |     |       |
   |                           |         |     |           |         |       |        |     |       |
   |   |   |   | TestUUID      |         | id  | test_uuid |         |       | open   |     |       |
   |   |   |   |   | id        | integer |     | id        |         |       |        |     |       |
   |   |   |   |   | guid      | uuid    |     | guid      |         |       |        |     |       |
    """),
    )

    app = create_client(rc, tmp_path, uuid_db)

    resp = app.get("/datasets/uuid/example/TestUUID")
    assert resp.status_code == 200
    assert listdata(resp, "id", "guid") == [
        (1, "5394173a-7750-4dab-81ba-95c807e04f72"),
        (2, "9c6aa93a-352b-4d36-a694-356aa99dfab2"),
        (3, "6314ae6d-ac99-4fba-a121-c692ecac19d8"),
    ]


def test_uuid_sql_select(context, rc: RawConfig, tmp_path: Path, uuid_db: Sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
id | d | r | b | m | property  | type    | ref | source    | prepare | level | access | uri | title | description
   | datasets/uuid/example     |         |     |           |         |       |        |     |       |
   |   | data                  | sql     |     |           |         |       |        |     |       |
   |                           |         |     |           |         |       |        |     |       |
   |   |   |   | TestUUID      |         | id  | test_uuid |         |       | open   |     |       |
   |   |   |   |   | id        | integer |     | id        |         |       |        |     |       |
   |   |   |   |   | guid      | uuid    |     | guid      |         |       |        |     |       |
    """),
    )

    app = create_client(rc, tmp_path, uuid_db)

    resp = app.get("/datasets/uuid/example/TestUUID?select(guid)")
    assert resp.status_code == 200
    assert len(resp.json()["_data"]) == 3


def test_uuid_sql_filter_eq(context, rc: RawConfig, tmp_path: Path, uuid_db: Sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
id | d | r | b | m | property  | type    | ref | source    | prepare | level | access | uri | title | description
   | datasets/uuid/example     |         |     |           |         |       |        |     |       |
   |   | data                  | sql     |     |           |         |       |        |     |       |
   |                           |         |     |           |         |       |        |     |       |
   |   |   |   | TestUUID      |         | id  | test_uuid |         |       | open   |     |       |
   |   |   |   |   | id        | integer |     | id        |         |       |        |     |       |
   |   |   |   |   | guid      | uuid    |     | guid      |         |       |        |     |       |
    """),
    )

    app = create_client(rc, tmp_path, uuid_db)
    resp = app.get('datasets/uuid/example/TestUUID?guid="5394173a-7750-4dab-81ba-95c807e04f72"')
    assert resp.status_code == 200
    assert listdata(resp, "id", "guid") == [(1, "5394173a-7750-4dab-81ba-95c807e04f72")]


def test_uuid_sql_filter_ne(context, rc: RawConfig, tmp_path: Path, uuid_db: Sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
id | d | r | b | m | property  | type    | ref | source    | prepare | level | access | uri | title | description
   | datasets/uuid/example     |         |     |           |         |       |        |     |       |
   |   | data                  | sql     |     |           |         |       |        |     |       |
   |                           |         |     |           |         |       |        |     |       |
   |   |   |   | TestUUID      |         | id  | test_uuid |         |       | open   |     |       |
   |   |   |   |   | id        | integer |     | id        |         |       |        |     |       |
   |   |   |   |   | guid      | uuid    |     | guid      |         |       |        |     |       |
    """),
    )

    app = create_client(rc, tmp_path, uuid_db)
    resp = app.get('datasets/uuid/example/TestUUID?guid!="5394173a-7750-4dab-81ba-95c807e04f72"')
    assert resp.status_code == 200
    assert sorted(listdata(resp, "id", "guid")) == sorted(
        [(2, "9c6aa93a-352b-4d36-a694-356aa99dfab2"), (3, "6314ae6d-ac99-4fba-a121-c692ecac19d8")]
    )


def test_uuid_sql_filter_lt(context, rc: RawConfig, tmp_path: Path, uuid_db: Sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
id | d | r | b | m | property  | type    | ref | source    | prepare | level | access | uri | title | description
   | datasets/uuid/example     |         |     |           |         |       |        |     |       |
   |   | data                  | sql     |     |           |         |       |        |     |       |
   |                           |         |     |           |         |       |        |     |       |
   |   |   |   | TestUUID      |         | id  | test_uuid |         |       | open   |     |       |
   |   |   |   |   | id        | integer |     | id        |         |       |        |     |       |
   |   |   |   |   | guid      | uuid    |     | guid      |         |       |        |     |       |
    """),
    )

    app = create_client(rc, tmp_path, uuid_db)
    resp = app.get('datasets/uuid/example/TestUUID?guid<"6314ae6d-ac99-4fba-a121-c692ecac19d8"')
    assert resp.status_code == 200
    assert listdata(resp, "id", "guid") == [(1, "5394173a-7750-4dab-81ba-95c807e04f72")]


def test_uuid_sql_filter_contains(context, rc: RawConfig, tmp_path: Path, uuid_db: Sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
id | d | r | b | m | property  | type    | ref | source    | prepare | level | access | uri | title | description
   | datasets/uuid/example     |         |     |           |         |       |        |     |       |
   |   | data                  | sql     |     |           |         |       |        |     |       |
   |                           |         |     |           |         |       |        |     |       |
   |   |   |   | TestUUID      |         | id  | test_uuid |         |       | open   |     |       |
   |   |   |   |   | id        | integer |     | id        |         |       |        |     |       |
   |   |   |   |   | guid      | uuid    |     | guid      |         |       |        |     |       |
    """),
    )

    app = create_client(rc, tmp_path, uuid_db)
    resp = app.get('datasets/uuid/example/TestUUID?guid.contains("81ba")')
    assert resp.status_code == 200
    assert listdata(resp, "id", "guid") == [(1, "5394173a-7750-4dab-81ba-95c807e04f72")]


def test_uuid_sql_filter_sort(context, rc: RawConfig, tmp_path: Path, uuid_db: Sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
id | d | r | b | m | property  | type    | ref | source    | prepare | level | access | uri | title | description
   | datasets/uuid/example     |         |     |           |         |       |        |     |       |
   |   | data                  | sql     |     |           |         |       |        |     |       |
   |                           |         |     |           |         |       |        |     |       |
   |   |   |   | TestUUID      |         | id  | test_uuid |         |       | open   |     |       |
   |   |   |   |   | id        | integer |     | id        |         |       |        |     |       |
   |   |   |   |   | guid      | uuid    |     | guid      |         |       |        |     |       |
    """),
    )

    app = create_client(rc, tmp_path, uuid_db)
    resp = app.get("datasets/uuid/example/TestUUID?sort(guid)")
    assert resp.status_code == 200
    assert listdata(resp, "id", "guid") == sorted(
        [
            (1, "5394173a-7750-4dab-81ba-95c807e04f72"),
            (3, "6314ae6d-ac99-4fba-a121-c692ecac19d8"),
            (2, "9c6aa93a-352b-4d36-a694-356aa99dfab2"),
        ]
    )


def test_external_json(tmp_path: Path, rc: RawConfig):
    test_data = {
        "test_uuid": [
            {"id": 1, "guid": "5394173a-7750-4dab-81ba-95c807e04f72"},
            {"id": 2, "guid": "9c6aa93a-352b-4d36-a694-356aa99dfab2"},
        ]
    }
    json_file = tmp_path / "TestUUID.json"
    with open(json_file, "w") as file:
        json.dump(test_data, file)

    context, manifest = prepare_manifest(
        rc,
        f"""
id | d | r | b | m | property  | type      | ref | source      | prepare | level | access | uri | title | description
   | datasets/uuid/example     |           |     |             |         |       |        |     |       |
   |   | data                  | dask/json |     | {json_file} |         |       |        |     |       |
   |                           |           |     |             |         |       |        |     |       |
   |   |   |   | TestUUID      |           | id  | test_uuid   |         |       | open   |     |       |
   |   |   |   |   | id        | integer   |     | id          |         |       |        |     |       |
   |   |   |   |   | guid      | uuid      |     | guid        |         |       |        |     |       |
        """,
        mode=Mode.external,
    )
    context.loaded = True

    app = create_test_client(context)
    app.authmodel("datasets/uuid/example/TestUUID", ["insert", "getall", "search"])

    resp = app.get("datasets/uuid/example/TestUUID")
    assert resp.status_code == 200
    assert listdata(resp, "id", "guid") == [
        (1, "5394173a-7750-4dab-81ba-95c807e04f72"),
        (2, "9c6aa93a-352b-4d36-a694-356aa99dfab2"),
    ]


def test_external_json_select(tmp_path: Path, rc: RawConfig):
    test_data = {
        "test_uuid": [
            {"id": 1, "guid": "5394173a-7750-4dab-81ba-95c807e04f72"},
            {"id": 2, "guid": "9c6aa93a-352b-4d36-a694-356aa99dfab2"},
        ]
    }
    json_file = tmp_path / "TestUUID.json"
    with open(json_file, "w") as file:
        json.dump(test_data, file)

    context, manifest = prepare_manifest(
        rc,
        f"""
id | d | r | b | m | property  | type      | ref | source      | prepare | level | access | uri | title | description
   | datasets/uuid/example     |           |     |             |         |       |        |     |       |
   |   | data                  | dask/json |     | {json_file} |         |       |        |     |       |
   |                           |           |     |             |         |       |        |     |       |
   |   |   |   | TestUUID      |           | id  | test_uuid   |         |       | open   |     |       |
   |   |   |   |   | id        | integer   |     | id          |         |       |        |     |       |
   |   |   |   |   | guid      | uuid      |     | guid        |         |       |        |     |       |
        """,
        mode=Mode.external,
    )
    context.loaded = True

    app = create_test_client(context)
    app.authmodel("datasets/uuid/example/TestUUID", ["insert", "getall", "search"])

    resp = app.get("datasets/uuid/example/TestUUID?select(guid)")
    assert resp.status_code == 200
    assert len(resp.json()["_data"]) == 2


def test_external_csv(tmp_path: Path, rc: RawConfig):
    test_data = """id,guid,,,
1,5394173a-7750-4dab-81ba-95c807e04f72,,,
2,9c6aa93a-352b-4d36-a694-356aa99dfab2,,,"""
    csv_file = tmp_path / "TestUUID.csv"
    with open(csv_file, "w") as file:
        file.write(test_data)

    context, manifest = prepare_manifest(
        rc,
        f"""
id | d | r | b | m | property  | type     | ref | source      | prepare | level | access | uri | title | description
   | datasets/uuid/example     |          |     |             |         |       |        |     |       |
   |   | data                  | dask/csv |     | {csv_file}  |         |       |        |     |       |
   |                           |          |     |             |         |       |        |     |       |
   |   |   |   | TestUUID      |          | id  | test_uuid   |         |       | open   |     |       |
   |   |   |   |   | id        | integer  |     | id          |         |       |        |     |       |
   |   |   |   |   | guid      | uuid     |     | guid        |         |       |        |     |       |
    """,
        mode=Mode.external,
    )
    context.loaded = True

    app = create_test_client(context)
    app.authmodel("datasets/uuid/example/TestUUID", ["insert", "getall", "search"])

    resp = app.get("datasets/uuid/example/TestUUID")
    assert resp.status_code == 200
    assert listdata(resp, "id", "guid") == [
        (1, "5394173a-7750-4dab-81ba-95c807e04f72"),
        (2, "9c6aa93a-352b-4d36-a694-356aa99dfab2"),
    ]


def test_external_csv_select(tmp_path: Path, rc: RawConfig):
    test_data = f"""id,guid,,,
1,{str(uuid.uuid4())},,,
2,{str(uuid.uuid4())},,,"""
    csv_file = tmp_path / "TestUUID.csv"
    with open(csv_file, "w") as file:
        file.write(test_data)

    context, manifest = prepare_manifest(
        rc,
        f"""
id | d | r | b | m | property  | type     | ref | source      | prepare | level | access | uri | title | description
   | datasets/uuid/example     |          |     |             |         |       |        |     |       |
   |   | data                  | dask/csv |     | {csv_file}  |         |       |        |     |       |
   |                           |          |     |             |         |       |        |     |       |
   |   |   |   | TestUUID      |          | id  | test_uuid   |         |       | open   |     |       |
   |   |   |   |   | id        | integer  |     | id          |         |       |        |     |       |
   |   |   |   |   | guid      | uuid     |     | guid        |         |       |        |     |       |
    """,
        mode=Mode.external,
    )
    context.loaded = True

    app = create_test_client(context)
    app.authmodel("datasets/uuid/example/TestUUID", ["insert", "getall", "search"])

    resp = app.get("datasets/uuid/example/TestUUID?select(guid)")
    assert resp.status_code == 200
    assert len(resp.json()["_data"]) == 2


def test_external_xml(tmp_path: Path, rc: RawConfig):
    test_data = """
<items>
    <test_uuid>
        <id>1</id>
        <guid>5394173a-7750-4dab-81ba-95c807e04f72</guid>
    </test_uuid>
    <test_uuid>
        <id>2</id>
        <guid>9c6aa93a-352b-4d36-a694-356aa99dfab2</guid>
    </test_uuid>
</items>
"""
    xml_file = tmp_path / "TestUUID.xml"
    with open(xml_file, "w") as file:
        file.write(test_data)

    context, manifest = prepare_manifest(
        rc,
        f"""
id | d | r | b | m | property  | type     | ref | source            | prepare | level | access | uri | title | description
   | datasets/uuid/example     |          |     |                   |         |       |        |     |       |
   |   | data                  | dask/xml |     | {xml_file}        |         |       |        |     |       |
   |                           |          |     |                   |         |       |        |     |       |
   |   |   |   | TestUUID      |          | id  | /items/test_uuid  |         |       | open   |     |       |
   |   |   |   |   | id        | integer  |     | id                |         |       |        |     |       |
   |   |   |   |   | guid      | uuid     |     | guid              |         |       |        |     |       |
    """,
        mode=Mode.external,
    )
    context.loaded = True

    app = create_test_client(context)
    app.authmodel("datasets/uuid/example/TestUUID", ["insert", "getall", "search"])

    resp = app.get("datasets/uuid/example/TestUUID")
    assert resp.status_code == 200
    assert listdata(resp, "id", "guid") == [
        (1, "5394173a-7750-4dab-81ba-95c807e04f72"),
        (2, "9c6aa93a-352b-4d36-a694-356aa99dfab2"),
    ]


def test_external_xml_select(tmp_path: Path, rc: RawConfig):
    test_data = f"""
<items>
    <test_uuid>
        <id>1</id>
        <guid>{str(uuid.uuid4())}</guid>
    </test_uuid>
    <test_uuid>
        <id>2</id>
        <guid>{str(uuid.uuid4())}</guid>
    </test_uuid>
</items>
"""
    xml_file = tmp_path / "TestUUID.xml"
    with open(xml_file, "w") as file:
        file.write(test_data)

    context, manifest = prepare_manifest(
        rc,
        f"""
id | d | r | b | m | property  | type     | ref | source            | prepare | level | access | uri | title | description
   | datasets/uuid/example     |          |     |                   |         |       |        |     |       |
   |   | data                  | dask/xml |     | {xml_file}        |         |       |        |     |       |
   |                           |          |     |                   |         |       |        |     |       |
   |   |   |   | TestUUID      |          | id  | /items/test_uuid  |         |       | open   |     |       |
   |   |   |   |   | id        | integer  |     | id                |         |       |        |     |       |
   |   |   |   |   | guid      | uuid     |     | guid              |         |       |        |     |       |
    """,
        mode=Mode.external,
    )
    context.loaded = True

    app = create_test_client(context)
    app.authmodel("datasets/uuid/example/TestUUID", ["insert", "getall", "search"])

    resp = app.get("datasets/uuid/example/TestUUID?select(guid)")
    assert resp.status_code == 200
    assert len(resp.json()["_data"]) == 2
