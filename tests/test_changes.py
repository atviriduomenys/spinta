import pytest
from _pytest.fixtures import FixtureRequest
import sqlalchemy as sa
from pathlib import Path

from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import send, listdata
from spinta.testing.manifest import bootstrap_manifest


@pytest.mark.models(
    "backends/postgres/Report",
    # 'backends/mongo/Report',
)
def test_changes(model, context, app):
    app.authmodel(model, ["insert", "patch", "changes"])
    obj = send(app, model, "insert", {"status": "1"})
    obj = send(app, model, "patch", obj, {"status": "2"})
    obj = send(app, model, "patch", obj, {"status": "3"})
    obj = send(app, model, "patch", obj, {"status": "4"})

    assert send(app, model, "changes", select=["_cid", "_op", "status"]) == [
        {"_cid": 1, "_op": "insert", "status": "1"},
        {"_cid": 2, "_op": "patch", "status": "2"},
        {"_cid": 3, "_op": "patch", "status": "3"},
        {"_cid": 4, "_op": "patch", "status": "4"},
    ]

    assert send(app, model, ":changes?limit(1)", select=["_cid", "_op", "status"]) == [
        {"_cid": 1, "_op": "insert", "status": "1"},
    ]

    assert send(app, model, ":changes/2?limit(2)", select=["_cid", "_op", "status"]) == [
        {"_cid": 2, "_op": "patch", "status": "2"},
        {"_cid": 3, "_op": "patch", "status": "3"},
    ]

    assert send(app, model, ":changes/3?limit(2)", select=["_cid", "_op", "status"]) == [
        {"_cid": 3, "_op": "patch", "status": "3"},
        {"_cid": 4, "_op": "patch", "status": "4"},
    ]


@pytest.mark.models(
    "backends/postgres/Report",
    # 'backends/mongo/Report',
)
def test_changes_negative_offset(model, context, app):
    app.authmodel(model, ["insert", "patch", "changes"])
    obj = send(app, model, "insert", {"status": "1"})
    obj = send(app, model, "patch", obj, {"status": "2"})
    obj = send(app, model, "patch", obj, {"status": "3"})
    obj = send(app, model, "patch", obj, {"status": "4"})

    assert send(app, model, ":changes/-1?limit(1)", select=["_cid", "_op", "status"]) == [
        {"_cid": 4, "_op": "patch", "status": "4"},
    ]

    assert send(app, model, ":changes/-2?limit(2)", select=["_cid", "_op", "status"]) == [
        {"_cid": 3, "_op": "patch", "status": "3"},
        {"_cid": 4, "_op": "patch", "status": "4"},
    ]

    assert send(app, model, ":changes/-3?limit(2)", select=["_cid", "_op", "status"]) == [
        {"_cid": 2, "_op": "patch", "status": "2"},
        {"_cid": 3, "_op": "patch", "status": "3"},
    ]

    assert send(app, model, ":changes/-4?limit(2)", select=["_cid", "_op", "status"]) == [
        {"_cid": 1, "_op": "insert", "status": "1"},
        {"_cid": 2, "_op": "patch", "status": "2"},
    ]

    assert send(app, model, ":changes/-5?limit(2)", select=["_cid", "_op", "status"]) == [
        {"_cid": 1, "_op": "insert", "status": "1"},
        {"_cid": 2, "_op": "patch", "status": "2"},
    ]


@pytest.mark.models(
    "backends/postgres/Report",
    # 'backends/mongo/Report',
)
def test_changes_empty_patch(model, context, app):
    app.authmodel(model, ["insert", "patch", "changes"])
    obj = send(app, model, "insert", {"status": "1"})
    obj = send(app, model, "patch", obj, {"status": "1"})
    obj = send(app, model, "patch", obj, {"status": "1"})

    assert send(app, model, ":changes", select=["_cid", "_op", "status"]) == [
        {"_cid": 1, "_op": "insert", "status": "1"},
    ]


def test_changes_with_ref(context, app):
    model = "backends/postgres/Country"
    app.authmodel(model, ["insert"])
    country = send(app, model, "insert", {"title": "Lithuania"})

    model = "backends/postgres/City"
    app.authmodel(model, ["insert", "changes"])
    send(app, model, "insert", {"title": "Vilnius", "country": {"_id": country.id}})
    send(app, model, "insert", {"title": "Kaunas", "country": {"_id": country.id}})

    resp = app.get(f"{model}/:changes/:format/ascii")
    _, header, *lines, _ = resp.text.splitlines()
    header = header.split()
    assert header == [
        "_cid",
        "_created",
        "_op",
        "_id",
        "_txn",
        "_revision",
        "_same_as",
        "title",
        "country._id",
    ]
    lines = (dict(zip(header, line.split())) for line in lines)
    lines = [
        (
            x["_op"],
            x["title"],
            x["country._id"],
        )
        for x in lines
    ]
    assert lines == [
        ("insert", "Vilnius", country.id),
        ("insert", "Kaunas", country.id),
    ]


def test_changes_with_ref_reassign(context, app):
    model = "backends/postgres/Country"
    app.authmodel(model, ["insert"])
    lt = send(app, model, "insert", {"title": "Lithuania"})
    lv = send(app, model, "insert", {"title": "Latvia"})

    model = "backends/postgres/City"
    app.authmodel(model, ["insert", "changes", "patch"])
    vilnius = send(app, model, "insert", {"title": "Vilnius", "country": {"_id": lt.id}})
    kaunas = send(app, model, "insert", {"title": "Kaunas", "country": {"_id": lv.id}})
    send(app, model, "patch", kaunas, {"country": {"_id": lt.id}})

    resp = app.get(f"{model}/:changes/:format/ascii")
    _, header, *lines, _ = resp.text.splitlines()
    header = header.split()
    assert header == [
        "_cid",
        "_created",
        "_op",
        "_id",
        "_txn",
        "_revision",
        "_same_as",
        "title",
        "country._id",
    ]
    lines = (dict(zip(header, line.split())) for line in lines)
    lines = [
        (
            x["_op"],
            x["_id"],
            x["country._id"],
        )
        for x in lines
    ]
    assert lines == [
        ("insert", vilnius.id, lt.id),
        ("insert", kaunas.id, lv.id),
        ("patch", kaunas.id, lt.id),
    ]

    assert app.get(f"{model}/:changes/:format/html").status_code == 200
    assert app.get(f"{model}/:changes/:format/json").status_code == 200
    assert app.get(f"{model}/:changes/:format/jsonl").status_code == 200
    assert app.get(f"{model}/:changes/:format/rdf").status_code == 200


def test_changes_with_ref_unassign(context, app):
    model = "backends/postgres/Country"
    app.authmodel(model, ["insert"])
    lt = send(app, model, "insert", {"title": "Lithuania"})
    lv = send(app, model, "insert", {"title": "Latvia"})

    model = "backends/postgres/City"
    app.authmodel(model, ["insert", "changes", "patch"])
    vilnius = send(app, model, "insert", {"title": "Vilnius", "country": {"_id": lt.id}})
    kaunas = send(app, model, "insert", {"title": "Kaunas", "country": {"_id": lv.id}})
    send(app, model, "patch", kaunas, {"country": None})

    resp = app.get(f"{model}/:changes/:format/ascii")
    _, header, *lines, _ = resp.text.splitlines()
    header = header.split()
    assert header == [
        "_cid",
        "_created",
        "_op",
        "_id",
        "_txn",
        "_revision",
        "_same_as",
        "title",
        "country._id",
    ]
    lines = (dict(zip(header, line.split())) for line in lines)
    lines = [
        (
            x["_op"],
            x["_id"],
            x["country._id"],
        )
        for x in lines
    ]
    assert lines == [
        ("insert", vilnius.id, lt.id),
        ("insert", kaunas.id, lv.id),
        ("patch", kaunas.id, "∅"),
    ]

    assert app.get(f"{model}/:changes/:format/html").status_code == 200
    assert app.get(f"{model}/:changes/:format/json").status_code == 200
    assert app.get(f"{model}/:changes/:format/jsonl").status_code == 200
    assert app.get(f"{model}/:changes/:format/rdf").status_code == 200


@pytest.mark.manifests("internal_sql", "csv")
def test_changes_invalid_ref_changelog(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type    | ref      | access  | uri
            example/ref/invalid     |         |          |         |
              |   |   | Test         |         | id, name, ref | open    | 
              |   |   |   | id       | integer |          |         | 
              |   |   |   | name     | string  |          |         |
              |   |   |   | ref      | ref     | Ref      |         |
              |   |   | Ref          |         | id       | open    | 
              |   |   |   | id       | integer |          |         | 
            """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/ref/invalid", ["insert", "getall", "search", "patch", "changes"])
    ref = send(app, "example/ref/invalid/Ref", "insert", {"id": 0})
    model = "/example/ref/invalid/Test"
    test = send(app, model, "insert", {"id": 0, "name": "Test", "ref": {"_id": ref.id}})

    # Generate different change (need to do id changes, so it generates entries (same changes are skipped)
    test0 = send(app, model, "patch", test, {"id": 1, "ref": None})
    test1 = send(app, model, "patch", test0, {"id": 2})
    test2 = send(app, model, "patch", test1, {"id": 3})
    test3 = send(app, model, "patch", test2, {"id": 0})

    # Imitate incorrect changelog
    engine = sa.create_engine(postgresql)
    with engine.connect() as conn:
        conn.execute(f"""
        UPDATE "example/ref/invalid/Test/:changelog" 
        SET data = \'{{"ref": null}}\'
        WHERE _revision = '{test0.rev}'
        """)

        conn.execute(f"""
        UPDATE "example/ref/invalid/Test/:changelog" 
        SET data = \'{{"ref": {{"_id": null}}}}\'
        WHERE _revision = '{test1.rev}'
        """)

        conn.execute(f"""
        UPDATE "example/ref/invalid/Test/:changelog" 
        SET data = \'{{"ref": ""}}\'
        WHERE _revision = '{test2.rev}'
        """)

        conn.execute(f"""
        UPDATE "example/ref/invalid/Test/:changelog" 
        SET data = \'{{"ref": {{"_id": ""}}}}\'
        WHERE _revision = '{test3.rev}'
        """)

    resp = app.get(f"{model}/:changes/:format/ascii")
    _, header, *lines, _ = resp.text.splitlines()
    header = header.split()
    assert header == [
        "_cid",
        "_created",
        "_op",
        "_id",
        "_txn",
        "_revision",
        "_same_as",
        "id",
        "name",
        "ref._id",
    ]
    lines = (dict(zip(header, line.split())) for line in lines)
    lines = [
        (
            x["_cid"],
            x["_op"],
            x["_id"],
            x["ref._id"],
        )
        for x in lines
    ]
    assert lines == [
        ("1", "insert", test.id, ref.id),
        ("2", "patch", test.id, "∅"),
        ("3", "patch", test.id, "∅"),
        ("4", "patch", test.id, "∅"),
        ("5", "patch", test.id, "∅"),
    ]

    resp = app.get(f"{model}/:changes/:format/json")
    assert listdata(resp, "_cid", "_op", "_id", "ref") == [
        (1, "insert", test.id, {"_id": ref.id}),
        (2, "patch", test.id, None),
        (3, "patch", test.id, None),
        (4, "patch", test.id, None),
        (5, "patch", test.id, None),
    ]

    assert app.get(f"{model}/:changes/:format/html").status_code == 200
    assert app.get(f"{model}/:changes/:format/jsonl").status_code == 200
    assert app.get(f"{model}/:changes/:format/rdf").status_code == 200
