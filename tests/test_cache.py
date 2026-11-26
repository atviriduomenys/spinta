import datetime
from datetime import timezone
from email.utils import format_datetime

import pytest
from starlette.routing import Route

from spinta.core.config import RawConfig
from spinta.middlewares import _is_normalized_path
from spinta.testing.client import create_test_client
from spinta.testing.manifest import prepare_manifest


def test_cache_control_postgres_get_one(context, app):
    model = "backends/postgres/Report"
    app.authmodel(model, ["insert", "getone", "changes"])
    app.authorize(["spinta_set_meta_fields"])

    # create extra report
    resp = app.post(
        f"/{model}",
        json={
            "_type": "Report",
            "status": "1",
        },
    )
    assert resp.status_code == 201, resp.text
    entry_id = resp.json()["_id"]
    app.post(
        f"/{model}",
        json={
            "_type": "Report",
            "status": "2",
        },
    )

    last_changelog_entry = app.get(
        f"/{model}/:changes/-1",
    )
    last_changelog_data = last_changelog_entry.json()["_data"][0]

    changelog_entry = app.get(
        f"/{model}/{entry_id}/:changes/-1",
    )
    changelog_data = changelog_entry.json()["_data"][0]

    assert last_changelog_data["_revision"] != changelog_data["_revision"]

    resp = app.get(
        f"/{model}/{entry_id}",
    )
    assert resp.status_code == 200
    assert resp.headers["Cache-Control"] == "public, max-age=60, must-revalidate"
    assert resp.headers["Vary"] == "Accept, Accept-Language, Authorization"
    assert resp.headers["ETag"] == changelog_data["_revision"]
    assert resp.headers["Last-Modified"] == format_datetime(
        datetime.datetime.fromisoformat(changelog_data["_created"]).replace(tzinfo=timezone.utc), usegmt=True
    )


def test_model_getall_becomes_cacheable_after_first_insert(app):
    model = "backends/postgres/Report"
    app.authmodel(model, ["insert", "getall", "changes"])
    app.authorize(["spinta_set_meta_fields"])

    empty = app.get(f"/{model}")

    assert empty.status_code == 200
    assert empty.json()["_data"] == []
    assert empty.headers["Cache-Control"] == "no-store"
    assert "ETag" not in empty.headers
    assert "Last-Modified" not in empty.headers

    inserted = app.post(
        f"/{model}",
        json={
            "_type": "Report",
            "status": "1",
        },
    )
    assert inserted.status_code == 201
    assert inserted.headers["Cache-Control"] == "no-store"
    report = inserted.json()

    changelog_entry = app.get(
        f"/{model}/:changes/-1",
    )
    changelog_data = changelog_entry.json()["_data"][0]

    resp = app.get(
        f"/{model}",
    )
    assert resp.status_code == 200
    assert [row["_id"] for row in resp.json()["_data"]] == [report["_id"]]
    assert resp.headers["Cache-Control"] == "public, max-age=60, must-revalidate"
    assert resp.headers["Vary"] == "Accept, Accept-Language, Authorization"
    assert resp.headers["ETag"] == changelog_data["_revision"]
    assert resp.headers["Last-Modified"] == format_datetime(
        datetime.datetime.fromisoformat(changelog_data["_created"]).replace(tzinfo=timezone.utc), usegmt=True
    )


def test_populated_model_head_returns_get_cache_headers(app):
    model = "backends/postgres/Report"
    app.authmodel(model, ["insert", "getall"])
    inserted = app.post(f"/{model}", json={"status": "1"})
    assert inserted.status_code == 201, inserted.text

    get = app.get(f"/{model}")
    head = app.head(f"/{model}")

    assert get.status_code == head.status_code == 200
    assert get.headers["Cache-Control"] == "public, max-age=60, must-revalidate"
    assert head.content == b""
    for header in ("Cache-Control", "Vary", "ETag", "Last-Modified"):
        assert head.headers[header] == get.headers[header]


def test_populated_model_summary_is_cacheable(app):
    model = "backends/postgres/Report"
    app.authmodel(model, ["insert", "getall", "search"])
    inserted = app.post(f"/{model}", json={"count": 1})
    assert inserted.status_code == 201, inserted.text

    get = app.get(f"/{model}")
    summary = app.get(f"/{model}/:summary/count")

    assert get.status_code == summary.status_code == 200
    assert summary.json()["_data"]
    assert summary.headers["Cache-Control"] == "public, max-age=60, must-revalidate"
    assert summary.headers["ETag"] == inserted.json()["_revision"]
    for header in ("Vary", "Last-Modified"):
        assert summary.headers[header] == get.headers[header]


def test_empty_model_uses_delete_revision_after_last_row_is_removed(app):
    model = "backends/postgres/Report"
    app.authmodel(model, ["insert", "delete", "getall", "changes"])
    inserted = app.post(f"/{model}", json={"status": "1"})
    assert inserted.status_code == 201, inserted.text
    report = inserted.json()
    deleted = app.delete(f"/{model}/{report['_id']}")
    assert deleted.status_code == 204, deleted.text
    assert deleted.headers["Cache-Control"] == "no-store"

    changelog = app.get(f"/{model}/:changes/-1")
    assert changelog.status_code == 200, changelog.text
    change = changelog.json()["_data"][0]
    assert change["_op"] == "delete"

    empty = app.get(f"/{model}")
    assert empty.status_code == 200
    assert empty.json()["_data"] == []
    assert empty.headers["Cache-Control"] == "public, max-age=60, must-revalidate"
    assert empty.headers["ETag"] == change["_revision"] != report["_revision"]
    assert empty.headers["Last-Modified"] == format_datetime(
        datetime.datetime.fromisoformat(change["_created"]).replace(tzinfo=timezone.utc), usegmt=True
    )


def test_model_patch_is_not_cacheable(app):
    model = "backends/postgres/Report"
    app.authmodel(model, ["insert", "patch"])
    inserted = app.post(f"/{model}", json={"status": "1"})
    assert inserted.status_code == 201, inserted.text
    report = inserted.json()

    patched = app.patch(f"/{model}/{report['_id']}", json={"status": "2", "_revision": report["_revision"]})

    assert patched.status_code == 200, patched.text
    assert patched.headers["Cache-Control"] == "no-store"


def test_cache_control_postgres_changes(context, app):
    model = "backends/postgres/Report"
    app.authmodel(model, ["insert", "changes"])
    app.authorize(["spinta_set_meta_fields"])

    # create extra report
    app.post(
        f"/{model}",
        json={
            "_type": "Report",
            "status": "1",
        },
    )

    app.post(
        f"/{model}",
        json={
            "_type": "Report",
            "status": "2",
        },
    )

    changelog_entry = app.get(
        f"/{model}/:changes/-1",
    )
    changelog_data = changelog_entry.json()["_data"][0]

    resp = app.get(
        f"/{model}/:changes",
    )
    assert resp.status_code == 200
    assert resp.headers["Cache-Control"] == "public, max-age=60, must-revalidate"
    assert resp.headers["ETag"] == changelog_data["_revision"]
    assert resp.headers["Last-Modified"] == format_datetime(
        datetime.datetime.fromisoformat(changelog_data["_created"]).replace(tzinfo=timezone.utc), usegmt=True
    )


def test_cache_control_postgres_changes_specific(context, app):
    model = "backends/postgres/Report"
    app.authmodel(model, ["insert", "changes"])
    app.authorize(["spinta_set_meta_fields"])

    # create extra report
    resp = app.post(
        f"/{model}",
        json={
            "_type": "Report",
            "status": "1",
        },
    )
    entry_id = resp.json()["_id"]

    app.post(
        f"/{model}",
        json={
            "_type": "Report",
            "status": "2",
        },
    )

    changelog_entry = app.get(
        f"/{model}/{entry_id}/:changes/-1",
    )
    changelog_data = changelog_entry.json()["_data"][0]

    resp = app.get(
        f"/{model}/{entry_id}/:changes",
    )
    assert resp.status_code == 200
    assert resp.headers["Cache-Control"] == "public, max-age=60, must-revalidate"
    assert resp.headers["ETag"] == changelog_data["_revision"]
    assert resp.headers["Last-Modified"] == format_datetime(
        datetime.datetime.fromisoformat(changelog_data["_created"]).replace(tzinfo=timezone.utc), usegmt=True
    )


def test_cache_control_etag(context, app):
    model = "backends/postgres/Report"
    app.authmodel(model, ["insert", "getall", "changes"])
    app.authorize(["spinta_set_meta_fields"])

    # create extra report
    resp = app.post(
        f"/{model}",
        json={
            "_type": "Report",
            "status": "1",
        },
    )
    entry_revision = resp.json()["_revision"]

    app.post(
        f"/{model}",
        json={
            "_type": "Report",
            "status": "2",
        },
    )

    changelog_entry = app.get(
        f"/{model}/:changes/-1",
    )
    changelog_data = changelog_entry.json()["_data"][0]

    resp = app.get(
        f"/{model}",
        headers={"If-None-Match": entry_revision},
    )
    assert resp.status_code == 200
    assert resp.headers["Cache-Control"] == "public, max-age=60, must-revalidate"
    assert resp.headers["ETag"] == changelog_data["_revision"]
    assert resp.headers["Last-Modified"] == format_datetime(
        datetime.datetime.fromisoformat(changelog_data["_created"]).replace(tzinfo=timezone.utc), usegmt=True
    )

    resp = app.get(
        f"/{model}",
        headers={"If-None-Match": changelog_data["_revision"]},
    )
    assert resp.status_code == 304
    assert resp.headers["Cache-Control"] == "public, max-age=60, must-revalidate"
    assert resp.headers["ETag"] == changelog_data["_revision"]
    assert resp.headers["Last-Modified"] == format_datetime(
        datetime.datetime.fromisoformat(changelog_data["_created"]).replace(tzinfo=timezone.utc), usegmt=True
    )


def test_cache_control_last_modified(context, app):
    model = "backends/postgres/Report"
    app.authmodel(model, ["insert", "getall", "changes"])
    app.authorize(["spinta_set_meta_fields"])

    # create extra report
    resp = app.post(
        f"/{model}",
        json={
            "_type": "Report",
            "status": "1",
        },
    )
    entry_id = resp.json()["_id"]

    app.post(
        f"/{model}",
        json={
            "_type": "Report",
            "status": "2",
        },
    )

    changelog_entry = app.get(
        f"/{model}/{entry_id}/:changes/-1",
    )
    entry_modified = changelog_entry.json()["_data"][0]["_created"]

    changelog_entry = app.get(
        f"/{model}/:changes/-1",
    )
    changelog_data = changelog_entry.json()["_data"][0]

    resp = app.get(
        f"/{model}",
        headers={
            "If-Modified-Since": format_datetime(
                datetime.datetime.fromisoformat(entry_modified).replace(year=1900, tzinfo=timezone.utc),
                usegmt=True,
            )
        },
    )
    assert resp.status_code == 200
    assert resp.headers["Cache-Control"] == "public, max-age=60, must-revalidate"
    assert resp.headers["ETag"] == changelog_data["_revision"]
    assert resp.headers["Last-Modified"] == format_datetime(
        datetime.datetime.fromisoformat(changelog_data["_created"]).replace(tzinfo=timezone.utc), usegmt=True
    )

    resp = app.get(
        f"/{model}",
        headers={
            "If-Modified-Since": format_datetime(
                datetime.datetime.fromisoformat(changelog_data["_created"]).replace(tzinfo=timezone.utc), usegmt=True
            )
        },
    )
    assert resp.status_code == 304
    assert resp.headers["Cache-Control"] == "public, max-age=60, must-revalidate"
    assert resp.headers["ETag"] == changelog_data["_revision"]
    assert resp.headers["Last-Modified"] == format_datetime(
        datetime.datetime.fromisoformat(changelog_data["_created"]).replace(tzinfo=timezone.utc), usegmt=True
    )


def test_cache_control_priority(context, app):
    model = "backends/postgres/Report"
    app.authmodel(model, ["insert", "getall", "changes"])
    app.authorize(["spinta_set_meta_fields"])

    # create extra report
    app.post(
        f"/{model}",
        json={
            "_type": "Report",
            "status": "1",
        },
    )

    resp = app.post(
        f"/{model}",
        json={
            "_type": "Report",
            "status": "2",
        },
    )
    entry_id = resp.json()["_id"]

    changelog_entry = app.get(
        f"/{model}/{entry_id}/:changes/-1",
    )
    entry_modified = changelog_entry.json()["_data"][0]["_created"]

    changelog_entry = app.get(
        f"/{model}/:changes/-1",
    )
    changelog_data = changelog_entry.json()["_data"][0]

    # Should ignore Modified since, because ETag is present
    resp = app.get(
        f"/{model}",
        headers={
            "If-None-Match": changelog_data["_revision"],
            "If-Modified-Since": format_datetime(
                datetime.datetime.fromisoformat(entry_modified).replace(year=1900, tzinfo=timezone.utc),
                usegmt=True,
            ),
        },
    )
    assert resp.status_code == 304
    assert resp.headers["Cache-Control"] == "public, max-age=60, must-revalidate"
    assert resp.headers["ETag"] == changelog_data["_revision"]
    assert resp.headers["Last-Modified"] == format_datetime(
        datetime.datetime.fromisoformat(changelog_data["_created"]).replace(tzinfo=timezone.utc), usegmt=True
    )

    resp = app.get(
        f"/{model}",
        headers={
            "If-Modified-Since": format_datetime(
                datetime.datetime.fromisoformat(changelog_data["_created"]).replace(tzinfo=timezone.utc), usegmt=True
            )
        },
    )
    assert resp.status_code == 304
    assert resp.headers["Cache-Control"] == "public, max-age=60, must-revalidate"
    assert resp.headers["ETag"] == changelog_data["_revision"]
    assert resp.headers["Last-Modified"] == format_datetime(
        datetime.datetime.fromisoformat(changelog_data["_created"]).replace(tzinfo=timezone.utc), usegmt=True
    )


@pytest.mark.parametrize(
    "path, raw_path, expected",
    [
        ("/", b"/", True),
        ("/example/City", b"/example/City", True),
        ("/example/City/", b"/example/City/", True),
        ("//example/City", b"//example/City", False),
        ("/example/City//", b"/example/City//", False),
        ("/example/../City", b"/example/../City", False),
        ("/example/./City", b"/example/./City", False),
        ("/example//City", b"/example//City", False),
        ("/example/City/..", b"/example/City/..", False),
        ("/example/../City", b"/example/..%2fCity", False),
        ("/example/../City", b"/example/..%2FCity", False),
        ("/example/City", b"/example%2fCity", False),
        ("/example\\City", b"/example%5cCity", False),
        ("/example/.%2e/City", b"/example/.%2e/City", False),
    ],
)
def test_is_normalized_path(path: str, raw_path: bytes, expected: bool):
    scope = {"type": "http", "path": path, "raw_path": raw_path}
    assert _is_normalized_path(scope) is expected


def _create_memory_app(rc: RawConfig):
    context, manifest = prepare_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref  | access
    example                  |        |      |
      |   |   | City         |        | name |
      |   |   |   | name     | string |      | open
    """,
        backend="memory",
    )
    context.loaded = True
    return create_test_client(context)


def test_error_response_not_cacheable(rc: RawConfig):
    app = _create_memory_app(rc)
    resp = app.get("/example/DoesNotExist")
    assert resp.status_code == 404
    assert resp.headers["Cache-Control"] == "no-store"


@pytest.mark.parametrize("method", ["GET", "HEAD"])
@pytest.mark.parametrize("path", ["/example", "/example/City", "/version"])
def test_responses_default_to_no_store(rc: RawConfig, method: str, path: str):
    app = _create_memory_app(rc)
    app.authmodel("example", ["getall", "search"])
    app.authmodel("example/City", ["getall", "search"])

    resp = app.request(method, path, headers={"If-None-Match": '"unknown"'})

    assert resp.status_code == 200
    assert resp.headers["Cache-Control"] == "no-store"


def test_debug_error_response_defaults_to_no_store(rc: RawConfig):
    app = create_test_client(rc.fork({"debug": True}), raise_server_exceptions=False)

    async def fail(request):
        raise RuntimeError("test server error")

    app.app.routes.insert(0, Route("/test-error", fail))
    resp = app.get("/test-error")

    assert resp.status_code == 500
    assert resp.headers["Cache-Control"] == "no-store"


def test_non_normalized_path_rejected(rc: RawConfig):
    app = _create_memory_app(rc)
    resp = app.get("/example/..%2fCity")
    assert resp.status_code == 404
    assert resp.headers["Cache-Control"] == "no-store"
    assert resp.json()["errors"][0]["code"] == "InvalidRequestPath"
