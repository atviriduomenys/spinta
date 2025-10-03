import datetime
from datetime import timezone
from email.utils import format_datetime


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
    assert resp.headers["ETag"] == changelog_data["_revision"]
    assert resp.headers["Last-Modified"] == format_datetime(
        datetime.datetime.fromisoformat(changelog_data["_created"]).replace(tzinfo=timezone.utc), usegmt=True
    )


def test_cache_control_postgres_get_all(context, app):
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

    changelog_entry = app.get(
        f"/{model}/:changes/-1",
    )
    changelog_data = changelog_entry.json()["_data"][0]

    resp = app.get(
        f"/{model}",
    )
    assert resp.status_code == 200
    assert resp.headers["Cache-Control"] == "public, max-age=60, must-revalidate"
    assert resp.headers["ETag"] == changelog_data["_revision"]
    assert resp.headers["Last-Modified"] == format_datetime(
        datetime.datetime.fromisoformat(changelog_data["_created"]).replace(tzinfo=timezone.utc), usegmt=True
    )


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
