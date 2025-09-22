import json
import pathlib
import datetime

import pytest
from _pytest.capture import CaptureFixture
from _pytest.fixtures import FixtureRequest

from spinta.accesslog.file import FileAccessLog
from spinta.auth import get_default_auth_client_id, load_key, KeyType, create_access_token
from spinta.components import Store
from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.context import create_test_context
from spinta.testing.manifest import bootstrap_manifest


def _upload_pdf(model, app):
    resp = app.post(f"/{model}", json={"status": "42"})
    assert resp.status_code == 201
    data = resp.json()
    id_ = data["_id"]
    rev = data["_revision"]

    resp = app.put(
        f"/{model}/{id_}/pdf",
        content=b"BINARYDATA",
        headers={
            "revision": rev,
            "content-type": "application/pdf",
            "content-disposition": 'attachment; filename="test.pdf"',
        },
    )
    assert resp.status_code == 200, resp.text
    return id_, rev, resp


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
def test_post_accesslog(model, app, context):
    app.authmodel(model, ["insert"])

    resp = app.post(f"/{model}", json={"status": "42"})
    assert resp.status_code == 201, resp.json()

    accesslog = context.get("accesslog.stream")
    assert len(accesslog) == 2
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "client": "test-client",
            "method": "POST",
            "url": f"https://testserver/{model}",
            "agent": "testclient",
            "rctype": "application/json",
            "format": "json",
            "action": "insert",
            "model": model,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
def test_post_array_accesslog(model, app, context):
    app.authmodel(model, ["insert"])
    resp = app.post(
        f"/{model}",
        json={
            "status": "42",
            "notes": [
                {
                    "note": "foo",
                }
            ],
        },
    )
    assert resp.status_code == 201

    accesslog = context.get("accesslog.stream")
    assert len(accesslog) == 2
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "method": "POST",
            "agent": "testclient",
            "rctype": "application/json",
            "format": "json",
            "action": "insert",
            "url": f"https://testserver/{model}",
            "client": "test-client",
            "model": model,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
def test_put_accesslog(model, app, context):
    app.authmodel(model, ["insert", "update"])
    resp = app.post(f"/{model}", json={"status": "42"})
    assert resp.status_code == 201
    data = resp.json()
    id_ = data["_id"]
    rev = data["_revision"]
    revision = data["_revision"]

    resp = app.put(
        f"/{model}/{id_}",
        json={
            "status": "314",
            "_revision": revision,
        },
    )
    assert resp.status_code == 200

    accesslog = context.get("accesslog.stream")
    assert len(accesslog) == 4
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "method": "PUT",
            "agent": "testclient",
            "rctype": "application/json",
            "format": "json",
            "action": "update",
            "url": f"https://testserver/{model}/{id_}",
            "client": "test-client",
            "model": model,
            "id": id_,
            "rev": rev,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
def test_pdf_put_accesslog(model, app, context):
    app.authmodel(model, ["insert", "update", "pdf_update"])
    id_, rev, resp = _upload_pdf(model, app)

    accesslog = context.get("accesslog.stream")
    assert len(accesslog) == 4  # 2 accesses overall: POST and PUT
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "action": "update",
            "agent": "testclient",
            "rctype": "application/pdf",
            "format": "json",
            "method": "PUT",
            "url": f"https://testserver/{model}/{id_}/pdf",
            "client": "test-client",
            "model": model,
            "prop": "pdf",
            "id": id_,
            "rev": rev,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
def test_patch_accesslog(model, app, context):
    app.authmodel(model, ["insert", "patch"])
    resp = app.post(f"/{model}", json={"status": "42"})
    assert resp.status_code == 201
    data = resp.json()
    id_ = data["_id"]
    rev = data["_revision"]

    resp = app.patch(
        f"/{model}/{id_}",
        json={
            "_revision": rev,
            "status": "13",
        },
    )
    assert resp.status_code == 200

    accesslog = context.get("accesslog.stream")
    assert len(accesslog) == 4
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "agent": "testclient",
            "rctype": "application/json",
            "format": "json",
            "action": "patch",
            "method": "PATCH",
            "url": f"https://testserver/{model}/{id_}",
            "client": "test-client",
            "model": model,
            "id": id_,
            "rev": rev,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
def test_get_accesslog(app, model, context):
    app.authmodel(model, ["insert", "getone"])
    resp = app.post(f"/{model}", json={"status": "42"})
    assert resp.status_code == 201

    data = resp.json()
    id_ = data["_id"]
    resp = app.get(f"/{model}/{id_}")
    assert resp.status_code == 200

    accesslog = context.get("accesslog.stream")
    assert len(accesslog) == 4
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "agent": "testclient",
            "format": "json",
            "action": "getone",
            "method": "GET",
            "url": f"https://testserver/{model}/{id_}",
            "client": "test-client",
            "model": model,
            "id": id_,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
def test_get_array_accesslog(model, app, context):
    app.authmodel(model, ["insert", "getone"])
    resp = app.post(
        f"/{model}",
        json={
            "status": "42",
            "notes": [
                {
                    "note": "foo",
                }
            ],
        },
    )
    assert resp.status_code == 201
    data = resp.json()
    id_ = data["_id"]

    app.get(f"/{model}/{id_}")
    accesslog = context.get("accesslog.stream")
    assert len(accesslog) == 4
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "agent": "testclient",
            "format": "json",
            "action": "getone",
            "method": "GET",
            "url": f"https://testserver/{model}/{id_}",
            "client": "test-client",
            "model": model,
            "id": id_,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
def test_pdf_get_accesslog(model, app, context):
    app.authmodel(model, ["insert", "update", "pdf_update", "pdf_getone"])
    id_, revision, resp = _upload_pdf(model, app)

    app.get(f"/{model}/{id_}/pdf")
    accesslog = context.get("accesslog.stream")
    assert len(accesslog) == 6  # 3 accesses overall: POST, PUT, GET
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "format": "json",
            "agent": "testclient",
            "action": "getone",
            "method": "GET",
            "url": f"https://testserver/{model}/{id_}/pdf",
            "client": "test-client",
            "model": model,
            "prop": "pdf",
            "id": id_,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
def test_get_prop_accesslog(app, model, context):
    app.authmodel(model, ["insert", "getone"])
    resp = app.post(f"/{model}", json={"status": "42"})
    assert resp.status_code == 201

    data = resp.json()
    pk = data["_id"]
    resp = app.get(f"/{model}/{pk}/sync")
    assert resp.status_code == 200

    accesslog = context.get("accesslog.stream")
    assert len(accesslog) == 4
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "agent": "testclient",
            "format": "json",
            "action": "getone",
            "method": "GET",
            "url": f"https://testserver/{model}/{pk}/sync",
            "client": "test-client",
            "model": model,
            "prop": "sync",
            "id": pk,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
def test_get_w_select_accesslog(app, model, context):
    app.authmodel(model, ["insert", "getone"])
    resp = app.post(f"/{model}", json={"status": "42"})
    assert resp.status_code == 201

    data = resp.json()
    pk = data["_id"]
    resp = app.get(f"/{model}/{pk}?select(status)")
    assert resp.status_code == 200

    accesslog = context.get("accesslog.stream")
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "agent": "testclient",
            "format": "json",
            "action": "getone",
            "method": "GET",
            "url": f"https://testserver/{model}/{pk}?select(status)",
            "client": "test-client",
            "model": model,
            "id": pk,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
def test_getall_accesslog(app, model, context):
    app.authmodel(model, ["insert", "getall"])

    resp = app.post(f"/{model}", json={"status": "42"})
    assert resp.status_code == 201

    resp = app.get(f"/{model}")
    assert resp.status_code == 200

    accesslog = context.get("accesslog.stream")
    assert len(accesslog) == 4
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "agent": "testclient",
            "format": "json",
            "action": "getall",
            "method": "GET",
            "url": f"https://testserver/{model}",
            "client": "test-client",
            "model": model,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
def test_getall_w_select_accesslog(app, model, context):
    app.authmodel(model, ["insert", "getall", "search"])
    resp = app.post(f"/{model}", json={"status": "42"})
    assert resp.status_code == 201

    resp = app.get(f"/{model}?select(status)")
    assert resp.status_code == 200

    accesslog = context.get("accesslog.stream")
    assert len(accesslog) == 4
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "agent": "testclient",
            "format": "json",
            "action": "search",
            "method": "GET",
            "url": f"https://testserver/{model}?select(status)",
            "client": "test-client",
            "model": model,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.models(
    "backends/postgres/Report",
)
def test_accesslog_file(model, postgresql, rc, request, tmp_path):
    logfile = tmp_path / "accesslog.log"

    rc = rc.fork(
        {
            "accesslog": {
                "type": "file",
                "file": str(logfile),
            },
        }
    )

    context = create_test_context(rc)
    request.addfinalizer(context.wipe_all)
    app = create_test_client(context)

    app.authmodel(model, ["insert"])
    resp = app.post(f"/{model}", json={"status": "42"})
    assert resp.status_code == 201

    accesslog = [json.loads(line) for line in logfile.read_text().splitlines()]
    assert len(accesslog) == 2
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "agent": "testclient",
            "rctype": "application/json",
            "format": "json",
            "action": "insert",
            "method": "POST",
            "url": f"https://testserver/{model}",
            "client": "test-client",
            "model": model,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.models(
    "backends/postgres/Report",
)
def test_accesslog_file_dev_null(model, postgresql, rc, request):
    rc = rc.fork(
        {
            "accesslog": {
                "type": "file",
                "file": "/dev/null",
            },
        }
    )

    context = create_test_context(rc)
    request.addfinalizer(context.wipe_all)
    app = create_test_client(context)

    app.authmodel(model, ["insert"])
    resp = app.post(f"/{model}", json={"status": "42"})
    assert resp.status_code == 201

    store: Store = context.get("store")
    assert isinstance(store.accesslog, FileAccessLog)
    assert store.accesslog.file is None


@pytest.mark.models(
    "backends/postgres/Report",
)
def test_accesslog_file_null(model, postgresql, rc, request):
    rc = rc.fork(
        {
            "accesslog": {
                "type": "file",
                "file": "null",
            },
        }
    )

    context = create_test_context(rc)
    request.addfinalizer(context.wipe_all)
    app = create_test_client(context)

    app.authmodel(model, ["insert"])
    resp = app.post(f"/{model}", json={"status": "42"})
    assert resp.status_code == 201

    store: Store = context.get("store")
    assert isinstance(store.accesslog, FileAccessLog)
    assert store.accesslog.file is None


@pytest.mark.models(
    "backends/postgres/Report",
)
def test_accesslog_file_stdin(
    model: str,
    postgresql,
    rc: RawConfig,
    request,
    capsys: CaptureFixture,
):
    rc = rc.fork(
        {
            "accesslog": {
                "type": "file",
                "file": "stdout",
            },
        }
    )

    context = create_test_context(rc)
    request.addfinalizer(context.wipe_all)
    app = create_test_client(context)

    app.authmodel(model, ["insert"])
    resp = app.post(f"/{model}", json={"status": "42"})
    assert resp.status_code == 201

    store: Store = context.get("store")
    assert isinstance(store.accesslog, FileAccessLog)

    cap = capsys.readouterr()
    accesslog = [
        json.loads(line)
        for line in cap.out.splitlines()
        # Skip other lines from stdout that are not json
        if line.startswith("{")
    ]
    assert len(accesslog) == 2
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "action": "insert",
            "agent": "testclient",
            "client": "test-client",
            "format": "json",
            "method": "POST",
            "model": "backends/postgres/Report",
            "rctype": "application/json",
            "url": "https://testserver/backends/postgres/Report",
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.models(
    "backends/postgres/Report",
)
def test_accesslog_file_stderr(
    model: str,
    postgresql,
    rc: RawConfig,
    request,
    capsys: CaptureFixture,
):
    rc = rc.fork(
        {
            "accesslog": {
                "type": "file",
                "file": "stderr",
            },
        }
    )

    context = create_test_context(rc)
    request.addfinalizer(context.wipe_all)
    app = create_test_client(context)

    app.authmodel(model, ["insert"])
    resp = app.post(f"/{model}", json={"status": "42"})
    assert resp.status_code == 201

    store: Store = context.get("store")
    assert isinstance(store.accesslog, FileAccessLog)

    cap = capsys.readouterr()
    accesslog = [
        json.loads(line)
        for line in cap.err.splitlines()
        # Skip other lines from stdout that are not json
        if line.startswith("{")
    ]
    assert len(accesslog) == 2
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "action": "insert",
            "agent": "testclient",
            "client": "test-client",
            "format": "json",
            "method": "POST",
            "model": "backends/postgres/Report",
            "rctype": "application/json",
            "url": "https://testserver/backends/postgres/Report",
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
def test_delete_accesslog(model, app, context):
    app.authmodel(model, ["insert", "delete"])
    resp = app.post(f"/{model}", json={"status": "42"})
    assert resp.status_code == 201
    data = resp.json()
    id_ = data["_id"]

    resp = app.delete(f"/{model}/{id_}")
    assert resp.status_code == 204
    assert resp.content == b""

    accesslog = context.get("accesslog.stream")
    assert len(accesslog) == 4
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "agent": "testclient",
            "format": "json",
            "action": "delete",
            "method": "DELETE",
            "url": f"https://testserver/{model}/{id_}",
            "client": "test-client",
            "model": model,
            "id": data["_id"],
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
def test_pdf_delete_accesslog(model, app, context):
    app.authmodel(model, ["insert", "update", "getone", "pdf_getone", "pdf_update", "pdf_delete"])
    id_, rev, resp = _upload_pdf(model, app)

    resp = app.delete(f"/{model}/{id_}/pdf")
    assert resp.status_code == 204
    assert resp.content == b""

    accesslog = context.get("accesslog.stream")
    assert len(accesslog) == 6  # 3 accesses overall: POST, PUT, DELETE
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "agent": "testclient",
            "format": "json",
            "action": "delete",
            "method": "DELETE",
            "url": f"https://testserver/{model}/{id_}/pdf",
            "client": "test-client",
            "model": model,
            "prop": "pdf",
            "id": id_,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


def _get_object_rev(app, model: str, id_: str) -> str:
    resp = app.get(f"/{model}/{id_}")
    assert resp.status_code == 200
    data = resp.json()
    return data["_revision"]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
def test_pdf_ref_update_accesslog(model, app, context, tmp_path):
    app.authmodel(model, ["insert", "update", "getone", "pdf_getone", "pdf_update", "pdf_delete"])
    _id, rev, resp = _upload_pdf(model, app)

    image = tmp_path / "image.png"
    image.write_bytes(b"IMAGEDATA")

    rev = _get_object_rev(app, model, _id)
    resp = app.put(f"/{model}/{_id}/pdf:ref", json={"_id": "image.png", "_revision": rev})
    assert resp.status_code == 200

    accesslog = context.get("accesslog.stream")
    srv = "https://testserver"
    assert [
        (
            a["type"],
            a.get("method"),
            a.get("url"),
        )
        for a in accesslog
    ] == [
        ("request", "POST", f"{srv}/{model}"),
        ("response", None, None),
        ("request", "PUT", f"{srv}/{model}/{_id}/pdf"),
        ("response", None, None),
        ("request", "GET", f"{srv}/{model}/{_id}"),
        ("response", None, None),
        ("request", "PUT", f"{srv}/{model}/{_id}/pdf:ref"),
        ("response", None, None),
    ]
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "agent": "testclient",
            "rctype": "application/json",
            "format": "json",
            "action": "update",
            "method": "PUT",
            "url": f"https://testserver/{model}/{_id}/pdf:ref",
            "client": "test-client",
            "model": model,
            "prop": "pdf",
            "id": _id,
            "rev": rev,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
def test_batch_write(model, app, context, tmp_path):
    ns = model[: -len("/Report")]

    app.authmodel(ns, ["insert"])

    resp = app.post(
        f"/{ns}",
        json={
            "_data": [
                {
                    "_op": "insert",
                    "_type": model,
                    "status": "ok",
                },
            ],
        },
    )
    resp.raise_for_status()

    accesslog = context.get("accesslog.stream")
    assert len(accesslog) == 2
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "agent": "testclient",
            "rctype": "application/json",
            "format": "json",
            "action": "insert",
            "method": "POST",
            "url": f"https://testserver/{ns}",
            "client": "test-client",
            "ns": ns,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
def test_stream_write(model, app, context, tmp_path):
    ns = model[: -len("/Report")]

    app.authmodel(ns, ["insert"])

    headers = {"content-type": "application/x-ndjson"}
    resp = app.post(
        f"/{ns}",
        headers=headers,
        content=json.dumps(
            {
                "_op": "insert",
                "_type": model,
                "status": "ok",
            }
        ),
    )
    resp.raise_for_status()

    accesslog = context.get("accesslog.stream")
    assert len(accesslog) == 2
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "agent": "testclient",
            "rctype": "application/x-ndjson",
            "format": "json",
            "action": "insert",
            "method": "POST",
            "url": f"https://testserver/{ns}",
            "client": "test-client",
            "ns": ns,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
def test_ns_read(model, app, context, tmp_path):
    ns = model[: -len("/Report")]

    app.authmodel(ns, ["getall"])

    resp = app.get(f"/{ns}/:ns/:all")
    assert resp.status_code == 200, resp.json()

    objects = {
        "backends/mongo/Report": 20,
        "backends/postgres/Report": 21,
    }

    accesslog = context.get("accesslog.stream")
    assert len(accesslog) == 2
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "agent": "testclient",
            "format": "json",
            "action": "getall",
            "method": "GET",
            "url": f"https://testserver/{ns}/:ns/:all",
            "client": "test-client",
            "ns": ns,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": objects[model],
        },
    ]


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
def test_ns_read_csv(model, app, context, tmp_path):
    ns = model[: -len("/Report")]

    app.authmodel(ns, ["getall"])

    resp = app.get(f"/{ns}/:ns/:all/:format/csv")
    assert resp.status_code == 200

    objects = {
        "backends/mongo/Report": 20,
        "backends/postgres/Report": 21,
    }

    accesslog = context.get("accesslog.stream")
    assert len(accesslog) == 2
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": accesslog[-2]["token"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "agent": "testclient",
            "format": "csv",
            "action": "getall",
            "method": "GET",
            "url": f"https://testserver/{ns}/:ns/:all/:format/csv",
            "client": "test-client",
            "ns": ns,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": objects[model],
        },
    ]


@pytest.mark.parametrize("scope", [{"spinta_getall", "spinta_search"}, {"uapi:/:getall", "uapi:/:search"}])
@pytest.mark.manifests("internal_sql", "csv")
def test_get_accesslog_default_user(
    manifest_type: str,
    tmp_path: pathlib.Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: set,
):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property      | type    | access
        backends/postgres/dtypes/test |         | open
          |   |   | Entity            |         |
          |   |   |   | id            | integer |
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    context.set("accesslog.stream", [])
    default_client_id = get_default_auth_client_id(context)
    token = create_access_token(
        context,
        load_key(context, KeyType.private),
        default_client_id,
        int(datetime.timedelta(days=10).total_seconds()),
        scope,
    )

    model = "backends/postgres/dtypes/test/Entity"
    app = create_test_client(context)
    app.authmodel(model, ["insert"])

    resp = app.post("/backends/postgres/dtypes/test/Entity", json={"id": 1})
    assert resp.status_code == 201, resp.json()

    # Get with default client auth
    resp = app.get("/backends/postgres/dtypes/test/Entity?select(id)", headers={"authorization": f"Bearer {token}"})
    assert resp.status_code == 200, resp.json()

    accesslog = context.get("accesslog.stream")
    assert len(accesslog) == 4
    assert accesslog[-2:] == [
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "type": "request",
            "time": accesslog[-2]["time"],
            "client": default_client_id,
            "method": "GET",
            "url": f"https://testserver/{model}?select(id)",
            "agent": "testclient",
            "format": "json",
            "action": "search",
            "model": model,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope", [["spinta_getall", "spinta_insert", "spinta_search"], ["uapi:/:getall", "uapi:/:create", "uapi:/:search"]]
)
def test_get_accesslog_not_default_user(
    manifest_type: str,
    tmp_path: pathlib.Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property      | type    | access
        backends/postgres/dtypes/test |         | open
          |   |   | Entity            |         |
          |   |   |   | id            | integer |
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    context.set("accesslog.stream", [])

    model = "backends/postgres/dtypes/test/Entity"
    app = create_test_client(context)
    app.authorize(scope, creds=("test-insert", "secret"))
    expected_scope = " ".join(sorted(scope))

    resp = app.post("/backends/postgres/dtypes/test/Entity", json={"id": 1})
    assert resp.status_code == 201, resp.json()

    # Get with test client
    resp = app.get("/backends/postgres/dtypes/test/Entity?select(id)")
    assert resp.status_code == 200, resp.json()

    accesslog = context.get("accesslog.stream")
    assert len(accesslog) == 5
    client = accesslog[-5]["client"]
    token = accesslog[-5]["token"]
    assert accesslog[-5:] == [
        {
            "agent": "testclient",
            "client": client,
            "format": "json",
            "method": "POST",
            "rctype": "application/x-www-form-urlencoded",
            "scope": expected_scope,
            "time": accesslog[-5]["time"],
            "token": token,
            "type": "auth",
            "url": "https://testserver/auth/token",
        },
        {
            "action": "insert",
            "agent": "testclient",
            "client": client,
            "format": "json",
            "method": "POST",
            "model": "backends/postgres/dtypes/test/Entity",
            "pid": accesslog[-4]["pid"],
            "rctype": "application/json",
            "time": accesslog[-4]["time"],
            "token": token,
            "txn": accesslog[-4]["txn"],
            "type": "request",
            "url": "https://testserver/backends/postgres/dtypes/test/Entity",
        },
        {
            "delta": accesslog[-3]["delta"],
            "memory": accesslog[-3]["memory"],
            "objects": 1,
            "time": accesslog[-3]["time"],
            "txn": accesslog[-4]["txn"],
            "type": "response",
        },
        {
            "txn": accesslog[-2]["txn"],
            "pid": accesslog[-2]["pid"],
            "token": token,
            "type": "request",
            "time": accesslog[-2]["time"],
            "client": client,
            "method": "GET",
            "url": "https://testserver/backends/postgres/dtypes/test/Entity?select(id)",
            "agent": "testclient",
            "format": "json",
            "action": "search",
            "model": model,
        },
        {
            "txn": accesslog[-2]["txn"],
            "type": "response",
            "time": accesslog[-1]["time"],
            "delta": accesslog[-1]["delta"],
            "memory": accesslog[-1]["memory"],
            "objects": 1,
        },
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize("scope", [["spinta_insert"], ["uapi:/:create"]])
def test_get_accesslog_scope_log_false(
    manifest_type: str,
    tmp_path: pathlib.Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property      | type    | access
        backends/postgres/dtypes/test |         | open
          |   |   | Entity            |         |
          |   |   |   | id            | integer |
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    context.set("accesslog.stream", [])

    # set config to not log scope
    context.get("config").scope_log = False

    model = "backends/postgres/dtypes/test/Entity"
    app = create_test_client(context)
    app.authorize(scope, creds=("test-insert", "secret"))

    resp = app.post("/backends/postgres/dtypes/test/Entity", json={"id": 1})
    assert resp.status_code == 201, resp.json()
