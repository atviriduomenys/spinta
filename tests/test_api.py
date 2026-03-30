from __future__ import annotations
import pathlib
import uuid
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import cast
from unittest.mock import ANY

import pytest

from spinta import commands
from spinta.auth import query_client, get_clients_path, get_keymap_path, create_client_file
from spinta.cli.helpers.store import _ensure_config_dir
from spinta.components import Context, Config
from spinta.core.config import RawConfig, configure_rc
from spinta.formats.html.components import Cell
from spinta.formats.html.helpers import short_id
from spinta.testing.client import TestClient, get_yaml_data
from spinta.testing.client import TestClientResponse
from spinta.testing.client import get_html_tree
from spinta.testing.context import create_test_context
from spinta.testing.utils import get_error_codes, error
from spinta.testing.manifest import prepare_manifest
from spinta.testing.data import pushdata
from spinta.utils.nestedstruct import flatten
from spinta.testing.client import create_test_client
from spinta.backends.memory.components import Memory
from spinta.testing.data import send
from spinta.utils.types import is_str_uuid


def clients_url() -> str:
    return "/auth/clients"


def client_url(client_id: str) -> str:
    return f"/auth/clients/{client_id}"


def create_client(
    config: Config,
    client_id: uuid.UUID | None = None,
    name: str | None = None,
    secret: str = "test_secret",
    scopes: list[str] | None = None,
) -> dict:
    client_id = str(client_id or uuid.uuid4())
    name = name or client_id

    _, client = create_client_file(get_clients_path(config), name, client_id, secret, scopes)

    return {
        "client_id": client["client_id"],
        "client_name": client["client_name"],
        "scopes": client["scopes"],
    }


def _cleaned_context(resp: TestClientResponse, *, data: bool = True, remove_page: bool = True) -> Dict[str, Any]:
    context = resp.context.copy()
    page_index = None
    if remove_page:
        if "header" in context and "_page" in context["header"]:
            page_index = context["header"].index("_page")
            context["header"].remove("_page")

    if "data" in context:
        context["data"] = [
            [cell.as_dict() for i, cell in enumerate(row) if not remove_page or page_index is None or i != page_index]
            for row in cast(List[List[Cell]], resp.context["data"])
        ]
    if "row" in context:
        context["row"] = [
            (name, cell.as_dict())
            for name, cell in cast(Tuple[str, Cell], context["row"])
            if not remove_page or name != "_page"
        ]

    if data:
        header = context["header"]
        context["data"] = [
            {k: v["value"] for k, v in zip(header, d)} for d in cast(List[List[Dict[str, Any]]], context["data"])
        ]
    del context["params"]
    del context["zip"]
    if "request" in context:
        del context["request"]
    return context


def ensure_temp_context_and_app(rc: RawConfig, tmp_path: pathlib.Path) -> Tuple[Context, TestClient]:
    temp_rc = configure_rc(
        rc.fork(
            {
                "config_path": str(tmp_path),
            }
        ),
        None,
    )
    context = create_test_context(temp_rc)
    config = context.get("config")
    commands.load(context, config)
    _ensure_config_dir(
        config,
        config.config_path,
        config.default_auth_client,
    )
    app = create_test_client(context)
    context.set("client", app)
    return context, context.get("client")


def test_version(app):
    resp = app.get("/version")
    assert resp.status_code == 200
    assert sorted(next(flatten(resp.json())).keys()) == [
        "dsa.version",
        "implementation.name",
        "implementation.version",
        "uapi.version",
    ]


def test_app(app):
    app.authorize(["uapi:/:getall"])
    resp = app.get("/", headers={"accept": "text/html"})
    assert resp.status_code == 200, resp.text

    resp.context.pop("request")
    data = _cleaned_context(resp)
    assert data == {
        "location": [
            ("🏠", "/"),
        ],
        "empty": False,
        "header": ["name", "title", "description"],
        "data": data["data"],
        "formats": [
            ("CSV", "/:format/csv"),
            ("JSON", "/:format/json"),
            ("JSONL", "/:format/jsonl"),
            ("ASCII", "/:format/ascii"),
            ("RDF", "/:format/rdf"),
        ],
    }
    assert next(d for d in data["data"] if d["title"] == "Country") == {
        "name": "📄 Country",
        "title": "Country",
        "description": "",
    }

    html = get_html_tree(resp)
    rows = html.cssselect("table.table tr td:nth-child(1)")
    rows = [row.text_content().strip() for row in rows]
    assert "📄 Country" in rows
    assert "📁 datasets/" in rows


def test_directory(app):
    app.authorize(["uapi:/datasets/:getall"])
    resp = app.get("/datasets/xlsx/Rinkimai/:ns", headers={"accept": "text/html"})
    assert resp.status_code == 200

    resp.context.pop("request")
    assert _cleaned_context(resp, data=False) == {
        "location": [
            ("🏠", "/"),
            ("datasets", "/datasets"),
            ("xlsx", "/datasets/xlsx"),
            ("Rinkimai", None),
        ],
        "empty": False,
        "header": ["name", "title", "description"],
        "data": [
            [
                {"value": "📄 Apygarda", "link": "/datasets/xlsx/Rinkimai/Apygarda"},
                {"value": "", "color": "#f5f5f5"},
                {"value": "", "color": "#f5f5f5"},
            ],
            [
                {"value": "📄 Apylinke", "link": "/datasets/xlsx/Rinkimai/Apylinke"},
                {"value": "", "color": "#f5f5f5"},
                {"value": "", "color": "#f5f5f5"},
            ],
            [
                {"value": "📄 Kandidatas", "link": "/datasets/xlsx/Rinkimai/Kandidatas"},
                {"value": "", "color": "#f5f5f5"},
                {"value": "", "color": "#f5f5f5"},
            ],
            [
                {"value": "📄 Turas", "link": "/datasets/xlsx/Rinkimai/Turas"},
                {"value": "", "color": "#f5f5f5"},
                {"value": "", "color": "#f5f5f5"},
            ],
        ],
        "formats": [
            ("CSV", "/datasets/xlsx/Rinkimai/:ns/:format/csv"),
            ("JSON", "/datasets/xlsx/Rinkimai/:ns/:format/json"),
            ("JSONL", "/datasets/xlsx/Rinkimai/:ns/:format/jsonl"),
            ("ASCII", "/datasets/xlsx/Rinkimai/:ns/:format/ascii"),
            ("RDF", "/datasets/xlsx/Rinkimai/:ns/:format/rdf"),
        ],
    }


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_model(model, context, app):
    app.authmodel(model, ["insert", "getall"])
    resp = app.post(
        model,
        json={
            "_data": [
                {
                    "_op": "insert",
                    "_type": model,
                    "status": "ok",
                    "count": 42,
                },
            ]
        },
    )
    (row,) = resp.json()["_data"]

    resp = app.get(f"/{model}", headers={"accept": "text/html"})
    assert resp.status_code == 200

    resp.context.pop("request")

    backend = model[len("backends/") : len(model) - len("/Report")]
    assert _cleaned_context(resp, data=False) == {
        "location": [
            ("🏠", "/"),
            ("backends", "/backends"),
            (backend, f"/backends/{backend}"),
            ("Report", None),
            ("Changes", f"/{model}/:changes/-10"),
        ],
        "empty": False,
        "header": [
            "_id",
            "report_type",
            "status",
            "valid_from_date",
            "update_time",
            "count",
            "notes[].note",
            "notes[].note_type",
            "notes[].create_date",
            "operating_licenses[].license_types[]",
            "sync.sync_revision",
            "sync.sync_resources[].sync_id",
            "sync.sync_resources[].sync_source",
        ],
        "data": [
            [
                {"value": row["_id"][:8], "link": f"/{model}/%s" % row["_id"]},
                {"value": "", "color": "#f5f5f5"},
                {"value": "ok"},
                {"value": "", "color": "#f5f5f5"},
                {"value": "", "color": "#f5f5f5"},
                {"value": 42},
                {"value": "", "color": "#f5f5f5"},
                {"value": "", "color": "#f5f5f5"},
                {"value": "", "color": "#f5f5f5"},
                {"value": "", "color": "#f5f5f5"},
                {"value": "", "color": "#f5f5f5"},
                {"value": "", "color": "#f5f5f5"},
                {"value": "", "color": "#f5f5f5"},
            ],
        ],
        "formats": [
            ("CSV", f"/{model}/:format/csv"),
            ("JSON", f"/{model}/:format/json"),
            ("JSONL", f"/{model}/:format/jsonl"),
            ("ASCII", f"/{model}/:format/ascii"),
            ("RDF", f"/{model}/:format/rdf"),
        ],
    }


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_model_get(model, app):
    app.authmodel(model, ["insert", "getone"])
    resp = app.post(
        model,
        json={
            "_data": [
                {
                    "_op": "insert",
                    "_type": model,
                    "status": "ok",
                    "count": 42,
                },
            ]
        },
    )
    (row,) = resp.json()["_data"]

    _id = row["_id"]
    resp = app.get(f"/{model}/{_id}", headers={"accept": "text/html"})
    assert resp.status_code == 200

    resp.context.pop("request")
    backend = model[len("backends/") : len(model) - len("/report")]
    assert _cleaned_context(resp, data=True) == {
        "location": [
            ("🏠", "/"),
            ("backends", "/backends"),
            (backend, f"/backends/{backend}"),
            ("Report", f"/{model}"),
            (_id[:8], None),
            ("Changes", f"/{model}/%s/:changes/-10" % _id),
        ],
        "header": [
            "_type",
            "_id",
            "_revision",
            "report_type",
            "status",
            "valid_from_date",
            "update_time",
            "count",
            "notes[].note",
            "notes[].note_type",
            "notes[].create_date",
            "operating_licenses[].license_types[]",
            "sync.sync_revision",
            "sync.sync_resources[].sync_id",
            "sync.sync_resources[].sync_source",
        ],
        "data": [
            {
                "_type": model,
                "_id": short_id(_id),
                "_revision": row["_revision"],
                "count": 42,
                "notes[].note": "",
                "notes[].note_type": "",
                "notes[].create_date": "",
                "operating_licenses[].license_types[]": "",
                "report_type": "",
                "status": "ok",
                "sync.sync_resources[].sync_id": "",
                "sync.sync_resources[].sync_source": "",
                "sync.sync_revision": "",
                "update_time": "",
                "valid_from_date": "",
            }
        ],
        "empty": False,
        "formats": [
            ("CSV", f"/{model}/%s/:format/csv" % row["_id"]),
            ("JSON", f"/{model}/%s/:format/json" % row["_id"]),
            ("JSONL", f"/{model}/%s/:format/jsonl" % row["_id"]),
            ("ASCII", f"/{model}/%s/:format/ascii" % row["_id"]),
            ("RDF", f"/{model}/%s/:format/rdf" % row["_id"]),
        ],
    }


def test_dataset(app):
    app.authmodel("datasets/json/Rinkimai", ["insert", "getall"])
    resp = app.post(
        "/datasets/json/Rinkimai",
        json={
            "id": "1",
            "pavadinimas": "Rinkimai 1",
        },
    )
    data = resp.json()
    assert resp.status_code == 201, data

    resp = app.get("/datasets/json/Rinkimai", headers={"accept": "text/html"})
    assert resp.status_code == 200, resp.json()

    pk = data["_id"]
    resp.context.pop("request")
    assert _cleaned_context(resp, data=False) == {
        "location": [
            ("🏠", "/"),
            ("datasets", "/datasets"),
            ("json", "/datasets/json"),
            ("Rinkimai", None),
            ("Changes", "/datasets/json/Rinkimai/:changes/-10"),
        ],
        "header": ["_id", "id", "pavadinimas"],
        "empty": False,
        "data": [
            [
                {"link": f"/datasets/json/Rinkimai/{pk}", "value": pk[:8]},
                {"value": "1"},
                {"value": "Rinkimai 1"},
            ],
        ],
        "formats": [
            ("CSV", "/datasets/json/Rinkimai/:format/csv"),
            ("JSON", "/datasets/json/Rinkimai/:format/json"),
            ("JSONL", "/datasets/json/Rinkimai/:format/jsonl"),
            ("ASCII", "/datasets/json/Rinkimai/:format/ascii"),
            ("RDF", "/datasets/json/Rinkimai/:format/rdf"),
        ],
    }


@pytest.mark.parametrize(
    "select_syntax",
    [
        "select(pavadinimas)",
        "_select=pavadinimas",
    ],
)
def test_dataset_with_show(context, app, select_syntax):
    app.authmodel("/datasets/json/Rinkimai", ["insert", "search"])

    resp = app.post(
        "/datasets/json/Rinkimai",
        json={
            "id": "1",
            "pavadinimas": "Rinkimai 1",
        },
    )
    data = resp.json()
    assert resp.status_code == 201, data

    resp = app.get(
        f"/datasets/json/Rinkimai?{select_syntax}",
        headers={
            "accept": "text/html",
        },
    )
    assert resp.status_code == 200

    context = _cleaned_context(resp)
    assert context["header"] == ["pavadinimas"]
    assert context["data"] == [
        {
            "pavadinimas": "Rinkimai 1",
        }
    ]


def test_dataset_url_without_resource(context, app):
    app.authmodel("datasets/json/Rinkimai", ["insert", "getall"])
    resp = app.post(
        "/datasets/json/Rinkimai",
        json={
            "id": "1",
            "pavadinimas": "Rinkimai 1",
        },
    )
    data = resp.json()
    pk = data["_id"]
    assert resp.status_code == 201, data
    resp = app.get("/datasets/json/Rinkimai", headers={"accept": "text/html"})
    assert resp.status_code == 200
    context = _cleaned_context(resp, data=False)
    assert context["header"] == ["_id", "id", "pavadinimas"]
    assert context["data"] == [
        [
            {"link": f"/datasets/json/Rinkimai/{pk}", "value": pk[:8]},
            {"value": "1"},
            {"value": "Rinkimai 1"},
        ]
    ]


def test_nested_dataset(app):
    app.authmodel("datasets/nested/dataset/name/Model", ["insert", "getall"])
    resp = app.post(
        "/datasets/nested/dataset/name/Model",
        json={
            "name": "Nested One",
        },
    )
    data = resp.json()
    assert resp.status_code == 201, data
    pk = data["_id"]

    resp = app.get("/datasets/nested/dataset/name/Model", headers={"accept": "text/html"})
    assert resp.status_code == 200

    resp.context.pop("request")
    assert _cleaned_context(resp, data=False) == {
        "location": [
            ("🏠", "/"),
            ("datasets", "/datasets"),
            ("nested", "/datasets/nested"),
            ("dataset", "/datasets/nested/dataset"),
            ("name", "/datasets/nested/dataset/name"),
            ("Model", None),
            ("Changes", "/datasets/nested/dataset/name/Model/:changes/-10"),
        ],
        "header": ["_id", "name"],
        "empty": False,
        "data": [
            [
                {"link": f"/datasets/nested/dataset/name/Model/{pk}", "value": pk[:8]},
                {"value": "Nested One"},
            ],
        ],
        "formats": [
            ("CSV", "/datasets/nested/dataset/name/Model/:format/csv"),
            ("JSON", "/datasets/nested/dataset/name/Model/:format/json"),
            ("JSONL", "/datasets/nested/dataset/name/Model/:format/jsonl"),
            ("ASCII", "/datasets/nested/dataset/name/Model/:format/ascii"),
            ("RDF", "/datasets/nested/dataset/name/Model/:format/rdf"),
        ],
    }


def test_dataset_key(app):
    app.authmodel("datasets/json/Rinkimai", ["insert", "getone"])
    resp = app.post(
        "/datasets/json/Rinkimai",
        json={
            "id": "1",
            "pavadinimas": "Rinkimai 1",
        },
    )
    data = resp.json()
    assert resp.status_code == 201, data
    pk = data["_id"]
    rev = data["_revision"]

    resp = app.get(f"/datasets/json/Rinkimai/{pk}", headers={"accept": "text/html"})
    assert resp.status_code == 200

    resp.context.pop("request")
    assert _cleaned_context(resp, data=False) == {
        "location": [
            ("🏠", "/"),
            ("datasets", "/datasets"),
            ("json", "/datasets/json"),
            ("Rinkimai", "/datasets/json/Rinkimai"),
            (short_id(pk), None),
            ("Changes", f"/datasets/json/Rinkimai/{pk}/:changes/-10"),
        ],
        "formats": [
            ("CSV", f"/datasets/json/Rinkimai/{pk}/:format/csv"),
            ("JSON", f"/datasets/json/Rinkimai/{pk}/:format/json"),
            ("JSONL", f"/datasets/json/Rinkimai/{pk}/:format/jsonl"),
            ("ASCII", f"/datasets/json/Rinkimai/{pk}/:format/ascii"),
            ("RDF", f"/datasets/json/Rinkimai/{pk}/:format/rdf"),
        ],
        "header": [
            "_type",
            "_id",
            "_revision",
            "id",
            "pavadinimas",
        ],
        "empty": False,
        "data": [
            [
                {"value": "datasets/json/Rinkimai"},
                {"value": short_id(pk), "link": f"/datasets/json/Rinkimai/{pk}"},
                {"value": rev},
                {"value": "1"},
                {"value": "Rinkimai 1"},
            ]
        ],
    }


def test_changes_single_object(app: TestClient, mocker):
    app.authmodel("datasets/json/Rinkimai", ["insert", "patch", "changes"])

    model = "datasets/json/Rinkimai"

    obj = send(
        app,
        model,
        "insert",
        {
            "id": "1",
            "pavadinimas": "Rinkimai 1",
        },
    )
    obj1 = send(
        app,
        model,
        "patch",
        obj,
        {
            "id": "1",
            "pavadinimas": "Rinkimai 2",
        },
    )

    resp = app.get(
        f"/datasets/json/Rinkimai/{obj.id}/:changes/-10",
        headers={
            "accept": "text/html",
        },
    )
    assert resp.status_code == 200

    change = _cleaned_context(resp)["data"]
    assert _cleaned_context(resp, data=False) == {
        "location": [
            ("🏠", "/"),
            ("datasets", "/datasets"),
            ("json", "/datasets/json"),
            ("Rinkimai", "/datasets/json/Rinkimai"),
            (obj1.sid, f"/datasets/json/Rinkimai/{obj.id}"),
            ("Changes", None),
        ],
        "formats": [
            ("CSV", f"/{model}/{obj.id}/:changes/-10/:format/csv?limit(100)"),
            ("JSON", f"/{model}/{obj.id}/:changes/-10/:format/json?limit(100)"),
            ("JSONL", f"/{model}/{obj.id}/:changes/-10/:format/jsonl?limit(100)"),
            ("ASCII", f"/{model}/{obj.id}/:changes/-10/:format/ascii?limit(100)"),
            ("RDF", f"/{model}/{obj.id}/:changes/-10/:format/rdf?limit(100)"),
        ],
        "header": [
            "_cid",
            "_created",
            "_op",
            "_id",
            "_txn",
            "_revision",
            "_same_as",
            "id",
            "pavadinimas",
        ],
        "empty": False,
        "data": [
            [
                {"value": 1},
                {"value": change[0]["_created"]},
                {"value": "insert"},
                {"value": obj.sid, "link": f"/{model}/{obj.id}"},
                {"value": change[0]["_txn"]},
                {"value": obj.rev},
                {"value": "", "color": "#f5f5f5"},
                {"value": "1"},
                {"value": "Rinkimai 1"},
            ],
            [
                {"value": 2},
                {"value": change[1]["_created"]},
                {"value": "patch"},
                {"value": obj.sid, "link": f"/datasets/json/Rinkimai/{obj.id}"},
                {"value": change[1]["_txn"]},
                {"value": obj1.rev},
                {"value": "", "color": "#f5f5f5"},
                {"value": "", "color": "#f5f5f5"},
                {"value": "Rinkimai 2"},
            ],
        ],
    }


def test_changes_object_list(app, mocker):
    app.authmodel("datasets/json/Rinkimai", ["insert", "patch", "changes"])

    model = "datasets/json/Rinkimai"

    obj = send(
        app,
        model,
        "insert",
        {
            "id": "1",
            "pavadinimas": "Rinkimai 1",
        },
    )
    obj1 = send(
        app,
        model,
        "patch",
        obj,
        {
            "id": "1",
            "pavadinimas": "Rinkimai 2",
        },
    )

    resp = app.get("/datasets/json/Rinkimai/:changes/-10", headers={"accept": "text/html"})
    assert resp.status_code == 200

    change = _cleaned_context(resp)["data"]
    assert _cleaned_context(resp, data=False) == {
        "location": [
            ("🏠", "/"),
            ("datasets", "/datasets"),
            ("json", "/datasets/json"),
            ("Rinkimai", f"/{model}"),
            ("Changes", None),
        ],
        "formats": [
            ("CSV", f"/{model}/:changes/-10/:format/csv?limit(100)"),
            ("JSON", f"/{model}/:changes/-10/:format/json?limit(100)"),
            ("JSONL", f"/{model}/:changes/-10/:format/jsonl?limit(100)"),
            ("ASCII", f"/{model}/:changes/-10/:format/ascii?limit(100)"),
            ("RDF", f"/{model}/:changes/-10/:format/rdf?limit(100)"),
        ],
        "header": [
            "_cid",
            "_created",
            "_op",
            "_id",
            "_txn",
            "_revision",
            "_same_as",
            "id",
            "pavadinimas",
        ],
        "empty": False,
        "data": [
            [
                {"value": 1},
                {"value": change[0]["_created"]},
                {"value": "insert"},
                {"link": f"/{model}/{obj.id}", "value": obj.sid},
                {"value": change[0]["_txn"]},
                {"value": obj.rev},
                {"value": "", "color": "#f5f5f5"},
                {"value": "1"},
                {"value": "Rinkimai 1"},
            ],
            [
                {"value": 2},
                {"value": change[1]["_created"]},
                {"value": "patch"},
                {"link": f"/{model}/{obj.id}", "value": obj.sid},
                {"value": change[1]["_txn"]},
                {"value": obj1.rev},
                {"value": "", "color": "#f5f5f5"},
                {"value": "", "color": "#f5f5f5"},
                {"value": "Rinkimai 2"},
            ],
        ],
    }


@pytest.mark.parametrize(
    "count_syntax",
    [
        "count()",
        "_count",
    ],
)
def test_count(app, count_syntax):
    app.authmodel("/datasets/json/Rinkimai", ["upsert", "search"])

    resp = app.post(
        "/datasets/json/Rinkimai",
        json={
            "_data": [
                {
                    "_op": "upsert",
                    "_type": "datasets/json/Rinkimai",
                    "_where": 'id="1"',
                    "id": "1",
                    "pavadinimas": "Rinkimai 1",
                },
                {
                    "_op": "upsert",
                    "_type": "datasets/json/Rinkimai",
                    "_where": 'id="2"',
                    "id": "2",
                    "pavadinimas": "Rinkimai 2",
                },
            ]
        },
    )
    # FIXME: Status code on multiple objects must be 207.
    assert resp.status_code == 200, resp.json()

    # Backwards compatibility support
    resp = app.get(f"/datasets/json/Rinkimai?{count_syntax}", headers={"accept": "text/html"})
    assert resp.status_code == 200

    resp = app.get(f"/datasets/json/Rinkimai?select({count_syntax})", headers={"accept": "text/html"})
    assert resp.status_code == 200

    context = _cleaned_context(resp)
    assert context["data"] == [{"count()": 2}]


@pytest.mark.parametrize(
    "select_syntax",
    [
        "select(_type,count())",
        "select(_type,_count)",
        "_select=_type, count()",
        "_select=_type,_count",
    ],
)
def test_select_with_function(app, select_syntax):
    app.authmodel("/datasets/json/Rinkimai", ["upsert", "search"])

    resp = app.post(
        "/datasets/json/Rinkimai",
        json={
            "_data": [
                {
                    "_op": "upsert",
                    "_type": "datasets/json/Rinkimai",
                    "_where": 'id="1"',
                    "id": "1",
                    "pavadinimas": "Rinkimai 1",
                },
                {
                    "_op": "upsert",
                    "_type": "datasets/json/Rinkimai",
                    "_where": 'id="2"',
                    "id": "2",
                    "pavadinimas": "Rinkimai 2",
                },
            ]
        },
    )
    # FIXME: Status code on multiple objects must be 207.
    assert resp.status_code == 200, resp.json()

    # Backwards compatibility support
    resp = app.get(f"/datasets/json/Rinkimai/:format/json?{select_syntax}", headers={"accept": "text/html"})
    assert resp.status_code == 200, resp.status_code

    context = resp.json()
    assert context["_data"] == [{"_type": "datasets/json/Rinkimai", "count()": 2}], context["_data"]


@pytest.mark.parametrize(
    "sort_syntax, expected_titles",
    [
        ("sort(pavadinimas)", ["Apskričių rinkimai", "Savivaldybių Rinkimai"]),
        ("_sort=pavadinimas", ["Apskričių rinkimai", "Savivaldybių Rinkimai"]),
        ("sort(-pavadinimas)", ["Savivaldybių Rinkimai", "Apskričių rinkimai"]),
        ("_sort=-pavadinimas", ["Savivaldybių Rinkimai", "Apskričių rinkimai"]),
    ],
)
def test_sort(app, sort_syntax, expected_titles):
    app.authmodel("/datasets/json/Rinkimai", ["upsert", "search"])

    resp = app.post(
        "/datasets/json/Rinkimai",
        json={
            "_data": [
                {
                    "_op": "upsert",
                    "_type": "datasets/json/Rinkimai",
                    "_where": 'id="1"',
                    "id": "1",
                    "pavadinimas": "Savivaldybių Rinkimai",
                },
                {
                    "_op": "upsert",
                    "_type": "datasets/json/Rinkimai",
                    "_where": 'id="2"',
                    "id": "2",
                    "pavadinimas": "Apskričių rinkimai",
                },
            ]
        },
    )
    assert resp.status_code == 200, resp.json()
    resp = app.get(
        f"/datasets/json/Rinkimai/:format/json?_select=pavadinimas&{sort_syntax}", headers={"accept": "text/html"}
    )

    context = resp.json()
    titles = list(data["pavadinimas"] for data in context["_data"])
    assert titles == expected_titles


@pytest.mark.parametrize(
    "limit_syntax, expected_count",
    [
        ("limit(1)", 1),
        ("limit(2)", 2),
        ("_limit=1", 1),
        ("_limit=2", 2),
    ],
)
def test_limit(app, limit_syntax, expected_count):
    app.authmodel("/datasets/json/Rinkimai", ["upsert", "search"])

    resp = app.post(
        "/datasets/json/Rinkimai",
        json={
            "_data": [
                {
                    "_op": "upsert",
                    "_type": "datasets/json/Rinkimai",
                    "_where": 'id="1"',
                    "id": "1",
                    "pavadinimas": "Savivaldybių Rinkimai",
                },
                {
                    "_op": "upsert",
                    "_type": "datasets/json/Rinkimai",
                    "_where": 'id="2"',
                    "id": "2",
                    "pavadinimas": "Apskričių rinkimai",
                },
                {
                    "_op": "upsert",
                    "_type": "datasets/json/Rinkimai",
                    "_where": 'id="3"',
                    "id": "2",
                    "pavadinimas": "Kaimų rinkimai",
                },
            ]
        },
    )
    assert resp.status_code == 200, resp.json()
    resp = app.get(f"/datasets/json/Rinkimai/:format/json?{limit_syntax}", headers={"accept": "text/html"})

    context = resp.json()
    assert len(context["_data"]) == expected_count


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_post(model, context, app):
    app.authmodel(model, ["insert", "getone"])

    # tests basic object creation
    resp = app.post(
        f"/{model}",
        json={
            "status": "ok",
            "count": 42,
        },
    )
    assert resp.status_code == 201
    data = resp.json()
    id_ = data["_id"]
    revision = data["_revision"]
    assert uuid.UUID(id_).version == 4
    assert data == {
        "_type": model,
        "_id": id_,
        "_revision": revision,
        "report_type": None,
        "status": "ok",
        "valid_from_date": None,
        "update_time": None,
        "count": 42,
        "notes": [],
        "operating_licenses": [],
        "sync": {
            "sync_revision": None,
            "sync_resources": [],
        },
    }

    resp = app.get(f"/{model}/{id_}")
    assert resp.status_code == 200
    assert resp.json() == {
        "_type": model,
        "_id": id_,
        "_revision": revision,
        "report_type": None,
        "status": "ok",
        "valid_from_date": None,
        "update_time": None,
        "count": 42,
        "notes": [],
        "operating_licenses": [],
        "sync": {
            "sync_revision": None,
            "sync_resources": [],
        },
    }


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_post_invalid_json(model, context, app):
    # tests 400 response on invalid json
    app.authmodel(model, ["insert"])
    headers = {"content-type": "application/json"}
    resp = app.post(
        f"/{model}",
        headers=headers,
        content="""{
        "status": "ok",
        "count": 42
    ]""",
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["JSONError"]


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_post_empty_content(model, context, app):
    # tests posting empty content
    app.authmodel(model, ["insert"])
    headers = {
        "content-length": "0",
        "content-type": "application/json",
    }
    resp = app.post(f"/{model}", headers=headers, json=None)
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["JSONError"]


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_post_id(model, context, app):
    # tests 400 response when trying to create object with id
    app.authmodel(model, ["insert"])

    # XXX: there's a funny thing that id value is loaded/validated first
    # before it's checked that user has correct scope
    resp = app.post(
        f"/{model}",
        json={
            "_id": "0007ddec-092b-44b5-9651-76884e6081b4",
            "status": "ok",
            "count": 42,
        },
    )
    assert resp.status_code == 403
    assert get_error_codes(resp.json()) == ["InsufficientScopeError"]
    assert "www-authenticate" in resp.headers
    assert resp.headers["www-authenticate"] == 'Bearer error="insufficient_scope"'


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_insufficient_scope(model, context, app):
    # tests 400 response when trying to create object with id
    app.authmodel(model, ["getone"])

    # XXX: there's a funny thing that id value is loaded/validated first
    # before it's checked that user has correct scope
    resp = app.post(
        f"/{model}",
        json={
            "status": "ok",
            "count": 42,
        },
    )
    assert resp.status_code == 403
    assert get_error_codes(resp.json()) == ["InsufficientScopeError"]
    assert "www-authenticate" in resp.headers
    assert resp.headers["www-authenticate"] == 'Bearer error="insufficient_scope"'


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_post_update_postgres(model, context, app):
    # tests if update works with `id` present in the json
    app.authorize(["uapi:/:set_meta_fields"])
    app.authmodel(model, ["insert"])
    resp = app.post(
        f"/{model}",
        json={
            "_id": "0007ddec-092b-44b5-9651-76884e6081b4",
            "status": "ok",
            "count": 42,
        },
    )

    assert resp.status_code == 201
    assert resp.json()["_id"] == "0007ddec-092b-44b5-9651-76884e6081b4"

    # POST invalid id
    resp = app.post(
        f"/{model}",
        json={
            "_id": 123,
            "status": "not-ok",
            "count": 13,
        },
    )

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    # POST invalid id
    resp = app.post(
        f"/{model}",
        json={
            "_id": "123",
            "status": "not-ok",
            "count": 13,
        },
    )

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    # POST invalid id
    resp = app.post(
        f"/{model}",
        json={
            "_id": None,
            "status": "not-ok",
            "count": 13,
        },
    )

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_post_revision(model, context, app):
    # tests 400 response when trying to create object with revision
    app.authmodel(model, ["insert"])
    resp = app.post(
        f"/{model}",
        json={
            "_revision": "r3v1510n",
            "status": "valid",
            "count": 42,
        },
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["ManagedProperty"]


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_post_duplicate_id(model, app):
    # tests 400 response when trying to create object with id which exists
    app.authorize(["uapi:/:set_meta_fields"])
    app.authmodel(model, ["insert"])
    resp = app.post(
        f"/{model}",
        json={
            "status": "valid",
            "count": 42,
        },
    )
    assert resp.status_code == 201
    data = resp.json()
    id_ = data["_id"]

    resp = app.post(
        f"/{model}",
        json={
            "_id": id_,
            "status": "valid",
            "count": 42,
        },
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["UniqueConstraint"]


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_patch_duplicate_id(model, context, app):
    # tests that duplicate ID detection works with PATCH requests
    app.authmodel(model, ["insert", "getone", "patch"])
    app.authorize(["uapi:/:set_meta_fields"])

    # create extra report
    resp = app.post(
        f"/{model}",
        json={
            "_type": "Report",
            "status": "1",
        },
    )
    assert resp.status_code == 201

    data = app.post(
        f"/{model}",
        json={
            "_type": "Report",
            "status": "1",
        },
    ).json()
    id_ = data["_id"]
    new_id = "0007ddec-092b-44b5-9651-76884e6081b4"
    # try to PATCH id with set_meta_fields scope
    # this should be successful because id that we want to setup
    # does not exist in database
    resp = app.patch(
        f"/{model}/{id_}",
        json={
            "_id": new_id,
            "_revision": data["_revision"],
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    id_ = data["_id"]
    assert id_ == new_id

    # try to patch report with id of itself
    resp = app.patch(
        f"/{model}/{id_}",
        json={
            "_id": new_id,
            "_revision": data["_revision"],
        },
    )
    assert resp.status_code == 200
    assert resp.json()["_id"] == new_id

    # try to PATCH id with set_meta_fields scope
    # this should not be successful because id that we want to setup
    # already exists in database
    resp = app.post(
        f"/{model}",
        json={
            "_type": "Report",
            "status": "1",
        },
    )
    existing_id = resp.json()["_id"]

    resp = app.patch(
        f"/{model}/{id_}",
        json={
            "_id": existing_id,
            "_revision": data["_revision"],
        },
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["UniqueConstraint"]


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_post_non_json_content_type(model, app):
    # tests 400 response when trying to make non-json request
    app.authmodel(model, ["insert"])
    headers = {"content-type": "application/text"}
    resp = app.post(
        f"/{model}",
        headers=headers,
        json={
            "status": "valid",
            "count": 42,
        },
    )
    assert resp.status_code == 415
    assert get_error_codes(resp.json()) == ["UnknownContentType"]


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_post_bad_auth_header(model, app):
    # tests 400 response when authorization header is missing `Bearer `
    auth_header = {"authorization": "Fail f00b4rb4z"}
    resp = app.post(
        f"/{model}",
        headers=auth_header,
        json={
            "status": "valid",
            "count": 42,
        },
    )
    assert resp.status_code == 401
    assert get_error_codes(resp.json()) == ["UnsupportedTokenTypeError"]


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_post_missing_auth_header(model, context, app, mocker):
    mocker.patch.object(context.get("config"), "default_auth_client", None)
    resp = app.post(
        f"/{model}",
        json={
            "status": "valid",
            "count": 42,
        },
    )
    assert resp.status_code == 401
    assert get_error_codes(resp.json()) == ["MissingAuthorizationError"]


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_post_invalid_report_schema(model, app):
    # tests validation of correct value types according to manifest's schema
    app.authmodel(model, ["insert", "getone"])

    resp = app.post(
        f"/{model}",
        json={
            "count": False,
        },
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    resp = app.post(
        f"/{model}",
        json={
            "count": [1, 2, 3],
        },
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    resp = app.post(
        f"/{model}",
        json={
            "count": {"a": 1, "b": 2},
        },
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    resp = app.post(
        f"/{model}",
        json={
            "count": 123,
        },
    )
    assert resp.status_code == 201

    # sanity check, that integers are still allowed.
    resp = app.post(
        f"/{model}",
        json={
            "status": 42,
        },
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    # test string validation
    resp = app.post(
        f"/{model}",
        json={
            "status": True,
        },
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    resp = app.post(
        f"/{model}",
        json={
            "status": [1, 2, 3],
        },
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    resp = app.post(
        f"/{model}",
        json={
            "status": {"a": 1, "b": 2},
        },
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    # sanity check, that strings are still allowed.
    resp = app.post(
        f"/{model}",
        json={
            "status": "42",
        },
    )
    assert resp.status_code == 201


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_streaming_response(model, app):
    app.authmodel(model, ["insert", "getall"])
    resp = app.post(
        f"/{model}",
        json={
            "_data": [
                {
                    "_op": "insert",
                    "_type": model,
                    "count": 42,
                    "status": "ok",
                },
                {
                    "_op": "insert",
                    "_type": model,
                    "count": 13,
                    "status": "not-ok",
                },
            ]
        },
    )
    # FIXME: Should be 207 status code.
    assert resp.status_code == 200, resp.json()

    resp = app.get(f"/{model}").json()
    data = resp["_data"]
    data = sorted((x["count"], x["status"]) for x in data)
    assert data == [
        (13, "not-ok"),
        (42, "ok"),
    ]


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_multi_backends(model, app):
    app.authmodel(model, ["insert", "getone", "getall", "search"])
    resp = app.post(f"/{model}", json={"status": "42"})
    assert resp.status_code == 201
    data = resp.json()
    id_ = data["_id"]
    revision = data["_revision"]

    # return the object on GETONE
    resp = app.get(f"/{model}/{id_}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["_id"] == id_
    assert data["_revision"] == revision
    assert data["_type"] == model
    assert data["status"] == "42"

    # check that query parameters work on GETONE
    resp = app.get(f"/{model}/{id_}?select(status)")
    assert resp.status_code == 200
    assert resp.json() == {
        "status": "42",
    }

    # return the objects on GETALL
    resp = app.get(f"/{model}")
    assert resp.status_code == 200
    assert len(resp.json()["_data"]) == 1
    data = resp.json()["_data"][0]
    assert data["_id"] == id_
    assert data["_revision"] == revision
    assert data["_type"] == model
    assert data["status"] == "42"

    # check that query parameters work on GETALL
    resp = app.get(f"/{model}?select(status)")
    assert resp.status_code == 200
    assert resp.json()["_data"] == [
        {
            "status": "42",
        }
    ]


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_location_header(model, app, context):
    app.authmodel(model, ["insert"])
    resp = app.post(f"/{model}", json={"status": "42"})
    assert resp.status_code == 201
    assert "location" in resp.headers
    id_ = resp.json()["_id"]
    server_url = context.get("config").server_url
    assert resp.headers["location"] == f"{server_url}{model}/{id_}"


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_upsert_where_ast(model, app):
    app.authmodel(model, ["upsert", "changes"])

    payload = {
        "_op": "upsert",
        "_where": {
            "name": "eq",
            "args": [
                {"name": "bind", "args": ["status"]},
                "ok",
            ],
        },
        "status": "ok",
        "valid_from_date": "2021-03-31",
    }

    resp = app.post("/" + model, json=payload)
    data = resp.json()
    assert resp.status_code == 201, data
    rev = data["_revision"]

    resp = app.post("/" + model, json=payload)
    data = resp.json()
    assert resp.status_code == 200, data
    assert data["_revision"] == rev


def test_robots(app: TestClient):
    resp = app.get("/robots.txt")
    assert resp.status_code == 200
    assert resp.text == "User-Agent: *\nAllow: /\n"


@pytest.mark.parametrize(
    "path",
    [
        "example/City",
        "example/City/19e4f199-93c5-40e5-b04e-a575e81ac373",
        "example/City/:changes",
        "example/City/19e4f199-93c5-40e5-b04e-a575e81ac373/:changes",
    ],
)
def test_head_method(
    rc: RawConfig,
    path: str,
):
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

    db: Memory = manifest.backend
    db.insert(
        {
            "_type": "example/City",
            "_id": "19e4f199-93c5-40e5-b04e-a575e81ac373",
            "_revision": "b6197bb7-3592-4cdb-a61c-5a618f44950c",
            "name": "Vilnius",
        }
    )

    app = create_test_client(
        context,
        scope=[
            "uapi:/:getall",
            "uapi:/:search",
            "uapi:/:getone",
            "uapi:/:changes",
        ],
    )

    resp = app.head(f"/{path}")
    assert resp.status_code == 200
    assert resp.content == b""


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_delete(
    model: str,
    app: TestClient,
):
    app.authmodel(model, ["insert", "delete"])
    data = pushdata(app, model, {"status": "ok"})
    _id = data["_id"]

    resp = app.delete(f"/{model}/{_id}")
    assert resp.status_code == 204
    assert resp.content == b""


@pytest.mark.models(
    "backends/postgres/Report",
    "backends/mongo/Report",
)
def test_delete_batch(
    model: str,
    app: TestClient,
):
    app.authmodel(model, ["insert", "delete"])
    data = pushdata(app, model, {"status": "ok"})
    _id = data["_id"]

    resp = app.post(
        "/",
        json={
            "_data": [
                {
                    "_op": "delete",
                    "_where": f'_id="{_id}"',
                    "_type": f"{model}",
                }
            ],
        },
    )
    assert resp.status_code == 200

    data = resp.json()
    item = data["_data"][0]
    assert data == {
        "_status": "ok",
        "_transaction": data["_transaction"],
        "_data": [
            {
                "_type": model,
                "_id": item["_id"],
                "_revision": item["_revision"],
            }
        ],
    }


def test_get_gt_ge_lt_le_ne(app):
    app.authorize(["uapi:/:set_meta_fields"])
    app.authmodel("/datasets/json/Rinkimai", ["insert", "upsert", "search"])

    resp = app.post(
        "/datasets/json/Rinkimai",
        json={
            "_data": [
                {
                    "_id": "3ba54cb1-2099-49c8-9b6e-629f6ad60a3b",
                    "_op": "upsert",
                    "_type": "datasets/json/Rinkimai",
                    "_where": 'id="1"',
                    "id": "1",
                    "pavadinimas": "Rinkimai 1",
                },
                {
                    "_id": "1e4fff26-0082-4ce8-87b8-dd8abf7fadae",
                    "_op": "upsert",
                    "_type": "datasets/json/Rinkimai",
                    "_where": 'id="2"',
                    "id": "2",
                    "pavadinimas": "Rinkimai 2",
                },
                {
                    "_id": "7109da99-6c02-49bf-97aa-a602e23b6659",
                    "_op": "upsert",
                    "_type": "datasets/json/Rinkimai",
                    "_where": 'id="3"',
                    "id": "3",
                    "pavadinimas": "Rinkimai 3",
                },
                {
                    "_id": "959e8881-9d84-4b44-a0e8-31183aac0de6",
                    "_op": "upsert",
                    "_type": "datasets/json/Rinkimai",
                    "_where": 'id="4"',
                    "id": "4",
                    "pavadinimas": "Rinkimai 4",
                },
                {
                    "_id": "24e613cc-3b3d-4075-96dd-093f2edbdf08",
                    "_op": "upsert",
                    "_type": "datasets/json/Rinkimai",
                    "_where": 'id="5"',
                    "id": "5",
                    "pavadinimas": "Rinkimai 5",
                },
            ]
        },
    )
    # FIXME: Status code on multiple objects must be 207.
    assert resp.status_code == 200, resp.json()

    eq_id_for_gt_ge = resp.json()["_data"][0]["_id"]
    request_line = '/datasets/json/Rinkimai?_id>"{0}"'.format(eq_id_for_gt_ge)
    resp = app.get(request_line)
    assert len(resp.json()["_data"]) == 2

    request_line = '/datasets/json/Rinkimai?_id>="{0}"'.format(eq_id_for_gt_ge)
    resp = app.get(request_line)

    assert len(resp.json()["_data"]) == 3

    eq_id_for_gt_ge = resp.json()["_data"][1]["_id"]
    request_line = '/datasets/json/Rinkimai?_id<="{0}"'.format(eq_id_for_gt_ge)
    resp = app.get(request_line)

    assert len(resp.json()["_data"]) == 4

    request_line = '/datasets/json/Rinkimai?_id<"{0}"'.format(eq_id_for_gt_ge)
    resp = app.get(request_line)
    assert len(resp.json()["_data"]) == 3

    request_line = '/datasets/json/Rinkimai?_id!="{0}"'.format(eq_id_for_gt_ge)
    resp = app.get(request_line)
    assert len(resp.json()["_data"]) == 4


@pytest.mark.parametrize(
    "client_name, scopes, secret",
    [
        ("test_client_id", ["uapi:/:getall"], "secret"),
        ("test_client_id", ["spinta_getall"], "secret"),
        ("test_only_client", None, "req"),
        (None, ["uapi:/:getall"], "req"),
        (None, ["spinta_getall"], "req"),
        (None, None, "onlysecret"),
    ],
    ids=["all given", "all given", "only name", "only scope", "only scope", "only secret"],
)
def test_auth_clients_create_authorized_correct(
    rc: RawConfig, tmp_path: pathlib.Path, client_name: str, scopes: list, secret: str
):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    app.authorize(["spinta_auth_clients"])
    data = {}
    if client_name:
        data["client_name"] = client_name
    if scopes:
        data["scopes"] = scopes
    if secret:
        data["secret"] = secret
    resp = app.post(clients_url(), json=data)
    resp_json = resp.json()
    resp_client_name = client_name if client_name else resp_json["client_name"]
    resp_scopes = scopes if scopes else []

    assert resp.status_code == 200
    assert resp.json() == {
        "client_id": resp_json["client_id"],
        "client_name": resp_client_name,
        "scopes": resp_scopes,
    }
    if not client_name:
        assert is_str_uuid(resp_client_name)
    if secret:
        path = get_clients_path(context.get("config"))
        client = query_client(path, client=resp_json["client_id"])
        assert client.check_client_secret(secret)


def test_auth_clients_create_authorized_incorrect(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    app.authorize(["spinta_auth_clients"])
    resp_created = app.post(clients_url(), json={"client_name": "exists", "secret": "secret"})
    assert resp_created.status_code == 200

    resp = app.post(clients_url(), json={"client_name": "exists", "secret": "secret"})
    assert resp.status_code == 400
    assert error(resp) == "ClientWithNameAlreadyExists"

    resp = app.post(clients_url(), json={"client_name": "exist", "secret": "secret", "new_field": "FIELD"})
    assert resp.status_code == 400
    err = error(resp, "context", "code")
    assert err["code"] == "ClientValidationError"
    assert err["context"]["errors"] == "{'new_field': 'Extra inputs are not permitted'}"

    resp = app.post(clients_url(), json={"client_name": "exist0"})
    assert resp.status_code == 400
    err = error(resp, "context", "code")
    assert err["code"] == "ClientValidationError"
    assert err["context"]["errors"] == "{'secret': 'Field required'}"

    resp = app.post(clients_url(), json={"client_name": "exist1", "secret": ""})
    assert resp.status_code == 400
    err = error(resp, "context", "code")
    assert err["code"] == "ClientValidationError"
    assert err["context"]["errors"] == "{'secret': 'String should have at least 1 character'}"


def test_auth_clients_create_unauthorized(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    resp = app.post(clients_url(), json={"client_name": "test", "secret": "secret", "scopes": ["spinta_getall"]})
    assert resp.status_code == 403
    assert error(resp) == "InsufficientPermission"


def test_auth_clients_delete_unauthorized(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    client = create_client(context.get("config"))

    app.authorize([], strict_set=True)
    resp = app.delete(client_url(client["client_id"]))
    assert resp.status_code == 403
    assert error(resp) == "InsufficientPermission"


def test_auth_clients_delete_authorized_correct(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    client = create_client(context.get("config"))

    app.authorize(["spinta_auth_clients"])
    resp = app.delete(client_url(client["client_id"]))
    assert resp.status_code == 204


def test_auth_clients_delete_authorized_incorrect(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)

    app.authorize(["spinta_auth_clients"])
    resp = app.delete(client_url("non-existent"))
    assert resp.status_code == 400
    assert error(resp) == "InvalidClientError"


def test_auth_clients_get_authorized(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    client = create_client(context.get("config"), name="to_get")

    app.authorize(["spinta_auth_clients"])
    resp = app.get(client_url(client["client_id"]))

    assert resp.status_code == 200
    assert resp.json() == {"client_id": client["client_id"], "client_name": "to_get", "scopes": []}


def test_auth_clients_get_unauthorized(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    client_own = create_client(context.get("config"), name="to_get", secret="secret")
    client_normal = create_client(context.get("config"), name="normal", secret="secret")

    app.authorize(creds=("normal", "secret"), strict_set=True)
    resp = app.get(client_url(client_own["client_id"]))
    assert resp.status_code == 403
    assert error(resp) == "InsufficientPermission"

    resp = app.get(client_url(client_normal["client_id"]))
    assert resp.status_code == 200
    assert resp.json() == {"client_id": resp.json()["client_id"], "client_name": "normal", "scopes": []}


def test_auth_clients_get_all_authorized(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    app.authorize(["uapi:/:auth_clients"])

    client_one = create_client(context.get("config"), name="one", secret="secret")
    client_two = create_client(context.get("config"), name="two", secret="secret")
    client_three = create_client(context.get("config"), name="three", secret="secret")

    resp = app.get(clients_url())
    resp_json = resp.json()
    assert resp.status_code == 200
    assert {"client_id": str(client_one["client_id"]), "client_name": "one"} in resp_json
    assert {"client_id": str(client_two["client_id"]), "client_name": "two"} in resp_json
    assert {"client_id": str(client_three["client_id"]), "client_name": "three"} in resp_json


def test_auth_clients_get_all_unauthorized(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    client = create_client(context.get("config"), secret="secret")

    app.authorize(creds=(client["client_name"], "secret"), strict_set=True)
    resp = app.get(clients_url())
    assert resp.status_code == 403
    assert error(resp) == "InsufficientPermission"


@pytest.mark.parametrize(
    "client_name, scopes, secret",
    [
        ("test_client_id", ["uapi:/:create"], "secret"),
        ("test_client_id", ["spinta_insert"], "secret"),
        (None, None, None),
        ("test_only_client", None, None),
        (None, ["uapi:/:create"], None),
        (None, ["spinta_insert"], None),
        (None, None, "onlysecret"),
    ],
    ids=["all given", "all given", "none given", "only name", "only scope", "only scope", "only secret"],
)
def test_auth_clients_update_authorized_admin_correct(
    rc: RawConfig, tmp_path: pathlib.Path, client_name: str, scopes: list, secret: str
):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    path = get_clients_path(context.get("config"))
    client = create_client(context.get("config"), name="TEST", secret="OLD_SECRET", scopes=["spinta_getall"])

    data = {}
    if client_name:
        data["client_name"] = client_name
    if scopes:
        data["scopes"] = scopes
    if secret:
        data["secret"] = secret
    app.authorize(["spinta_auth_clients"])
    resp = app.patch(client_url(client["client_id"]), json=data)

    assert resp.status_code == 200
    resp_json = resp.json()
    resp_client_name = client_name if client_name else client["client_name"]
    assert resp_json == {
        "client_id": client["client_id"],
        "client_name": resp_client_name,
        "scopes": scopes if scopes else ["spinta_getall"],
        "backends": {},
    }
    if not client_name:
        assert resp_client_name == "TEST"

    # Query client with id
    client = query_client(path, client=resp_json["client_id"])
    if secret:
        assert client.check_client_secret(secret)
    else:
        assert client.check_client_secret("OLD_SECRET")

    # Query client with name
    client = query_client(path, client=resp_json["client_name"], is_name=True)
    if secret:
        assert client.check_client_secret(secret)
    else:
        assert client.check_client_secret("OLD_SECRET")


@pytest.mark.parametrize(
    "backends",
    [
        {},
        {"new_backend": {}},
        {"new_backend": {"test1": 1}},
    ],
)
def test_auth_clients_update_backends_authorized_admin_correct(
    rc: RawConfig,
    tmp_path: pathlib.Path,
    backends: dict,
) -> None:
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    client = create_client(context.get("config"))

    app.authorize(["spinta_auth_clients"])
    resp = app.patch(client_url(client["client_id"]), json={"backends": backends})
    resp_json = resp.json()

    assert resp.status_code == 200
    assert resp_json == {
        "client_id": ANY,
        "client_name": ANY,
        "scopes": [],
        "backends": backends,
    }


def test_auth_clients_update_authorized_admin_incorrect(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    create_client(context.get("config"), name="TEST")
    client_new = create_client(context.get("config"), name="NEW")

    app.authorize(["spinta_auth_clients"])
    resp = app.patch(client_url(client_new["client_id"]), json={"client_name": "TEST"})
    assert resp.status_code == 400
    assert error(resp) == "ClientWithNameAlreadyExists"


def test_auth_clients_update_authorized_correct(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    path = get_clients_path(context.get("config"))
    client = create_client(context.get("config"), name="TEST", secret="OLD_SECRET", scopes=["spinta_getall"])

    app.authorize(creds=("TEST", "OLD_SECRET"), strict_set=True)
    resp = app.patch(client_url(client["client_id"]), json={"secret": "NEW_SECRET"})

    assert resp.status_code == 200
    assert resp.json() == {
        "client_id": client["client_id"],
        "client_name": "TEST",
        "scopes": ["spinta_getall"],
        "backends": {},
    }
    client = query_client(path, client=client["client_id"])
    assert client.check_client_secret("NEW_SECRET")


def test_auth_clients_update_authorized_incorrect(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    client = create_client(context.get("config"), name="TEST", secret="OLD_SECRET", scopes=["spinta_getall"])

    app.authorize(creds=("TEST", "OLD_SECRET"), strict_set=True)
    resp = app.patch(client_url(client["client_id"]), json={"client_name": "NEW"})
    assert resp.status_code == 400
    err = error(resp, "context", "code")
    assert err["code"] == "ClientValidationError"
    assert err["context"]["errors"] == "{'client_name': 'Extra inputs are not permitted'}"

    resp = app.patch(client_url(client["client_id"]), json={"scopes": ["spinta_insert"]})
    assert resp.status_code == 400
    err = error(resp, "context", "code")
    assert err["code"] == "ClientValidationError"
    assert err["context"]["errors"] == "{'scopes': 'Extra inputs are not permitted'}"

    resp = app.patch(client_url(client["client_id"]), json={"something": "else"})
    assert resp.status_code == 400
    err = error(resp, "context", "code")
    assert err["code"] == "ClientValidationError"
    assert err["context"]["errors"] == "{'something': 'Extra inputs are not permitted'}"

    resp = app.patch(client_url(client["client_id"]), json={"secret": ""})
    assert resp.status_code == 400
    err = error(resp, "context", "code")
    assert err["code"] == "ClientValidationError"
    assert err["context"]["errors"] == "{'secret': 'String should have at least 1 character'}"


def test_auth_clients_update_unauthorized(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    client = create_client(context.get("config"))

    app.authorize(scopes=[], strict_set=True)
    resp = app.patch(client_url(client["client_id"]), json={"secret": "NEW"})
    assert resp.status_code == 403
    assert error(resp) == "InsufficientPermission"


def test_auth_clients_update_name_full_check(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    path = get_clients_path(context.get("config"))
    keymap_path = get_keymap_path(path)
    client = create_client(
        context.get("config"), name="TEST", secret="OLD_SECRET", scopes=["spinta_auth_clients", "spinta_getall"]
    )
    client_new = create_client(context.get("config"), name="TESTNEW", secret="OLD_SECRET", scopes=["spinta_getall"])

    app.authorize(scopes=["spinta_auth_clients"], creds=("TEST", "OLD_SECRET"), strict_set=True)
    resp = app.patch(client_url(client["client_id"]), json={"client_name": "TESTNEW"})
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["ClientWithNameAlreadyExists"]

    keymap = get_yaml_data(keymap_path)
    assert "TEST" in keymap
    test_id = keymap["TEST"]
    assert "TESTNEW" in keymap
    test_new_id = keymap["TESTNEW"]

    app.authorize(scopes=["spinta_auth_clients"], creds=("TEST", "OLD_SECRET"), strict_set=True)
    resp = app.patch(client_url(client_new["client_id"]), json={"client_name": "TESTNEWOTHER"})
    resp_json = resp.json()

    assert resp.status_code == 200
    assert resp.json() == {
        "client_id": resp_json["client_id"],
        "client_name": "TESTNEWOTHER",
        "scopes": ["spinta_getall"],
        "backends": {},
    }

    keymap = get_yaml_data(keymap_path)
    assert "TEST" in keymap
    assert keymap["TEST"] == test_id
    assert "TESTNEWOTHER" in keymap
    assert keymap["TESTNEWOTHER"] == test_new_id
    assert "TESTNEW" not in keymap


def test_auth_clients_update_own_client_using_client_backends_update_self_scope(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    client = create_client(context.get("config"), secret="OLD_SECRET", scopes=["uapi:/:client_backends_update_self"])
    app.authorize(["uapi:/:client_backends_update_self"], creds=(client["client_id"], "OLD_SECRET"), strict_set=True)

    response = app.patch(client_url(client["client_id"]), json={"backends": {"resource": {"test_key": "test_value"}}})

    assert response.status_code == 200
    assert response.json() == {
        "client_id": client["client_id"],
        "client_name": client["client_name"],
        "scopes": client["scopes"],
        "backends": {"resource": {"test_key": "test_value"}},
    }


def test_auth_clients_update_own_client_fails_using_client_backends_update_self_scope_with_extra_data(
    rc: RawConfig, tmp_path: pathlib.Path
):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    client = create_client(context.get("config"), secret="OLD_SECRET", scopes=["uapi:/:client_backends_update_self"])
    app.authorize(["uapi:/:client_backends_update_self"], creds=(client["client_id"], "OLD_SECRET"), strict_set=True)

    response = app.patch(
        client_url(client["client_id"]),
        json={
            "backends": {"resource": {"test_key": "test_value"}},
            "scopes": ["uapi:/:client_backends_update_self", "another_scope"],
        },
    )

    assert response.status_code == 400
    err = error(response, "context", "code")
    assert err["code"] == "ClientValidationError"
    assert err["context"]["errors"] == "{'scopes': 'Extra inputs are not permitted'}"


def test_auth_clients_update_other_client_fails_using_client_backends_update_self_scope(
    rc: RawConfig, tmp_path: pathlib.Path
):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    client1 = create_client(context.get("config"), secret="OLD_SECRET", scopes=["uapi:/:client_backends_update_self"])
    client2 = create_client(context.get("config"), secret="OLD_SECRET", scopes=["uapi:/:client_backends_update_self"])

    app.authorize(["uapi:/:client_backends_update_self"], creds=(client1["client_id"], "OLD_SECRET"), strict_set=True)

    response = app.patch(
        client_url(client2["client_id"]),
        json={
            "backends": {"resource": {"test_key": "test_value"}},
            "scopes": ["uapi:/:client_backends_update_self", "another_scope"],
        },
    )

    assert response.status_code == 403
    err = error(response, "context", "code")
    assert err["code"] == "InsufficientPermission"
