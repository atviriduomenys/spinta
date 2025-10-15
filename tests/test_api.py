import pathlib
import uuid
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import cast
from unittest.mock import ANY

import pytest
from _pytest.fixtures import FixtureRequest

from spinta import commands
from spinta.auth import query_client, get_clients_path, get_keymap_path
from spinta.backends.memory.components import Memory
from spinta.cli.helpers.store import _ensure_config_dir
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.core.config import configure_rc
from spinta.formats.html.components import Cell
from spinta.formats.html.helpers import short_id
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.client import TestClient, get_yaml_data
from spinta.testing.client import TestClientResponse
from spinta.testing.client import create_test_client
from spinta.testing.client import get_html_tree
from spinta.testing.context import create_test_context
from spinta.testing.data import pushdata, listdata
from spinta.testing.data import send
from spinta.testing.manifest import prepare_manifest, bootstrap_manifest
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.utils import get_error_codes, get_error_context, error
from spinta.utils.config import get_limit_path
from spinta.utils.nestedstruct import flatten
from spinta.utils.types import is_str_uuid


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
    return context, app


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
    app.authorize(["spinta_getall"])
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
    app.authorize(["spinta_datasets_getall"])
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


def test_dataset_with_show(context, app):
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
        "/datasets/json/Rinkimai?select(pavadinimas)",
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


def test_count(app):
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
    resp = app.get("/datasets/json/Rinkimai?count()", headers={"accept": "text/html"})
    assert resp.status_code == 200

    resp = app.get("/datasets/json/Rinkimai?select(count())", headers={"accept": "text/html"})
    assert resp.status_code == 200

    context = _cleaned_context(resp)
    assert context["data"] == [{"count()": 2}]


def test_select_with_function(app):
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
    resp = app.get("/datasets/json/Rinkimai/:format/json?select(_type,count())", headers={"accept": "text/html"})
    assert resp.status_code == 200

    context = resp.json()
    assert context["_data"] == [{"_type": "datasets/json/Rinkimai", "count()": 2}]


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
    app.authorize(["spinta_set_meta_fields"])
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
    app.authorize(["spinta_set_meta_fields"])
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
    app.authorize(["spinta_set_meta_fields"])

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

    # test integer validation
    resp = app.post(
        f"/{model}",
        json={
            "count": "123",
        },
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]
    assert get_error_context(
        resp.json(),
        "InvalidValue",
        ["manifest", "model", "property", "type"],
    ) == {
        "manifest": "default",
        "model": model,
        "property": "count",
        "type": "integer",
    }

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
            "spinta_getall",
            "spinta_search",
            "spinta_getone",
            "spinta_changes",
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
    app.authorize(["spinta_set_meta_fields"])
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
        ("test_client_id", ["spinta_getall"], "secret"),
        ("test_only_client", None, "req"),
        (None, ["spinta_getall"], "req"),
        (None, None, "onlysecret"),
    ],
    ids=["all given", "only name", "only scope", "only secret"],
)
def test_auth_clients_create_authorized_correct(
    rc: RawConfig, tmp_path: pathlib.Path, client_name: str, scopes: list, secret: str
):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    path = get_clients_path(context.get("config"))
    app.authorize(["spinta_auth_clients"])
    data = {}
    if client_name:
        data["client_name"] = client_name
    if scopes:
        data["scopes"] = scopes
    if secret:
        data["secret"] = secret
    resp = app.post("/auth/clients", json=data)
    resp_json = resp.json()
    resp_client_name = client_name if client_name else resp_json["client_name"]
    resp_scopes = scopes if scopes else []

    assert resp.status_code == 200
    assert resp.json() == {
        "client_id": resp_json["client_id"],
        "client_name": resp_client_name,
        "scopes": resp_scopes,
        "backends": {},
    }
    if not client_name:
        assert is_str_uuid(resp_client_name)
    if secret:
        client = query_client(path, client=resp_json["client_id"])
        assert client.check_client_secret(secret)


@pytest.mark.parametrize(
    "backends",
    [
        {},
        {"default": {}},
        {"default": {"test1": 1, "test2": 2}},
    ],
)
def test_auth_clients_create_with_backends_authorized(
    rc: RawConfig,
    tmp_path: pathlib.Path,
    backends: dict,
) -> None:
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    app.authorize(["spinta_auth_clients"])
    data = {
        "secret": "test_secret",
        "backends": backends,
    }
    resp = app.post("/auth/clients", json=data)
    resp_json = resp.json()

    assert resp.status_code == 200
    assert resp_json == {
        "client_id": ANY,
        "client_name": ANY,
        "scopes": [],
        "backends": backends,
    }


def test_auth_clients_create_authorized_incorrect(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    app.authorize(["spinta_auth_clients"])
    resp_created = app.post("/auth/clients", json={"client_name": "exists", "secret": "secret"})
    assert resp_created.status_code == 200

    resp = app.post("/auth/clients", json={"client_name": "exists", "secret": "secret"})
    assert resp.status_code == 400
    assert error(resp) == "ClientWithNameAlreadyExists"

    resp = app.post("/auth/clients", json={"client_name": "exist", "secret": "secret", "new_field": "FIELD"})
    assert resp.status_code == 400
    err = error(resp, "context", "code")
    assert err["code"] == "ClientValidationError"
    assert err["context"]["errors"] == "{'new_field': 'Extra inputs are not permitted'}"

    resp = app.post("/auth/clients", json={"client_name": "exist0"})
    assert resp.status_code == 400
    err = error(resp, "context", "code")
    assert err["code"] == "ClientValidationError"
    assert err["context"]["errors"] == "{'secret': 'Field required'}"

    resp = app.post("/auth/clients", json={"client_name": "exist1", "secret": ""})
    assert resp.status_code == 400
    err = error(resp, "context", "code")
    assert err["code"] == "ClientValidationError"
    assert err["context"]["errors"] == "{'secret': 'String should have at least 1 character'}"


def test_auth_clients_create_unauthorized(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    resp = app.post("/auth/clients", json={"client_name": "test", "secret": "secret", "scopes": ["spinta_getall"]})
    assert resp.status_code == 403
    assert error(resp) == "InsufficientPermission"


def test_auth_clients_delete_unauthorized(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    app.authorize(["spinta_auth_clients"])
    resp_create = app.post("/auth/clients", json={"client_name": "to_delete", "secret": "secret"})
    assert resp_create.status_code == 200

    app.authorize([], strict_set=True)
    resp = app.delete(f"/auth/clients/{resp_create.json()['client_id']}")
    assert resp.status_code == 403
    assert error(resp) == "InsufficientPermission"


def test_auth_clients_delete_authorized_correct(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    app.authorize(["spinta_auth_clients"])
    resp = app.post("/auth/clients", json={"client_name": "to_delete", "secret": "secret"})
    assert resp.status_code == 200

    resp = app.delete(f"/auth/clients/{resp.json()['client_id']}")
    assert resp.status_code == 204


def test_auth_clients_delete_authorized_incorrect(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    app.authorize(["spinta_auth_clients"])
    resp = app.delete("/auth/clients/non-existent")
    assert resp.status_code == 400
    assert error(resp) == "InvalidClientError"


def test_auth_clients_get_authorized(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    app.authorize(["spinta_auth_clients"])
    resp_created = app.post("/auth/clients", json={"client_name": "to_get", "secret": "secret"})
    resp = app.get(f"/auth/clients/{resp_created.json()['client_id']}")
    resp_json = resp.json()

    assert resp.status_code == 200
    assert resp_json == {"client_id": resp_json["client_id"], "client_name": "to_get", "scopes": []}


def test_auth_clients_get_unauthorized(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    app.authorize(["spinta_auth_clients"])
    resp_created = app.post("/auth/clients", json={"client_name": "to_get", "secret": "secret"})
    assert resp_created.status_code == 200

    resp_created_own = app.post("/auth/clients", json={"client_name": "normal", "secret": "secret"})
    assert resp_created_own.status_code == 200

    app.authorize(creds=("normal", "secret"), strict_set=True)
    resp = app.get(f"/auth/clients/{resp_created.json()['client_id']}")
    assert resp.status_code == 403
    assert error(resp) == "InsufficientPermission"

    resp = app.get(f"/auth/clients/{resp_created_own.json()['client_id']}")
    assert resp.status_code == 200
    assert resp.json() == {"client_id": resp.json()["client_id"], "client_name": "normal", "scopes": []}


def test_auth_clients_get_all_authorized(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    app.authorize(["spinta_auth_clients"])

    resp_one = app.post("/auth/clients", json={"client_name": "one", "secret": "secret"})
    assert resp_one.status_code == 200
    resp_two = app.post("/auth/clients", json={"client_name": "two", "secret": "secret"})
    assert resp_two.status_code == 200
    resp_three = app.post("/auth/clients", json={"client_name": "three", "secret": "secret"})
    assert resp_three.status_code == 200

    resp = app.get("/auth/clients")
    resp_json = resp.json()
    assert resp.status_code == 200
    assert {"client_id": str(resp_one.json()["client_id"]), "client_name": "one"} in resp_json
    assert {"client_id": str(resp_two.json()["client_id"]), "client_name": "two"} in resp_json
    assert {"client_id": str(resp_three.json()["client_id"]), "client_name": "three"} in resp_json


def test_auth_clients_get_all_unauthorized(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    app.authorize(["spinta_auth_clients"])

    resp_one = app.post("/auth/clients", json={"client_name": "one", "secret": "secret"})
    assert resp_one.status_code == 200
    resp_two = app.post("/auth/clients", json={"client_name": "two", "secret": "secret"})
    assert resp_two.status_code == 200
    resp_three = app.post("/auth/clients", json={"client_name": "three", "secret": "secret"})
    assert resp_three.status_code == 200

    app.authorize(creds=("one", "secret"), strict_set=True)

    resp = app.get("/auth/clients")
    assert resp.status_code == 403
    assert error(resp) == "InsufficientPermission"


@pytest.mark.parametrize(
    "client_name, scopes, secret",
    [
        ("test_client_id", ["spinta_insert"], "secret"),
        (None, None, None),
        ("test_only_client", None, None),
        (None, ["spinta_insert"], None),
        (None, None, "onlysecret"),
    ],
    ids=["all given", "none given", "only name", "only scope", "only secret"],
)
def test_auth_clients_update_authorized_admin_correct(
    rc: RawConfig, tmp_path: pathlib.Path, client_name: str, scopes: list, secret: str
):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    path = get_clients_path(context.get("config"))
    app.authorize(["spinta_auth_clients"])
    resp = app.post("/auth/clients", json={"client_name": "TEST", "secret": "OLD_SECRET", "scopes": ["spinta_getall"]})
    assert resp.status_code == 200
    data = {}
    if client_name:
        data["client_name"] = client_name
    if scopes:
        data["scopes"] = scopes
    if secret:
        data["secret"] = secret
    resp = app.patch(f"/auth/clients/{resp.json()['client_id']}", json=data)
    resp_json = resp.json()
    resp_client_name = client_name if client_name else resp_json["client_name"]
    resp_scopes = scopes if scopes else ["spinta_getall"]

    assert resp.status_code == 200
    assert resp.json() == {
        "client_id": resp_json["client_id"],
        "client_name": resp_client_name,
        "scopes": resp_scopes,
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
    app.authorize(["spinta_auth_clients"])
    resp = app.post("/auth/clients", json={"secret": "OLD_SECRET", "backends": {"backend": {"test1": 1}}})
    assert resp.status_code == 200

    resp = app.patch(f"/auth/clients/{resp.json()['client_id']}", json={"backends": backends})
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
    app.authorize(["spinta_auth_clients"])
    resp_test = app.post(
        "/auth/clients", json={"client_name": "TEST", "secret": "OLD_SECRET", "scopes": ["spinta_getall"]}
    )
    assert resp_test.status_code == 200
    resp_new = app.post(
        "/auth/clients", json={"client_name": "NEW", "secret": "OLD_SECRET", "scopes": ["spinta_getall"]}
    )
    assert resp_new.status_code == 200

    resp = app.patch(f"/auth/clients/{resp_new.json()['client_id']}", json={"client_name": "TEST"})
    assert resp.status_code == 400
    assert error(resp) == "ClientWithNameAlreadyExists"


def test_auth_clients_update_authorized_correct(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    path = get_clients_path(context.get("config"))
    app.authorize(["spinta_auth_clients"])
    resp = app.post("/auth/clients", json={"client_name": "TEST", "secret": "OLD_SECRET", "scopes": ["spinta_getall"]})
    assert resp.status_code == 200

    app.authorize(creds=("TEST", "OLD_SECRET"), strict_set=True)
    resp = app.patch(f"/auth/clients/{resp.json()['client_id']}", json={"secret": "NEW_SECRET"})
    resp_json = resp.json()

    assert resp.status_code == 200
    assert resp.json() == {
        "client_id": resp_json["client_id"],
        "client_name": "TEST",
        "scopes": ["spinta_getall"],
        "backends": {},
    }
    client = query_client(path, client=resp_json["client_id"])
    assert client.check_client_secret("NEW_SECRET")


def test_auth_clients_update_authorized_incorrect(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    app.authorize(["spinta_auth_clients"])
    resp_created = app.post(
        "/auth/clients", json={"client_name": "TEST", "secret": "OLD_SECRET", "scopes": ["spinta_getall"]}
    )
    assert resp_created.status_code == 200

    app.authorize(creds=("TEST", "OLD_SECRET"), strict_set=True)
    resp = app.patch(f"/auth/clients/{resp_created.json()['client_id']}", json={"client_name": "NEW"})
    assert resp.status_code == 400
    err = error(resp, "context", "code")
    assert err["code"] == "ClientValidationError"
    assert err["context"]["errors"] == "{'client_name': 'Extra inputs are not permitted'}"

    resp = app.patch(f"/auth/clients/{resp_created.json()['client_id']}", json={"scopes": ["spinta_insert"]})
    assert resp.status_code == 400
    err = error(resp, "context", "code")
    assert err["code"] == "ClientValidationError"
    assert err["context"]["errors"] == "{'scopes': 'Extra inputs are not permitted'}"

    resp = app.patch(f"/auth/clients/{resp_created.json()['client_id']}", json={"something": "else"})
    assert resp.status_code == 400
    err = error(resp, "context", "code")
    assert err["code"] == "ClientValidationError"
    assert err["context"]["errors"] == "{'something': 'Extra inputs are not permitted'}"

    resp = app.patch(f"/auth/clients/{resp_created.json()['client_id']}", json={"secret": ""})
    assert resp.status_code == 400
    err = error(resp, "context", "code")
    assert err["code"] == "ClientValidationError"
    assert err["context"]["errors"] == "{'secret': 'String should have at least 1 character'}"


def test_auth_clients_update_unauthorized(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    app.authorize(["spinta_auth_clients"])
    resp_created = app.post(
        "/auth/clients", json={"client_name": "TEST", "secret": "OLD_SECRET", "scopes": ["spinta_getall"]}
    )
    assert resp_created.status_code == 200

    app.authorize(scopes=[], strict_set=True)
    resp = app.patch(f"/auth/clients/{resp_created.json()['client_id']}", json={"secret": "NEW"})
    assert resp.status_code == 403
    assert error(resp) == "InsufficientPermission"


def test_auth_clients_update_name_full_check(rc: RawConfig, tmp_path: pathlib.Path):
    context, app = ensure_temp_context_and_app(rc, tmp_path)
    path = get_clients_path(context.get("config"))
    keymap_path = get_keymap_path(path)
    app.authorize(["spinta_auth_clients"])
    resp = app.post(
        "/auth/clients",
        json={"client_name": "TEST", "secret": "OLD_SECRET", "scopes": ["spinta_auth_clients", "spinta_getall"]},
    )
    assert resp.status_code == 200
    test_client_id = resp.json()["client_id"]

    resp_new = app.post(
        "/auth/clients", json={"client_name": "TESTNEW", "secret": "OLD_SECRET", "scopes": ["spinta_getall"]}
    )
    assert resp_new.status_code == 200
    test_new_client_id = resp_new.json()["client_id"]

    app.authorize(scopes=["spinta_auth_clients"], creds=("TEST", "OLD_SECRET"), strict_set=True)
    resp = app.patch(f"/auth/clients/{test_client_id}", json={"client_name": "TESTNEW"})
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["ClientWithNameAlreadyExists"]

    keymap = get_yaml_data(keymap_path)
    assert "TEST" in keymap
    test_id = keymap["TEST"]
    assert "TESTNEW" in keymap
    test_new_id = keymap["TESTNEW"]
    app.authorize(scopes=["spinta_auth_clients"], creds=("TEST", "OLD_SECRET"), strict_set=True)
    resp = app.patch(f"/auth/clients/{test_new_client_id}", json={"client_name": "TESTNEWOTHER"})
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


def test_get_format_limit(app):
    config = app.context.get("config")
    limit = 5
    config.exporters["html"].limit = limit

    app.authmodel("datasets/json/Rinkimai", ["insert", "getall"])
    pks = []
    for i in range(10):
        resp = app.post(
            "/datasets/json/Rinkimai",
            json={
                "id": str(i),
                "pavadinimas": f"Rinkimai {i}",
            },
        )
        pks.append(resp.json()["_id"])

    resp = app.get("/datasets/json/Rinkimai", headers={"accept": "text/html"})
    assert resp.status_code == 200, resp.json()

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
                {"value": str(i)},
                {"value": f"Rinkimai {i}"},
            ]
            for i, pk in enumerate(pks[:limit])
        ],
        "formats": [
            ("CSV", "/datasets/json/Rinkimai/:format/csv"),
            ("JSON", "/datasets/json/Rinkimai/:format/json"),
            ("JSONL", "/datasets/json/Rinkimai/:format/jsonl"),
            ("ASCII", "/datasets/json/Rinkimai/:format/ascii"),
            ("RDF", "/datasets/json/Rinkimai/:format/rdf"),
        ],
    }


def test_get_maximum_limit_error(app):
    config = app.context.get("config")
    limit = 5
    config.exporters["html"].limit = limit

    app.authmodel("datasets/json/Rinkimai", ["insert", "getall", "search"])

    resp = app.get(f"/datasets/json/Rinkimai?limit({limit + 1})", headers={"accept": "text/html"})
    assert resp.status_code == 400, resp.json()
    assert error(resp, "code", ["maximum_limit", "given_limit"]) == {
        "code": "ExceededMaximumLimit",
        "context": {
            "maximum_limit": limit,
            "given_limit": limit + 1,
        },
    }


def test_get_default_limit(app):
    config = app.context.get("config")
    limit = 5
    config.default_limit_objects = limit

    app.authmodel("datasets/json/Rinkimai", ["insert", "getall", "search"])
    pks = []
    for i in range(10):
        resp = app.post(
            "/datasets/json/Rinkimai",
            json={
                "id": str(i),
                "pavadinimas": f"Rinkimai {i}",
            },
        )
        pks.append(resp.json()["_id"])

    resp = app.get("/datasets/json/Rinkimai")
    assert resp.status_code == 400
    assert error(resp, "code", ["maximum_limit"]) == {
        "code": "LimitOrPageIsRequired",
        "context": {
            "maximum_limit": limit,
        },
    }

    resp = app.get("/datasets/json/Rinkimai?page()")
    assert resp.status_code == 200
    assert listdata(resp) == [
        ("0", "Rinkimai 0"),
        ("1", "Rinkimai 1"),
        ("2", "Rinkimai 2"),
        ("3", "Rinkimai 3"),
        ("4", "Rinkimai 4"),
    ]

    resp = app.get(f"/datasets/json/Rinkimai?limit({limit})")
    assert resp.status_code == 200
    assert listdata(resp) == [
        ("0", "Rinkimai 0"),
        ("1", "Rinkimai 1"),
        ("2", "Rinkimai 2"),
        ("3", "Rinkimai 3"),
        ("4", "Rinkimai 4"),
    ]


def test_limit_by_model(
    context: Context,
    tmp_path: pathlib.Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    cli: SpintaCliRunner,
):
    config = context.get("config")
    limit_path = get_limit_path(config)
    limit = 20
    with limit_path.open("w") as file:
        file.write(f"datasets/limit/cli/req/Country: {limit}")

    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(
            """
        d | r | b | m | property      | type    | ref     | prepare | access | level
        datasets/limit/cli/req        |         |         |         |        |
          |   |   | Country           |         | id      |         |        |
          |   |   |   | id            | integer |         |         | open   |
          |   |   |   | name          | string  |         |         | open   |
          |   |   | City              |         | id      |         |        |
          |   |   |   | id            | integer |         |         | open   |
          |   |   |   | name          | string  |         |         | open   |
          |   |   |   | country       | ref     | Country |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )

    app = create_test_client(context)
    app.authorize(
        [
            "spinta_insert",
            "spinta_getone",
            "spinta_delete",
            "spinta_wipe",
            "spinta_search",
            "spinta_set_meta_fields",
            "spinta_move",
            "spinta_getall",
            "spinta_changes",
        ]
    )

    for i in range(100):
        resp = app.post("/datasets/limit/cli/req/Country", json={"id": i, "name": f"Country {i}"})
        id_ = resp.json()["_id"]
        app.post("/datasets/limit/cli/req/City", json={"id": i, "name": f"City {i}", "country": {"_id": id_}})

    resp = app.get("/datasets/limit/cli/req/Country")
    assert resp.status_code == 400
    assert error(resp, "code", ["maximum_limit"]) == {
        "code": "LimitOrPageIsRequired",
        "context": {
            "maximum_limit": limit,
        },
    }

    resp = app.get("/datasets/limit/cli/req/Country?page()")
    assert resp.status_code == 200
    assert listdata(resp, sort="id") == [(i, f"Country {i}") for i in range(limit)]

    resp = app.get("/datasets/limit/cli/req/City")
    assert resp.status_code == 200
    assert len(listdata(resp)) == 100
