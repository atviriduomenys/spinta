import uuid

import pytest

from spinta import commands
from spinta.formats.json.components import Json
from spinta.formats.jsonld.components import JsonLd
from spinta.formats.jsonld.helpers import jsonld_enabled
from spinta.testing.context import create_test_context

MODEL = "backends/postgres/Report"


@pytest.fixture
def report(app):
    app.authmodel(MODEL, ["insert", "getone", "getall", "changes"])
    app.authorize(["spinta_set_meta_fields"])
    resp = app.post(f"/{MODEL}", json={"_type": "Report", "status": "1"})
    assert resp.status_code == 201, resp.text
    return resp.json()["_id"]


def test_jsonld_switch_is_off_by_default(context):
    assert context.get("config").jsonld is False


def test_jsonld_switch_is_read_from_config(rc, request):
    context = create_test_context(rc.fork({"jsonld": True}), request, wipe_data=False)
    commands.load(context, context.get("config"))
    assert context.get("config").jsonld is True


def test_jsonld_format_is_registered_for_ld_json(context):
    exporters = context.get("config").exporters
    assert isinstance(exporters["jsonld"], JsonLd)
    assert exporters["jsonld"].content_type == "application/ld+json"
    assert "application/ld+json" in exporters["jsonld"].accept_types


@pytest.mark.parametrize("switch", [False, True])
def test_jsonld_enabled_for_json_follows_switch(context, mocker, switch):
    mocker.patch.object(context.get("config"), "jsonld", switch)
    fmt = context.get("config").exporters["json"]
    assert isinstance(fmt, Json)
    assert jsonld_enabled(context, fmt) is switch


@pytest.mark.parametrize("switch", [False, True])
def test_jsonld_enabled_for_jsonld_always(context, mocker, switch):
    mocker.patch.object(context.get("config"), "jsonld", switch)
    assert jsonld_enabled(context, context.get("config").exporters["jsonld"]) is True


@pytest.mark.parametrize("name", ["csv", "jsonl", "rdf", "html", "ascii", "xlsx"])
def test_jsonld_enabled_never_for_other_formats(context, mocker, name):
    mocker.patch.object(context.get("config"), "jsonld", True)
    assert jsonld_enabled(context, context.get("config").exporters[name]) is False


@pytest.mark.parametrize("switch", [False, True])
@pytest.mark.parametrize(
    "accept, url_suffix, content_type",
    [
        # `Accept: application/ld+json` and `/:format/jsonld` are always JSON-LD.
        ("application/ld+json", "", "application/ld+json"),
        (None, ":format/jsonld", "application/ld+json"),
        # `Accept: application/json` and `/:format/json` keep `application/json`.
        ("application/json", "", "application/json"),
        (None, ":format/json", "application/json"),
        # No `Accept` header is plain JSON.
        (None, "", "application/json"),
    ],
)
def test_get_one_content_type(app, report, mocker, switch, accept, url_suffix, content_type):
    mocker.patch.object(app.context.get("config"), "jsonld", switch)
    url = f"/{MODEL}/{report}"
    if url_suffix:
        url = f"{url}/{url_suffix}"
    headers = {"accept": accept} if accept else {}

    resp = app.get(url, headers=headers)

    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"] == content_type
    assert resp.json()["_id"] == report


@pytest.mark.parametrize("switch", [False, True])
@pytest.mark.parametrize(
    "accept, url_suffix, content_type",
    [
        ("application/ld+json", "", "application/ld+json"),
        (None, ":format/jsonld", "application/ld+json"),
        ("application/json", "", "application/json"),
        (None, ":format/json", "application/json"),
    ],
)
def test_get_all_content_type(app, report, mocker, switch, accept, url_suffix, content_type):
    mocker.patch.object(app.context.get("config"), "jsonld", switch)
    url = f"/{MODEL}"
    if url_suffix:
        url = f"{url}/{url_suffix}"
    headers = {"accept": accept} if accept else {}

    resp = app.get(url, headers=headers)

    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"] == content_type
    assert any(row["_id"] == report for row in resp.json()["_data"])


@pytest.mark.parametrize("switch", [False, True])
@pytest.mark.parametrize("accept", ["application/ld+json", "application/json", None])
def test_error_responses_are_always_json(app, report, mocker, switch, accept):
    mocker.patch.object(app.context.get("config"), "jsonld", switch)
    headers = {"accept": accept} if accept else {}

    resp = app.get(f"/{MODEL}/{uuid.uuid4()}", headers=headers)
    assert resp.status_code == 404, resp.text
    assert resp.headers["content-type"] == "application/json"
    assert "errors" in resp.json()

    resp = app.get("/backends/postgres/NoSuchModel", headers=headers)
    assert resp.status_code == 404, resp.text
    assert resp.headers["content-type"] == "application/json"
    assert "errors" in resp.json()


def test_plain_json_is_unchanged_with_switch_off(app, report, mocker):
    mocker.patch.object(app.context.get("config"), "jsonld", False)

    default = app.get(f"/{MODEL}/{report}")
    explicit = app.get(f"/{MODEL}/{report}", headers={"accept": "application/json"})
    by_format = app.get(f"/{MODEL}/{report}/:format/json")

    assert default.headers["content-type"] == "application/json"
    assert explicit.content == default.content
    assert by_format.content == default.content
    assert by_format.headers["content-type"] == "application/json"
