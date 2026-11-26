"""The `@context` value of a JSON-LD response: a URL or the inline context (`.tmp/specs/json-ld.md`, rule 4, 10.9, 10.14,
10.16), and the startup and request-time checks of an empty `uri`.
"""

import json
import logging

import pytest

from spinta import commands
from spinta.core.config import read_config
from spinta.formats.jsonld import helpers
from spinta.formats.jsonld.helpers import context_value, protocol_context_value
from spinta.lib import Spinta
from spinta.testing.context import create_test_context
from spinta.testing.manifest import load_manifest_and_context
from tests.manifests.jsonld.helpers import SPEC_CSV, UR, table

DEMO = "datasets/gov/vssa/demo"
COUNTRY = f"{DEMO}/Country"
CONTEXT_URL = "https://data.gov.lt/context"
WARNING_LOGGER = "spinta.formats.jsonld.helpers"
STARTUP_LOGGER = "spinta.types.config"


@pytest.fixture
def spec_manifest(tmp_path):
    path = tmp_path / "spec.csv"
    path.write_text(SPEC_CSV, encoding="utf-8")
    return path


@pytest.fixture(autouse=True)
def reset_warning(monkeypatch):
    # The request-time warning is logged once per process. Each test starts with it not logged.
    monkeypatch.setattr(helpers, "_warned_empty_uri", False)


def _load(rc, path, **config):
    context, manifest = load_manifest_and_context(rc.fork(config), path)
    return context, commands.get_models(context, manifest)


def _export(model: str, uri: str = UR, path=None) -> dict:
    """The `@context` that `spinta copy` writes for `model`, the value that the inline mode must give."""
    doc = Spinta(uri=uri).copy(path.read_text(encoding="utf-8").encode("utf-8"), "csv")
    return json.loads(Spinta(uri=uri).copy(doc, "jsonld", model=model))["@context"]


# Config.


def test_jsonld_context_url_is_none_by_default(context):
    assert context.get("config").jsonld_context_url is None


def test_jsonld_context_url_is_read_from_config(rc, request):
    context = create_test_context(rc.fork({"jsonld_context_url": CONTEXT_URL}), request, wipe_data=False)
    commands.load(context, context.get("config"))
    assert context.get("config").jsonld_context_url == CONTEXT_URL


def test_jsonld_context_url_is_read_from_environment(monkeypatch):
    # The environment is read when the configuration is read, as for `SPINTA_URI`.
    monkeypatch.setenv("SPINTA_JSONLD_CONTEXT_URL", CONTEXT_URL)
    assert read_config().get("jsonld_context_url") == CONTEXT_URL


# URL mode.


def test_context_value_is_the_model_url(rc, spec_manifest):
    context, models = _load(rc, spec_manifest, jsonld_context_url=CONTEXT_URL)
    assert context_value(context, models[COUNTRY]) == f"{CONTEXT_URL}/{COUNTRY}.json"
    assert context_value(context, models[COUNTRY]) == "https://data.gov.lt/context/datasets/gov/vssa/demo/Country.json"


def test_protocol_context_value_is_the_uapi_url(rc, spec_manifest):
    context, _ = _load(rc, spec_manifest, jsonld_context_url=CONTEXT_URL)
    assert protocol_context_value(context) == "https://data.gov.lt/context/_uapi.json"


@pytest.mark.parametrize("url", [f"{CONTEXT_URL}/", f"{CONTEXT_URL}//"])
def test_trailing_slash_is_removed(rc, spec_manifest, url):
    context, models = _load(rc, spec_manifest, jsonld_context_url=url)
    assert context_value(context, models[COUNTRY]) == f"{CONTEXT_URL}/{COUNTRY}.json"
    assert protocol_context_value(context) == f"{CONTEXT_URL}/_uapi.json"


def test_uri_does_not_change_the_context_url(rc, spec_manifest):
    context, models = _load(rc, spec_manifest, uri="https://other.example", jsonld_context_url=CONTEXT_URL)
    assert context_value(context, models[COUNTRY]).startswith(f"{CONTEXT_URL}/")
    assert "other.example" not in context_value(context, models[COUNTRY])
    assert protocol_context_value(context) == f"{CONTEXT_URL}/_uapi.json"


def test_uri_gives_no_context_url_when_jsonld_context_url_is_unset(rc, spec_manifest):
    context, models = _load(rc, spec_manifest, uri=UR)
    # Without `jsonld_context_url` the value is inline, a dict, never a URL built from `uri`.
    assert isinstance(context_value(context, models[COUNTRY]), dict)
    assert protocol_context_value(context) == {"_data": "@graph"}


# Inline mode.


@pytest.mark.parametrize("url", [None, ""])
def test_inline_context_value_is_the_exported_context(rc, spec_manifest, url):
    context, models = _load(rc, spec_manifest, uri=UR, jsonld_context_url=url)
    for name in (COUNTRY, f"{DEMO}/Region"):
        assert context_value(context, models[name]) == _export(name, path=spec_manifest)


def test_inline_protocol_context_value(rc, spec_manifest):
    context, _ = _load(rc, spec_manifest, uri=UR)
    assert protocol_context_value(context) == {"_data": "@graph"}


def test_inline_context_is_built_once_and_cached(rc, spec_manifest):
    context, models = _load(rc, spec_manifest, uri=UR)
    first = context_value(context, models[COUNTRY])
    assert context_value(context, models[COUNTRY]) is first


def test_inline_context_follows_uri(rc, spec_manifest):
    context, models = _load(rc, spec_manifest, uri=UR)
    relative = _export(f"{DEMO}/Region", uri="", path=spec_manifest)
    with_uri = context_value(context, models[f"{DEMO}/Region"])
    assert with_uri["name"] == f"{UR}/def/addresses/Region/name"
    assert relative["name"] == "/def/addresses/Region/name"


def test_protected_and_private_models_have_no_context_value(rc, tmp_path):
    path = tmp_path / "private.csv"
    path.write_text(
        table(
            [
                dict(dataset="ds", type="registry"),
                dict(type="prefix", ref="schema", uri="https://schema.org/"),
                dict(model="Secret", ref="id", visibility="private"),
                dict(property="id", type="integer", uri="schema:id"),
                dict(model="Open", ref="id", uri="schema:Open"),
                dict(property="id", type="integer", uri="schema:id"),
            ]
        ),
        encoding="utf-8",
    )
    for config in ({"jsonld_context_url": CONTEXT_URL}, {"uri": UR}):
        context, models = _load(rc, path, **config)
        assert context_value(context, models["ds/Secret"]) is None
        assert context_value(context, models["ds/Open"]) is not None


# Startup check and the request-time warning.


def test_startup_check_warns_when_switch_is_on_and_uri_is_empty(rc, request, caplog):
    context = create_test_context(rc.fork({"jsonld": True}), request, wipe_data=False)
    commands.load(context, context.get("config"))
    caplog.set_level(logging.WARNING, logger=STARTUP_LOGGER)
    commands.check(context, context.get("config"))  # the app still starts: no exception
    assert "`uri` is empty" in caplog.text


@pytest.mark.parametrize(
    "config",
    [
        {"jsonld": False},
        {"jsonld": True, "uri": UR},
        {"jsonld": True, "uri": UR + "/"},
    ],
)
def test_startup_check_is_silent_without_the_problem(rc, request, caplog, config):
    context = create_test_context(rc.fork(config), request, wipe_data=False)
    commands.load(context, context.get("config"))
    caplog.set_level(logging.WARNING, logger=STARTUP_LOGGER)
    commands.check(context, context.get("config"))
    assert "`uri` is empty" not in caplog.text


def test_enriched_response_warns_once_with_switch_off_and_empty_uri(rc, spec_manifest, caplog):
    # `Accept: application/ld+json` is enriched with the switch off. The first such value logs the warning, once.
    context, models = _load(rc, spec_manifest, jsonld=False)
    caplog.set_level(logging.WARNING, logger=WARNING_LOGGER)
    context_value(context, models[COUNTRY])
    protocol_context_value(context)
    assert caplog.text.count("`uri` is empty") == 1


def test_enriched_response_does_not_warn_with_uri_set(rc, spec_manifest, caplog):
    context, models = _load(rc, spec_manifest, uri=UR)
    caplog.set_level(logging.WARNING, logger=WARNING_LOGGER)
    context_value(context, models[COUNTRY])
    assert "`uri` is empty" not in caplog.text


def test_enriched_response_does_not_warn_again_with_switch_on(rc, spec_manifest, caplog):
    # With the switch on the startup check warns, so a response does not repeat it.
    context, models = _load(rc, spec_manifest, jsonld=True)
    caplog.set_level(logging.WARNING, logger=WARNING_LOGGER)
    context_value(context, models[COUNTRY])
    assert "`uri` is empty" not in caplog.text


def test_dataset_named_context_gives_no_warning_and_no_change(rc, request, tmp_path, caplog):
    path = tmp_path / "context.csv"
    path.write_text(
        table(
            [
                dict(dataset="context/gov", type="registry"),
                dict(type="prefix", ref="schema", uri="https://schema.org/"),
                dict(model="Country", ref="id", uri="schema:Country"),
                dict(property="id", type="integer", uri="schema:id"),
            ]
        ),
        encoding="utf-8",
    )
    context, models = _load(rc, path, uri=UR, jsonld_context_url=CONTEXT_URL, jsonld=True)
    caplog.set_level(logging.WARNING)
    assert context_value(context, models["context/gov/Country"]) == f"{CONTEXT_URL}/context/gov/Country.json"
    assert "`uri` is empty" not in caplog.text


# `spinta copy` with an empty `uri`.


def test_copy_with_empty_uri_warns_and_writes_the_file(rc, cli, tmp_path, caplog, spec_manifest):
    caplog.set_level(logging.WARNING, logger="spinta.manifests.jsonld.writer")
    output = tmp_path / "out.jsonld"
    result = cli.invoke(rc, ["copy", "-o", output, spec_manifest])
    assert result.exit_code == 0, result.output
    assert "`uri` is empty" in caplog.text
    assert json.loads(output.read_text(encoding="utf-8"))


def test_copy_with_empty_uri_and_switch_off_is_a_plain_context_file(rc, cli, tmp_path, spec_manifest):
    # `copy` does not read the `jsonld` switch or `jsonld_context_url` (10.15 (e)).
    output = tmp_path / "out.jsonld"
    cli.invoke(rc.fork({"jsonld_context_url": CONTEXT_URL}), ["copy", "-o", output, spec_manifest])
    assert json.loads(output.read_text(encoding="utf-8"))[COUNTRY]["@context"]["code"] == "schema:identifier"
