"""A single object (`GET {model}/{id}`) is enriched with `@context`, `@id` and `@type` (`.tmp/specs/json-ld.md`, rule 3,
section 3.2). Nothing else changes (rule 1), and only the single object is enriched in this task: the nested `continent`
ref is task 8, collections are task 10.

The model is the Country of section 1 of the spec, without its base chain. The base chain gives the same IRIs here as
the explicit `uri` of each element, so the response is the golden of `goldens/single.json` (see `helpers.py`).
"""

import logging

import pytest

from spinta import commands
from spinta.formats.jsonld.helpers import enrich_object
from spinta.testing.client import create_test_client
from spinta.testing.manifest import bootstrap_manifest
from tests.formats.jsonld.helpers import CONTEXT_URL, URL, bundle, golden
from tests.manifests.jsonld.helpers import SCHEMA, UR, table

DEMO = "datasets/gov/vssa/demo"
COUNTRY = f"{DEMO}/Country"
CONTINENT = f"{DEMO}/Continent"
SECRET = f"{DEMO}/Secret"
COUNTRY_ID = "38caeff5-a43a-4e6c-82d0-3c3046777dcf"
CONTINENT_ID = "25ed8eba-38e1-473d-987e-ea7c031fee30"
COUNTRY_IRI = f"{UR}/id/{COUNTRY}/{COUNTRY_ID}"
JSONLD_KEYS = ("@context", "@id", "@type")

ROWS = [
    dict(dataset=DEMO),
    dict(type="prefix", ref="schema", uri=SCHEMA),
    dict(model="Continent", ref="name", uri="schema:Continent"),
    dict(property="name", type="string", uri="schema:name"),
    dict(model="Country", ref="code", uri="schema:Country"),
    dict(property="code", type="string", uri="schema:identifier"),
    dict(property="name", type="text", uri="schema:name"),
    dict(property="name@en", type="string"),
    dict(property="name@lt", type="string"),
    dict(property="population", type="integer", uri="schema:population"),
    dict(property="founded", type="date", uri="schema:foundingDate"),
    dict(property="location", type="geometry", uri="schema:geo"),
    dict(property="continent", type="ref", ref="Continent", uri="schema:containedInPlace"),
    dict(property="flag", type="file", visibility="private"),
    dict(property="address", type="object", uri="schema:address"),
    dict(property="address.city", type="string", uri="schema:addressLocality"),
    dict(property="tags[]", type="string", uri="schema:keywords"),
    dict(property="updated", type="datetime", visibility="private"),
    # A private model has no context, so its responses are not enriched (10.2).
    dict(model="Secret", ref="name", visibility="private"),
    dict(property="name", type="string"),
]


@pytest.fixture
def app(rc, postgresql, request, tmp_path):
    path = tmp_path / "manifest.csv"
    path.write_text(table(ROWS), encoding="utf-8")
    context = bootstrap_manifest(rc.fork({"uri": UR}), path, backend=postgresql, tmp_path=tmp_path, request=request)
    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel(CONTINENT, ["insert", "getone"])
    app.authmodel(COUNTRY, ["insert", "getone", "getall"])
    app.authmodel(SECRET, ["insert", "getone", "getall"])

    resp = app.post(f"/{CONTINENT}", json={"_id": CONTINENT_ID, "name": "Europe"})
    assert resp.status_code == 201, resp.text
    resp = app.post(
        f"/{COUNTRY}",
        json={
            "_id": COUNTRY_ID,
            "code": "lt",
            "name": {"en": "Lithuania", "lt": "Lietuva"},
            "population": 2900000,
            "founded": "1253-07-06",
            "updated": "2024-01-01T10:00:00",
            "location": "POINT (23.9 55.1)",
            "continent": {"_id": CONTINENT_ID},
            "address": {"city": "Vilnius"},
            "tags": ["baltic", "eu"],
        },
    )
    assert resp.status_code == 201, resp.text
    resp = app.post(f"/{SECRET}", json={"name": "hidden"})
    assert resp.status_code == 201, resp.text
    return app


@pytest.fixture
def mode_config(app, mocker, mode):
    """`jsonld_context_url` for `mode`: unset is inline (10.16), set is URL mode."""
    mocker.patch.object(app.context.get("config"), "jsonld_context_url", CONTEXT_URL if mode == URL else None)
    return mode


def _plain(app, url: str) -> dict:
    """The response of `url` as plain JSON, with the switch off (the default)."""
    resp = app.get(url, headers={"accept": "application/json"})
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"] == "application/json"
    return resp.json()


def _without_jsonld(body: dict) -> dict:
    return {key: value for key, value in body.items() if key not in JSONLD_KEYS}


def test_single_object_has_the_jsonld_keys_first(app, mode_config, mode):
    resp = app.get(f"/{COUNTRY}/{COUNTRY_ID}", headers={"accept": "application/ld+json"})

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert list(body)[:3] == list(JSONLD_KEYS)
    assert body["@id"] == COUNTRY_IRI
    assert body["@type"] == SCHEMA + "Country"
    if mode == URL:
        assert body["@context"] == f"{CONTEXT_URL}/{COUNTRY}.json"
    else:
        assert body["@context"] == bundle()[COUNTRY]["@context"]


def test_single_object_keeps_its_keys_and_values(app, mode_config):
    # Rule 1: the keys and values that were there stay, in the same order.
    body = app.get(f"/{COUNTRY}/{COUNTRY_ID}", headers={"accept": "application/ld+json"}).json()
    plain = _plain(app, f"/{COUNTRY}/{COUNTRY_ID}")

    assert list(_without_jsonld(body)) == list(plain)
    assert _without_jsonld(body) == plain


def test_single_object_matches_the_golden_shape(app, mode_config, mode):
    body = app.get(f"/{COUNTRY}/{COUNTRY_ID}", headers={"accept": "application/ld+json"}).json()

    expected = golden("single", mode)
    # The continent ref is task 8: until it is done it keeps its plain form (rule 7 is not applied to it yet).
    expected["continent"] = {"_id": CONTINENT_ID}
    # The revision is set by the write, not fixed as in the golden.
    expected["_revision"] = body["_revision"]
    assert body == expected


@pytest.mark.parametrize("switch", [False, True])
@pytest.mark.parametrize(
    "accept, url_suffix, always",
    [
        # `application/ld+json` and `/:format/jsonld` are always enriched (rule 6).
        ("application/ld+json", "", True),
        (None, ":format/jsonld", True),
        # `application/json` and `/:format/json` are enriched only with the switch on (10.3).
        ("application/json", "", False),
        (None, ":format/json", False),
    ],
)
def test_single_object_enrichment_follows_format_and_switch(app, mocker, switch, accept, url_suffix, always):
    mocker.patch.object(app.context.get("config"), "jsonld", switch)
    url = f"/{COUNTRY}/{COUNTRY_ID}"
    if url_suffix:
        url = f"{url}/{url_suffix}"
    headers = {"accept": accept} if accept else {}

    resp = app.get(url, headers=headers)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    if always or switch:
        assert body["@id"] == COUNTRY_IRI
        assert body["@type"] == SCHEMA + "Country"
    else:
        assert not set(JSONLD_KEYS) & set(body)
        assert body == _plain(app, f"/{COUNTRY}/{COUNTRY_ID}")


def test_plain_json_with_switch_on_gets_the_same_body_as_ld_json(app, mocker, mode_config):
    mocker.patch.object(app.context.get("config"), "jsonld", True)
    plain_on = app.get(f"/{COUNTRY}/{COUNTRY_ID}", headers={"accept": "application/json"}).json()
    ld_json = app.get(f"/{COUNTRY}/{COUNTRY_ID}", headers={"accept": "application/ld+json"}).json()

    assert plain_on == ld_json


def test_protected_or_private_model_is_not_enriched(app, mode_config):
    # Rule 7 exception (10.2): no `@context`, `@id` or `@type`, and the body is the same as today.
    resp = app.get(f"/{SECRET}/{_secret_id(app)}", headers={"accept": "application/ld+json"})

    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"] == "application/ld+json"
    assert not set(JSONLD_KEYS) & set(resp.json())
    assert resp.json()["name"] == "hidden"


def _secret_id(app) -> str:
    resp = app.get(f"/{SECRET}", headers={"accept": "application/json"})
    assert resp.status_code == 200, resp.text
    return resp.json()["_data"][0]["_id"]


def test_sub_resource_is_not_enriched(app, mode_config):
    # `Country.address` is a nested object, not an instance of the class (6.1, 10.6).
    resp = app.get(f"/{COUNTRY}/{COUNTRY_ID}/address", headers={"accept": "application/ld+json"})

    assert resp.status_code == 200, resp.text
    assert not set(JSONLD_KEYS) & set(resp.json())


def test_object_without_id_gets_no_at_id_and_a_warning(app, mode_config, caplog):
    context = app.context
    model = commands.get_model(context, context.get("store").manifest, COUNTRY)
    data = {"_type": COUNTRY, "code": "lt"}

    with caplog.at_level(logging.WARNING, logger="spinta.formats.jsonld.helpers"):
        body = enrich_object(context, model, data)

    assert list(body) == ["@context", "@type", "_type", "code"]
    assert body["@type"] == SCHEMA + "Country"
    assert "@id" not in body
    assert "`@id` is not added" in caplog.text


def test_enrich_object_does_not_change_the_data(app, mode_config):
    context = app.context
    model = commands.get_model(context, context.get("store").manifest, COUNTRY)
    data = {"_type": COUNTRY, "_id": COUNTRY_ID, "code": "lt"}

    body = enrich_object(context, model, data)

    assert data == {"_type": COUNTRY, "_id": COUNTRY_ID, "code": "lt"}
    assert _without_jsonld(body) == data
