"""A collection (`GET {model}`, `?select(...)`, `?code=...`) is enriched with JSON-LD (`.tmp/specs/json-ld.md`, section
4.2, rule 7, 10.4).

Each item is enriched by the same code as a single object (`enrich_object`, task 7), and the top-level `@context` is the
protocol context `_uapi.json`, which makes `_data` a graph. `_page` is not changed. The model is the one of
`test_render_single.py`, and the expected shape is `goldens/collection.json`.
"""

import json

import pytest
from rdflib import BNode, Literal, URIRef
from rdflib.namespace import XSD

from spinta import commands
from spinta.formats.json.components import Json
from spinta.formats.jsonld.helpers import enrich_objects
from spinta.testing.client import create_test_client
from spinta.testing.manifest import bootstrap_manifest
from tests.formats.jsonld.helpers import (
    CONTEXT_URL,
    PROTOCOL_CONTEXT,
    PROTOCOL_URL,
    URL,
    golden,
    strip_jsonld,
    to_triples,
)
from tests.formats.jsonld.test_render_single import (
    CONTINENT,
    CONTINENT_ID,
    COUNTRY,
    COUNTRY_ID,
    COUNTRY_IRI,
    ROWS,
    SECRET,
)
from tests.manifests.jsonld.helpers import SCHEMA, UR, table

LATVIA_ID = "4240ea57-4057-4d14-8013-20511a4e89e0"
LATVIA_IRI = f"{UR}/id/{COUNTRY}/{LATVIA_ID}"
JSONLD_KEYS = ("@context", "@id", "@type")
# The response has these plain fields too, which the golden leaves out. `founded` and `location` have a term, so they
# are in the RDF. `flag` and `updated` are private, so they are not.
EXTRA_FIELDS = ("founded", "location", "flag", "updated")
GSP_WKT = URIRef("http://www.opengis.net/ont/geosparql#wktLiteral")


@pytest.fixture
def app(rc, postgresql, request, tmp_path):
    path = tmp_path / "manifest.csv"
    path.write_text(table(ROWS), encoding="utf-8")
    context = bootstrap_manifest(rc.fork({"uri": UR}), path, backend=postgresql, tmp_path=tmp_path, request=request)
    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])
    for model in (CONTINENT, COUNTRY, SECRET):
        app.authmodel(model, ["insert", "getone", "getall", "search"])

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
            "location": "POINT (23.9 55.1)",
            "continent": {"_id": CONTINENT_ID},
            "address": {"city": "Vilnius"},
            "tags": ["baltic", "eu"],
        },
    )
    assert resp.status_code == 201, resp.text
    resp = app.post(
        f"/{COUNTRY}",
        json={
            "_id": LATVIA_ID,
            "code": "lv",
            "name": {"en": "Latvia", "lt": "Latvija"},
            "continent": {"_id": CONTINENT_ID},
            "address": {"city": None},
            "tags": [],
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


def _get(app, url: str, accept: str = "application/ld+json"):
    resp = app.get(url, headers={"accept": accept})
    assert resp.status_code == 200, resp.text
    return resp


def _plain(app, url: str) -> dict:
    resp = _get(app, url, accept="application/json")
    assert resp.headers["content-type"] == "application/json"
    return resp.json()


def _protocol_context(mode: str):
    return PROTOCOL_URL if mode == URL else PROTOCOL_CONTEXT


def test_collection_has_the_protocol_context_before_data(app, mode_config, mode):
    body = _get(app, f"/{COUNTRY}").json()

    assert list(body) == ["@context", "_data", "_page"]
    assert body["@context"] == _protocol_context(mode)


def test_collection_items_have_the_jsonld_keys_first(app, mode_config, mode):
    body = _get(app, f"/{COUNTRY}").json()

    assert len(body["_data"]) == 2
    for item in body["_data"]:
        assert list(item)[:4] == ["@context", "@id", "@type", "_type"]
        assert item["@type"] == SCHEMA + "Country"
        if mode == URL:
            assert item["@context"] == f"{CONTEXT_URL}/{COUNTRY}.json"
        else:
            assert item["@context"] == golden("single", mode)["@context"]


def test_collection_matches_the_golden_shape(app, mode_config, mode):
    body = _get(app, f"/{COUNTRY}").json()

    expected = golden("collection", mode)
    assert body["@context"] == expected["@context"]
    assert body["_page"] == expected["_page"]
    assert len(body["_data"]) == len(expected["_data"])
    for item, want in zip(body["_data"], expected["_data"]):
        # The revision is set by the write, not fixed as in the golden.
        want = {**want, "_revision": item["_revision"]}
        # The golden shows the keys of section 4.2. The response has the plain fields too, and they are left out here.
        assert set(item) - set(want) == set(EXTRA_FIELDS)
        assert {key: item[key] for key in want} == want


def test_collection_without_protocol_context_has_no_triples(app, mode_config, mode):
    # The `_data` term is what makes the items reachable, so the protocol context is needed (10.4).
    body = _get(app, f"/{COUNTRY}").json()
    del body["@context"]

    assert to_triples(body) == set()


def test_collection_rdf_has_the_triples_of_the_golden(app, mode_config, mode):
    body = _get(app, f"/{COUNTRY}").json()

    triples = to_triples(body)
    golden_triples = to_triples(golden("collection", mode))

    assert len(golden_triples) == 13
    assert golden_triples <= triples
    assert triples - golden_triples == {
        (URIRef(COUNTRY_IRI), URIRef(SCHEMA + "foundingDate"), Literal("1253-07-06", datatype=XSD.date)),
        (URIRef(COUNTRY_IRI), URIRef(SCHEMA + "geo"), Literal("POINT (23.9 55.1)", datatype=GSP_WKT)),
    }


def test_collection_items_are_named_nodes(app, mode_config, mode):
    triples = to_triples(_get(app, f"/{COUNTRY}").json())

    subjects = {s for s, _, _ in triples}
    assert URIRef(COUNTRY_IRI) in subjects
    assert URIRef(LATVIA_IRI) in subjects


def test_collection_item_matches_the_single_object(app, mode_config):
    # The items are enriched by the same code as a single object (task 7), so the JSON-LD keys and the `ref` are equal.
    item = next(i for i in _get(app, f"/{COUNTRY}").json()["_data"] if i["_id"] == COUNTRY_ID)
    single = _get(app, f"/{COUNTRY}/{COUNTRY_ID}").json()

    assert {key: item[key] for key in JSONLD_KEYS} == {key: single[key] for key in JSONLD_KEYS}
    assert item["continent"] == single["continent"]


def test_collection_keeps_its_values_and_order(app, mode_config):
    # Rule 1: removing what JSON-LD adds gives the plain collection, with its keys in the same order.
    body = _get(app, f"/{COUNTRY}").json()
    plain = _plain(app, f"/{COUNTRY}")

    assert list(strip_jsonld(body)) == list(plain)
    assert strip_jsonld(body) == plain


def test_empty_collection_has_the_context_and_no_triples(app, mode_config, mode):
    body = _get(app, f"/{COUNTRY}?code='nope'").json()

    assert body["@context"] == _protocol_context(mode)
    assert body["_data"] == []
    assert to_triples(body) == set()
    assert strip_jsonld(body) == _plain(app, f"/{COUNTRY}?code='nope'")


def test_search_is_enriched_like_a_collection(app, mode_config, mode):
    body = _get(app, f"/{COUNTRY}?code='lt'").json()

    assert body["@context"] == _protocol_context(mode)
    [item] = body["_data"]
    assert item["_id"] == COUNTRY_ID
    assert item["@id"] == COUNTRY_IRI
    assert item["@type"] == SCHEMA + "Country"


def test_select_without_id_items_are_blank_nodes(app, mode_config, mode):
    body = _get(app, f"/{COUNTRY}?select(code)").json()

    assert body["@context"] == _protocol_context(mode)
    assert len(body["_data"]) == 2
    for item in body["_data"]:
        # Rule 7, 4.3, 10.17: `_type` is added, `_id` is not, so there is no `@id`.
        assert list(item) == ["@context", "@type", "_type", "code"]
        assert item["_type"] == COUNTRY
        assert "_id" not in item
        assert "@id" not in item
    # Each item is a blank node with its class, and no IRI (10.17).
    identified = [s for s, p, _ in to_triples(body) if p == URIRef(SCHEMA + "identifier")]
    assert len(identified) == 2
    assert all(isinstance(s, BNode) for s in identified)


def test_select_with_id_items_are_named_nodes(app, mode_config):
    body = _get(app, f"/{COUNTRY}?select(_id,code)").json()

    ids = {item["_id"]: item["@id"] for item in body["_data"]}
    assert ids == {COUNTRY_ID: COUNTRY_IRI, LATVIA_ID: LATVIA_IRI}


def test_count_is_not_enriched(app, mode_config):
    # A function result is not an object (4.4): no `@context`, and no `@context` at the top either.
    body = _get(app, f"/{COUNTRY}?select(count())").json()

    assert body == {"_data": [{"count()": 2}]}


def test_protected_collection_is_unchanged(app, mode_config):
    # 10.2: a private model is not enriched, so its collection body is the plain one.
    body = _get(app, f"/{SECRET}").json()

    assert body == _plain(app, f"/{SECRET}")
    assert not set(JSONLD_KEYS) & set(body)
    assert not set(JSONLD_KEYS) & set(body["_data"][0])


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
def test_collection_enrichment_follows_format_and_switch(app, mocker, switch, accept, url_suffix, always):
    mocker.patch.object(app.context.get("config"), "jsonld", switch)
    url = f"/{COUNTRY}"
    if url_suffix:
        url = f"{url}/{url_suffix}"
    headers = {"accept": accept} if accept else {}

    resp = app.get(url, headers=headers)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    if always or switch:
        assert body["@context"] in (PROTOCOL_URL, PROTOCOL_CONTEXT)
        assert all(item["@id"] for item in body["_data"])
    else:
        assert "@context" not in body
        assert body == _plain(app, url)


def test_plain_json_with_switch_off_has_no_context_and_is_unchanged(app, mocker):
    mocker.patch.object(app.context.get("config"), "jsonld", False)

    body = app.get(f"/{COUNTRY}", headers={"accept": "application/json"}).json()

    assert list(body) == ["_data", "_page"]
    assert all(list(item)[0] == "_type" for item in body["_data"])


def test_plain_json_with_switch_on_gets_the_same_body_as_ld_json(app, mocker, mode_config):
    mocker.patch.object(app.context.get("config"), "jsonld", True)

    plain_on = app.get(f"/{COUNTRY}", headers={"accept": "application/json"}).json()
    ld_json = app.get(f"/{COUNTRY}", headers={"accept": "application/ld+json"}).json()

    assert plain_on == ld_json


def test_enrich_objects_streams_one_item_at_a_time(app, mode_config):
    # The collection stays a stream: an item is enriched when it is read, not before (no full buffering).
    context = app.context
    model = commands.get_model(context, context.get("store").manifest, COUNTRY)
    pulled = []

    def rows():
        for index in range(3):
            pulled.append(index)
            yield {"_id": COUNTRY_ID, "code": str(index)}

    chunks = Json()(enrich_objects(context, model, rows()), context_value=PROTOCOL_CONTEXT)

    assert next(chunks).startswith('{"@context":{"_data":"@graph"},"_data":[')
    assert pulled == []
    next(chunks)
    assert pulled == [0]


def test_json_without_context_value_is_the_same_as_before():
    # The renderer's output without a context (switch off, `count()`) is not changed by the new keyword.
    rows = [{"_id": "a", "code": "lt"}, {"_id": "b", "code": "lv", "_page": "p"}]

    text = "".join(Json()(iter(rows)))

    assert text == '{"_data":[{"_id":"a","code":"lt"},{"_id":"b","code":"lv"}],"_page":{"next":"p"}}'
    assert json.loads(text)["_data"][0] == {"_id": "a", "code": "lt"}


def test_json_with_context_value_writes_it_before_data():
    rows = [{"_id": "a"}]

    text = "".join(Json()(iter(rows), context_value="https://example.invalid/_uapi.json"))

    # `ujson` escapes `/`, which is valid JSON, so the document is compared after parsing. The key order is in the text.
    assert json.loads(text) == {"@context": "https://example.invalid/_uapi.json", "_data": [{"_id": "a"}]}
    assert list(json.loads(text)) == ["@context", "_data"]
    assert text.index('"@context"') < text.index('"_data"')
