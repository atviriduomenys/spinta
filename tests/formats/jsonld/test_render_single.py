"""A single object (`GET {model}/{id}`) is enriched with `@context`, `@id` and `@type` (`.tmp/specs/json-ld.md`, rule 3,
section 3.2). Nothing else changes (rule 1). The `ref` values of the object (the nested `continent`) get their keys too
(task 8). Collections are task 10.

The model is the Country of section 1 of the spec, without its base chain. The base chain gives the same IRIs here as
the explicit `uri` of each element, so the response is the golden of `goldens/single.json` (see `helpers.py`).
"""

import logging

import pytest
from rdflib import BNode, Literal, URIRef
from rdflib.namespace import RDF

from spinta import commands
from spinta.formats.jsonld.helpers import enrich_object, enrich_refs
from spinta.testing.client import create_test_client
from spinta.testing.manifest import bootstrap_manifest
from tests.formats.jsonld.helpers import CONTEXT_URL, URL, bundle, golden, strip_jsonld, to_triples
from tests.manifests.jsonld.helpers import SCHEMA, UR, table

DEMO = "datasets/gov/vssa/demo"
COUNTRY = f"{DEMO}/Country"
CONTINENT = f"{DEMO}/Continent"
SECRET = f"{DEMO}/Secret"
HOLDER = f"{DEMO}/Holder"
HOLDER_ID = "0c1f7a9e-3b1d-4f5e-9a7c-2d6e8b4f1a20"
COUNTRY_ID = "38caeff5-a43a-4e6c-82d0-3c3046777dcf"
CONTINENT_ID = "25ed8eba-38e1-473d-987e-ea7c031fee30"
COUNTRY_IRI = f"{UR}/id/{COUNTRY}/{COUNTRY_ID}"
CONTINENT_IRI = f"{UR}/id/{CONTINENT}/{CONTINENT_ID}"
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
    # Refs in every place a ref can be: a plain ref to a private model, an array of refs, and a ref in a nested object.
    dict(model="Holder", ref="name"),
    dict(property="name", type="string"),
    dict(property="secret", type="ref", ref="Secret"),
    dict(property="continents[]", type="ref", ref="Continent"),
    dict(property="place", type="object"),
    dict(property="place.continent", type="ref", ref="Continent"),
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
    app.authmodel(HOLDER, ["insert", "getone"])

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
    resp = app.post(
        f"/{HOLDER}",
        json={
            "_id": HOLDER_ID,
            "name": "holder",
            "secret": {"_id": _secret_id(app)},
            "continents": [{"_id": CONTINENT_ID}],
            "place": {"continent": {"_id": CONTINENT_ID}},
        },
    )
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

    # The ref `continent` gets its keys too (rule 7), so the JSON-LD keys are removed at every depth.
    assert list(strip_jsonld(body)) == list(plain)
    assert strip_jsonld(body) == plain


def test_single_object_matches_the_golden_shape(app, mode_config, mode):
    body = app.get(f"/{COUNTRY}/{COUNTRY_ID}", headers={"accept": "application/ld+json"}).json()

    expected = golden("single", mode)
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


def test_object_without_id_gets_no_at_id_and_no_warning(app, mode_config, caplog):
    # A `select(...)` that leaves out `_id` is expected (task 9, 10.17): no `@id`, and nothing is logged.
    context = app.context
    model = commands.get_model(context, context.get("store").manifest, COUNTRY)
    data = {"_type": COUNTRY, "code": "lt"}

    with caplog.at_level(logging.WARNING, logger="spinta.formats.jsonld.helpers"):
        body = enrich_object(context, model, data)

    assert list(body) == ["@context", "@type", "_type", "code"]
    assert body["@type"] == SCHEMA + "Country"
    assert "@id" not in body
    assert caplog.text == ""


def test_enrich_object_does_not_change_the_data(app, mode_config):
    context = app.context
    model = commands.get_model(context, context.get("store").manifest, COUNTRY)
    data = {"_type": COUNTRY, "_id": COUNTRY_ID, "code": "lt"}

    body = enrich_object(context, model, data)

    assert data == {"_type": COUNTRY, "_id": COUNTRY_ID, "code": "lt"}
    assert _without_jsonld(body) == data


def test_ref_is_a_full_model_object(app, mode_config, mode):
    # Rule 7, 3.2: the continent ref carries the keys of a Continent object, with its `_type` and `_id`.
    body = app.get(f"/{COUNTRY}/{COUNTRY_ID}", headers={"accept": "application/ld+json"}).json()

    continent = body["continent"]
    assert list(continent) == ["@context", "@id", "@type", "_type", "_id"]
    assert continent["@id"] == CONTINENT_IRI
    assert continent["@type"] == SCHEMA + "Continent"
    assert continent["_type"] == CONTINENT
    assert continent["_id"] == CONTINENT_ID
    if mode == URL:
        assert continent["@context"] == f"{CONTEXT_URL}/{CONTINENT}.json"
    else:
        assert continent["@context"] == bundle()[CONTINENT]["@context"]


def test_single_object_rdf_has_13_triples_and_a_named_continent(app, mode_config, mode):
    body = app.get(f"/{COUNTRY}/{COUNTRY_ID}", headers={"accept": "application/ld+json"}).json()

    triples = to_triples(body)

    assert len(triples) == 13
    country, continent = URIRef(COUNTRY_IRI), URIRef(CONTINENT_IRI)
    # The link points to the Continent IRI, not to a blank node (finding 4), and the Continent is typed.
    assert (country, URIRef(SCHEMA + "containedInPlace"), continent) in triples
    assert (continent, RDF.type, URIRef(SCHEMA + "Continent")) in triples
    assert not [o for _, p, o in triples if p == URIRef(SCHEMA + "containedInPlace") and isinstance(o, BNode)]


def test_ref_to_private_model_is_unchanged(app, mode_config):
    # 10.2: a ref to a private model gets no keys, so its value is the plain one.
    body = app.get(f"/{HOLDER}/{HOLDER_ID}", headers={"accept": "application/ld+json"}).json()

    assert body["secret"] == {"_id": _secret_id(app)}


def test_null_ref_stays_null(app, mode_config):
    resp = app.post(f"/{HOLDER}", json={"name": "empty"})
    assert resp.status_code == 201, resp.text
    empty_id = resp.json()["_id"]

    body = app.get(f"/{HOLDER}/{empty_id}", headers={"accept": "application/ld+json"}).json()

    assert body["secret"] is None
    assert body["place"]["continent"] is None


def test_ref_array_and_ref_in_nested_object_are_enriched(app, mode_config, mode):
    body = app.get(f"/{HOLDER}/{HOLDER_ID}", headers={"accept": "application/ld+json"}).json()

    expected = {
        "@id": CONTINENT_IRI,
        "@type": SCHEMA + "Continent",
        "_type": CONTINENT,
        "_id": CONTINENT_ID,
    }
    [item] = body["continents"]
    assert {key: item[key] for key in expected} == expected
    assert set(item) == {"@context", *expected}
    nested = body["place"]["continent"]
    assert {key: nested[key] for key in expected} == expected
    assert set(nested) == {"@context", *expected}
    # The object itself has the keys of a top-level object too (rule 3), and the plain body is unchanged (rule 1).
    assert strip_jsonld(body) == _plain(app, f"/{HOLDER}/{HOLDER_ID}")


def test_enrich_refs_leaves_null_id_ref_unchanged(app, mode_config):
    # Task 8: a `ref` with `_id` null has no target, so it is left as it is.
    context = app.context
    model = commands.get_model(context, context.get("store").manifest, COUNTRY)
    data = {"_id": COUNTRY_ID, "continent": {"_id": None}}

    assert enrich_refs(context, model, data) == data


def test_enrich_refs_gives_a_ref_without_id_no_at_id(app, mode_config, caplog):
    # Rule 7 and 10.17: a `ref` without `_id` (a `select` that leaves it out) still gets `@context`, `@type` and
    # `_type`, but no `@id`, and it is not logged.
    context = app.context
    model = commands.get_model(context, context.get("store").manifest, COUNTRY)
    data = {"_id": COUNTRY_ID, "continent": {"name": "Europe"}}

    with caplog.at_level(logging.WARNING, logger="spinta.formats.jsonld.helpers"):
        body = enrich_refs(context, model, data)

    continent = body["continent"]
    assert list(continent) == ["@context", "@type", "_type", "name"]
    assert continent["@type"] == SCHEMA + "Continent"
    assert continent["_type"] == CONTINENT
    assert "@id" not in continent
    assert caplog.text == ""


def test_enrich_refs_does_not_change_the_data(app, mode_config):
    context = app.context
    model = commands.get_model(context, context.get("store").manifest, COUNTRY)
    data = {"_id": COUNTRY_ID, "continent": {"_id": CONTINENT_ID}}

    body = enrich_refs(context, model, data)

    assert data == {"_id": COUNTRY_ID, "continent": {"_id": CONTINENT_ID}}
    assert body["continent"]["@id"] == CONTINENT_IRI


# `select(...)` (task 9, rule 7, 4.3, 10.17): `_type` is added to the selection when it is missing, `_id` is not.


def _select(app, query: str, accept: str = "application/ld+json") -> dict:
    resp = app.get(f"/{COUNTRY}/{COUNTRY_ID}?{query}", headers={"accept": accept})
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_select_without_id_adds_type_and_no_id(app, mode_config):
    body = _select(app, "select(code)")

    assert list(body) == ["@context", "@type", "_type", "code"]
    assert body["@type"] == SCHEMA + "Country"
    assert body["_type"] == COUNTRY
    assert body["code"] == "lt"
    assert "_id" not in body
    assert "@id" not in body


def test_select_with_id_is_a_named_object(app, mode_config):
    body = _select(app, "select(_id,code)")

    assert list(body) == ["@context", "@id", "@type", "_type", "_id", "code"]
    assert body["@id"] == COUNTRY_IRI
    assert body["@type"] == SCHEMA + "Country"
    assert body["_type"] == COUNTRY
    assert body["_id"] == COUNTRY_ID


def test_select_type_is_the_model_name_when_selected_too(app, mode_config):
    body = _select(app, "select(_type,code)")

    assert body["_type"] == COUNTRY
    assert "_id" not in body


def test_select_keeps_the_values_of_the_plain_response(app, mode_config):
    body = _select(app, "select(code)")
    plain = _plain(app, f"/{COUNTRY}/{COUNTRY_ID}?select(code)")

    assert {key: value for key, value in body.items() if key not in JSONLD_KEYS} == {
        "_type": COUNTRY,
        **plain,
    }


def test_select_without_id_with_switch_off_is_unchanged(app, mocker):
    # Plain JSON with the switch off gives exactly today's output (10.3): no `_type`, no JSON-LD keys.
    mocker.patch.object(app.context.get("config"), "jsonld", False)

    resp = app.get(f"/{COUNTRY}/{COUNTRY_ID}?select(code)", headers={"accept": "application/json"})

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"code": "lt"}


def test_select_ld_json_gives_the_same_body_with_switch_on_or_off(app, mocker):
    mocker.patch.object(app.context.get("config"), "jsonld", False)
    off = _select(app, "select(code)")
    mocker.patch.object(app.context.get("config"), "jsonld", True)
    on = _select(app, "select(code)")

    assert off == on


def test_select_on_plain_json_with_switch_on_adds_type(app, mocker, mode_config):
    mocker.patch.object(app.context.get("config"), "jsonld", True)

    body = _select(app, "select(code)", accept="application/json")

    assert body["_type"] == COUNTRY
    assert "_id" not in body
    assert "@id" not in body


def test_select_of_a_protected_model_is_unchanged(app, mode_config):
    # 10.2: a private model is not enriched, so its `select(...)` gets no `_type` either.
    resp = app.get(f"/{SECRET}/{_secret_id(app)}?select(name)", headers={"accept": "application/ld+json"})

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"name": "hidden"}


def test_select_ref_without_id_is_a_blank_ref_object(app, mode_config):
    # Rule 7: the `continent` ref is selected without its `_id`, so it has `@context`, `@type` and `_type`, no `@id`.
    body = _select(app, "select(code,continent.name)")

    continent = body["continent"]
    assert list(continent) == ["@context", "@type", "_type", "name"]
    assert continent["@type"] == SCHEMA + "Continent"
    assert continent["_type"] == CONTINENT
    assert "@id" not in continent
    assert "_id" not in continent


def test_select_without_id_rdf_is_a_blank_node_with_the_class(app, mode_config):
    body = _select(app, "select(code)")

    triples = to_triples(body)

    assert len(triples) == 2
    [subject] = {s for s, _, _ in triples}
    assert isinstance(subject, BNode)
    assert (subject, RDF.type, URIRef(SCHEMA + "Country")) in triples
    assert (subject, URIRef(SCHEMA + "identifier"), Literal("lt")) in triples
    assert not any(isinstance(s, URIRef) for s, _, _ in triples)


def test_select_with_id_rdf_is_a_named_node(app, mode_config):
    triples = to_triples(_select(app, "select(_id,code)"))

    assert (URIRef(COUNTRY_IRI), RDF.type, URIRef(SCHEMA + "Country")) in triples
    assert (URIRef(COUNTRY_IRI), URIRef(SCHEMA + "identifier"), Literal("lt")) in triples


def test_select_ref_without_id_rdf_is_a_blank_node(app, mode_config):
    # The ref without `_id` is a blank node typed by its class, and it is linked from the (also blank) country.
    triples = to_triples(_select(app, "select(code,continent.name)"))

    links = [o for _, p, o in triples if p == URIRef(SCHEMA + "containedInPlace")]
    assert len(links) == 1 and isinstance(links[0], BNode)
    assert (links[0], RDF.type, URIRef(SCHEMA + "Continent")) in triples
    assert not any(isinstance(s, URIRef) for s, _, _ in triples)


def test_search_select_without_id_has_type_and_no_id(app, mode_config):
    # A `select(...)` on a collection is a SEARCH (`urlparams`), so this is the SEARCH path.
    app.authmodel(COUNTRY, ["search"])

    resp = app.get(f"/{COUNTRY}?select(code)", headers={"accept": "application/ld+json"})

    assert resp.status_code == 200, resp.text
    items = resp.json()["_data"]
    assert items
    for item in items:
        assert item["_type"] == COUNTRY
        assert "_id" not in item
        assert "@id" not in item


def test_search_select_with_always_show_id_keeps_the_id(app, mocker, mode_config):
    # `always_show_id` (existing config) still adds `_id` to SEARCH. Collections are enriched in task 10.
    app.authmodel(COUNTRY, ["search"])
    mocker.patch.object(app.context.get("config"), "always_show_id", True)

    resp = app.get(f"/{COUNTRY}?select(code)", headers={"accept": "application/ld+json"})

    assert resp.status_code == 200, resp.text
    [item] = resp.json()["_data"]
    assert item["_id"] == COUNTRY_ID
    assert item["_type"] == COUNTRY
    assert item["code"] == "lt"


def test_getall_with_always_show_id_gets_type_only_with_jsonld(app, mocker, mode_config):
    # A bare GETALL with `always_show_id` selects `_id` only (`_apply_always_show_id`). With JSON-LD it still has
    # `_type` (rule 7), and the item is enriched like any collection item (task 10). Plain JSON is unchanged.
    app.authmodel(COUNTRY, ["getall"])
    mocker.patch.object(app.context.get("config"), "always_show_id", True)

    resp = app.get(f"/{COUNTRY}", headers={"accept": "application/json"})
    assert resp.json()["_data"] == [{"_id": COUNTRY_ID}]

    resp = app.get(f"/{COUNTRY}", headers={"accept": "application/ld+json"})
    assert resp.status_code == 200, resp.text
    [item] = resp.json()["_data"]
    assert item["_type"] == COUNTRY
    assert item["_id"] == COUNTRY_ID
    assert list(item) == ["@context", "@id", "@type", "_type", "_id"]


def test_count_select_is_unchanged(app, mode_config):
    # A function result is not a model object (4.4), so `_type` is not added to it.
    app.authmodel(COUNTRY, ["search"])

    resp = app.get(f"/{COUNTRY}?select(count())", headers={"accept": "application/ld+json"})

    assert resp.status_code == 200, resp.text
    [item] = resp.json()["_data"]
    assert list(item) == ["count()"]
