"""`GET {ns}/:all` is enriched with JSON-LD, item by item (`.tmp/specs/json-ld.md`, section 5.1, rule 7, 10.4).

The items are of several models, so each item gets the context, `@id` and `@type` of its own model, and the top-level
`@context` is the protocol context `_uapi.json`. An item of a `protected` or `private` model is not enriched (10.2). The
data is the one of `test_render_single.py`: a Continent, a Country, a Secret (private) and a Holder. The golden of 5.1 is
`goldens/all.json`, which is the Continent and the Country of this data, so the tests filter the namespace to them.
"""

import pytest
from rdflib import BNode, URIRef
from rdflib.namespace import RDF

from tests.formats.jsonld.helpers import (
    CONTEXT_URL,
    PROTOCOL_CONTEXT,
    PROTOCOL_URL,
    URL,
    bundle,
    golden,
    strip_jsonld,
    to_triples,
)
from tests.formats.jsonld.test_render_single import (  # noqa: F401 (the fixtures)
    CONTINENT,
    CONTINENT_ID,
    CONTINENT_IRI,
    COUNTRY,
    COUNTRY_ID,
    COUNTRY_IRI,
    DEMO,
    SECRET,
    app,
    mode_config,
)
from tests.manifests.jsonld.helpers import SCHEMA

ALL = f"/{DEMO}/:all"
# The Continent and the Country of the golden. `or(...)` is the only filter that picks two models in one namespace.
TWO_ITEMS = f"?or(eq(_id,'{CONTINENT_ID}'),eq(_id,'{COUNTRY_ID}'))"
JSONLD_KEYS = ("@context", "@id", "@type")
# The golden shows the keys of section 5.1. The response has the plain fields too, which the golden leaves out.
EXTRA_FIELDS = {"_base", "code", "name", "founded", "location", "continent", "flag", "address", "tags", "updated"}


@pytest.fixture(autouse=True)
def _namespace_scopes(app):
    # `:all` checks the scopes of the namespace, not of one model, so the models' scopes are not enough.
    app.authorize(["spinta_getall", "spinta_search"])


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


def _item(body: dict, model_type: str) -> dict:
    [item] = [i for i in body["_data"] if i["_type"] == model_type]
    return item


def _model_context(mode: str, model: str) -> str | dict:
    """The `@context` of an item of `model`: its URL in URL mode, its context object inline (10.16)."""
    if mode == URL:
        return f"{CONTEXT_URL}/{model}.json"
    return bundle()[model]["@context"]


def test_all_has_the_protocol_context_before_data(app, mode_config, mode):
    body = _get(app, ALL).json()

    assert list(body) == ["@context", "_data"]
    assert body["@context"] == _protocol_context(mode)


def test_all_items_have_the_context_of_their_own_model(app, mode_config, mode):
    body = _get(app, ALL + TWO_ITEMS).json()

    assert _item(body, CONTINENT)["@context"] == _model_context(mode, CONTINENT)
    assert _item(body, COUNTRY)["@context"] == _model_context(mode, COUNTRY)
    assert _item(body, CONTINENT)["@type"] == SCHEMA + "Continent"
    assert _item(body, COUNTRY)["@type"] == SCHEMA + "Country"
    for item in body["_data"]:
        assert list(item)[:4] == ["@context", "@id", "@type", "_type"]


def test_all_matches_the_golden_shape(app, mode_config, mode):
    body = _get(app, ALL + TWO_ITEMS).json()

    expected = golden("all", mode)
    assert body["@context"] == expected["@context"]
    assert len(body["_data"]) == len(expected["_data"])
    for item, want in zip(body["_data"], expected["_data"]):
        # The revision is set by the write, not fixed as in the golden.
        want = {**want, "_revision": item["_revision"]}
        assert set(item) - set(want) <= EXTRA_FIELDS
        assert {key: item[key] for key in want} == want


def test_all_rdf_has_the_triples_of_the_golden(app, mode_config, mode):
    body = _get(app, ALL + TWO_ITEMS).json()

    golden_triples = to_triples(golden("all", mode))
    assert len(golden_triples) == 5  # 5.1
    # The response has the plain fields too, so it has more triples than the golden, and all of them.
    assert golden_triples <= to_triples(body)


def test_all_without_protocol_context_has_no_triples(app, mode_config, mode):
    # `_data` has no term without the protocol context, so the items are not reached (10.4).
    body = _get(app, ALL + TWO_ITEMS).json()
    del body["@context"]

    assert to_triples(body) == set()


def test_all_protected_item_is_unchanged(app, mode_config):
    # 10.2: a private model is not enriched, so its item is the plain one, with no JSON-LD key.
    body = _get(app, ALL).json()

    secret = _item(body, SECRET)
    assert not set(JSONLD_KEYS) & set(secret)
    assert secret == _item(_plain(app, ALL), SECRET)


def test_all_without_id_items_are_blank_nodes(app, mode_config, mode):
    # Rule 7, 4.3, 10.17: `select(name)` gives `_type` and `@type`, and no `_id` and no `@id`, so the items are blank.
    body = _get(app, ALL + TWO_ITEMS + "&select(name)").json()

    assert body["@context"] == _protocol_context(mode)
    assert len(body["_data"]) == 2
    for item in body["_data"]:
        assert list(item)[:3] == ["@context", "@type", "_type"]
        assert "_id" not in item and "@id" not in item

    types = {s: o for s, p, o in to_triples(body) if p == RDF.type}
    assert sorted(types.values()) == sorted([URIRef(SCHEMA + "Continent"), URIRef(SCHEMA + "Country")])
    assert all(isinstance(s, BNode) for s in types)


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
def test_all_enrichment_follows_format_and_switch(app, mocker, switch, accept, url_suffix, always):
    mocker.patch.object(app.context.get("config"), "jsonld", switch)
    url = ALL
    if url_suffix:
        url = f"{url}/{url_suffix}"
    headers = {"accept": accept} if accept else {}

    resp = app.get(url, headers=headers)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    if always or switch:
        assert body["@context"] in (PROTOCOL_URL, PROTOCOL_CONTEXT)
        assert all(item["@type"] for item in body["_data"] if item["_type"] != SECRET)
    else:
        assert "@context" not in body
        assert body == _plain(app, url)


def test_all_with_switch_off_is_the_plain_body(app, mocker):
    # `application/ld+json` is enriched whatever the switch says (rule 6). Removing what JSON-LD adds gives the plain
    # `:all` body, with its keys in the same order (rule 1).
    mocker.patch.object(app.context.get("config"), "jsonld", False)
    plain = _plain(app, ALL)
    ld = _get(app, ALL).json()

    assert list(plain) == ["_data"]
    assert ld["@context"] == PROTOCOL_URL or ld["@context"] == PROTOCOL_CONTEXT
    assert list(strip_jsonld(ld)) == list(plain)
    assert strip_jsonld(ld) == plain


def test_all_plain_json_with_switch_on_gets_the_same_body_as_ld_json(app, mocker, mode_config):
    mocker.patch.object(app.context.get("config"), "jsonld", True)

    plain_on = app.get(ALL, headers={"accept": "application/json"}).json()
    ld_json = app.get(ALL, headers={"accept": "application/ld+json"}).json()

    assert plain_on == ld_json
