"""JSON-LD test harness: the golden responses convert to the expected RDF in both modes (`.tmp/specs/json-ld.md`,
sections 9.4 and 9.5, decision 10.16).

The counts are the spec's (section 9.4), re-derived for the example manifest of `tests/manifests/jsonld/helpers.py`
and the goldens of `goldens/`. `jsonld.js`, the processor the spec was measured with, gives the same counts. Two differ
from the spec and are explained where they are used: the two-item collection (13, not 16) and `:ns` (0, not 6).
"""

import json

import pytest
from rdflib import RDF, BNode, Graph, Literal, URIRef
from rdflib.namespace import XSD

from spinta import commands
from spinta.formats.jsonld.helpers import context_value, protocol_context_value
from spinta.testing.manifest import load_manifest_and_context
from tests.formats.jsonld import helpers
from tests.formats.jsonld.helpers import (
    CONTEXT_URL,
    INLINE,
    PROTOCOL_CONTEXT,
    PROTOCOL_URL,
    URL,
    CollidingKeywords,
    UnservedContext,
    bundle,
    golden,
    plain,
    served_documents,
    strip_jsonld,
    to_triples,
)
from tests.manifests.jsonld.helpers import SCHEMA, SPEC_CSV, UR

DEMO = "datasets/gov/vssa/demo"
COUNTRY = f"{DEMO}/Country"
CONTINENT = f"{DEMO}/Continent"
COUNTRY_ID = "38caeff5-a43a-4e6c-82d0-3c3046777dcf"
COUNTRY_ID_2 = "4240ea57-4057-4d14-8013-20511a4e89e0"
CONTINENT_ID = "25ed8eba-38e1-473d-987e-ea7c031fee30"
COUNTRY_IRI = f"{UR}/id/{COUNTRY}/{COUNTRY_ID}"
CONTINENT_IRI = f"{UR}/id/{CONTINENT}/{CONTINENT_ID}"
REVISION = "84578ebb-1280-42f4-87eb-f6c66a296b69"

# Triple counts of the goldens (9.4). The spec's value is in the comment when it differs.
SINGLE_TRIPLES = 13  # 3.2 / 3.4
COLLECTION_TRIPLES = 13  # 4.2 with plain names (4.1). The spec's 16 is the variant below, with language maps and tags.
COLLECTION_VARIANT_TRIPLES = 16  # 10.4: item 1 with `name` as a language map and `tags` = ["baltic", "eu"]
ALL_TRIPLES = 5  # 5.1
BATCH_TRIPLES = 2  # 6.2
NS_TRIPLES = 0  # 5.2 said 6, but `:ns` is not enriched (10.11), so its entries are not in the RDF
WRITE_TRIPLES = 2  # PATCH: type and population
SELECT_TRIPLES = 3  # select(code, population) with `_id` and `_type` added (4.3): type, identifier, population

COUNTS = {
    "single": SINGLE_TRIPLES,
    "collection": COLLECTION_TRIPLES,
    "all": ALL_TRIPLES,
    "batch": BATCH_TRIPLES,
    "ns": NS_TRIPLES,
    "write": WRITE_TRIPLES,
    "select": SELECT_TRIPLES,
}

# The plain counterpart of each golden, with `strip_jsonld` (9.5). `select` adds `_id` and `_type` (rule 7).
PLAIN_CASES = {
    "single": {},
    "collection": {},
    "all": {},
    "batch": {},
    "write": {},
    "select": {"extra": ("_id", "_type")},
    "ns": {},
}


def _load(rc, path, **config):
    context, manifest = load_manifest_and_context(rc.fork({"uri": UR, **config}), path)
    return context, commands.get_models(context, manifest)


def _subject(triples, subject):
    return {(p, o) for s, p, o in triples if s == subject}


@pytest.mark.parametrize("name", COUNTS)
def test_golden_has_the_counted_triples_in_both_modes(name, mode):
    assert len(to_triples(golden(name, mode))) == COUNTS[name]


@pytest.mark.parametrize("name", COUNTS)
def test_both_modes_give_the_same_triples(name):
    # Blank nodes are canonical, so the two sets are equal, not only the same size.
    assert to_triples(golden(name, URL)) == to_triples(golden(name, INLINE))


def test_single_object_triples_of_3_4(mode):
    triples = to_triples(golden("single", mode))
    country = URIRef(COUNTRY_IRI)
    continent = URIRef(CONTINENT_IRI)
    assert (country, RDF.type, URIRef(SCHEMA + "Country")) in triples
    assert (country, URIRef(SCHEMA + "identifier"), Literal("lt")) in triples
    assert (country, URIRef(SCHEMA + "name"), Literal("Lietuva", lang="lt")) in triples
    assert (country, URIRef(SCHEMA + "name"), Literal("Lithuania", lang="en")) in triples
    assert (country, URIRef(SCHEMA + "population"), Literal("2900000", datatype=XSD.integer)) in triples
    assert (country, URIRef(SCHEMA + "foundingDate"), Literal("1253-07-06", datatype=XSD.date)) in triples
    assert (country, URIRef(SCHEMA + "containedInPlace"), continent) in triples
    assert (continent, RDF.type, URIRef(SCHEMA + "Continent")) in triples
    assert (country, URIRef(SCHEMA + "keywords"), Literal("baltic")) in triples
    assert (country, URIRef(SCHEMA + "keywords"), Literal("eu")) in triples
    address = [o for s, p, o in triples if s == country and p == URIRef(SCHEMA + "address")]
    assert len(address) == 1 and isinstance(address[0], BNode)
    assert (address[0], URIRef(SCHEMA + "addressLocality"), Literal("Vilnius")) in triples


def test_private_properties_and_protocol_keys_are_not_in_the_rdf(mode):
    # `flag` and `updated` are private (no term, rule 2). `_revision` and `_type` have no term (rule 5).
    triples = to_triples(golden("single", mode))
    predicates = {p for _, p, _ in triples}
    assert not any(str(p).startswith(f"{UR}/def/") for p in predicates)
    literals = {str(o) for _, _, o in triples}
    assert REVISION not in literals
    assert f"{COUNTRY}" not in literals


def test_collection_without_the_protocol_context_has_no_triples(mode):
    # `_data` has no term, so its items are not reached from the document root (finding 8, 10.4).
    body = golden("collection", mode)
    del body["@context"]
    assert to_triples(body) == set()


def test_collection_with_language_maps_and_tags_has_the_spec_count(mode):
    # The spec's 16 (10.4) is for the collection with language maps and tags. The goldens follow 4.1 (plain names).
    body = golden("collection", mode)
    body["_data"][0]["name"] = {"en": "Lithuania", "lt": "Lietuva"}
    body["_data"][0]["tags"] = ["baltic", "eu"]
    assert len(to_triples(body)) == COLLECTION_VARIANT_TRIPLES


def test_collection_items_are_named_nodes(mode):
    triples = to_triples(golden("collection", mode))
    subjects = {s for s, _, _ in triples}
    assert URIRef(COUNTRY_IRI) in subjects
    assert URIRef(f"{UR}/id/{COUNTRY}/{COUNTRY_ID_2}") in subjects


def test_all_models_items_have_their_own_class(mode):
    triples = to_triples(golden("all", mode))
    assert (URIRef(CONTINENT_IRI), RDF.type, URIRef(SCHEMA + "Continent")) in triples
    assert (URIRef(COUNTRY_IRI), RDF.type, URIRef(SCHEMA + "Country")) in triples
    assert (URIRef(CONTINENT_IRI), URIRef(SCHEMA + "name"), Literal("Europe")) in triples


def test_batch_item_is_the_country(mode):
    triples = to_triples(golden("batch", mode))
    item = URIRef(f"{UR}/id/{COUNTRY}/0963baf0-46a2-493f-81f8-1d6200046202")
    assert triples == {
        (item, RDF.type, URIRef(SCHEMA + "Country")),
        (item, URIRef(SCHEMA + "identifier"), Literal("ee")),
    }


def test_ns_is_not_enriched(mode):
    body = golden("ns", mode)
    assert body == plain("ns")
    assert "@context" not in json.dumps(body)
    assert "@id" not in json.dumps(body)
    assert "@type" not in json.dumps(body)
    assert len(to_triples(body)) == NS_TRIPLES


def test_plain_response_has_no_triples():
    # Without `@context` JSON-LD has no terms, so a response that is not enriched gives no RDF.
    assert to_triples(plain("single")) == set()
    assert to_triples(plain("collection")) == set()


@pytest.mark.parametrize("name", PLAIN_CASES)
def test_removing_the_jsonld_keys_gives_the_plain_response(name, mode):
    # The backward compatibility check (9.5): the enriched response, without what JSON-LD adds, is the plain one.
    enriched = golden(name, mode)
    assert strip_jsonld(enriched, **PLAIN_CASES[name]) == plain(name)


def test_strip_removes_the_jsonld_keys_and_the_ref_type_only():
    body = {
        "@context": "x",
        "@id": "y",
        "@type": "z",
        "_type": "kept",
        "continent": {"@context": "x", "@id": "y", "@type": "z", "_type": "dropped", "_id": "kept"},
        "_data": [{"@id": "y", "_type": "kept", "_id": "kept"}],
    }
    assert strip_jsonld(body) == {
        "_type": "kept",
        "continent": {"_id": "kept"},
        "_data": [{"_type": "kept", "_id": "kept"}],
    }


def test_strip_removes_extra_keys_of_model_objects_only():
    body = {"_data": [{"_id": "a", "_type": "t", "code": "lt"}], "continent": {"_id": "a", "_type": "t"}}
    assert strip_jsonld(body, extra=("_id", "_type")) == {
        "_data": [{"code": "lt"}],
        "continent": {"_id": "a"},
    }


# Helper tests (9.4 acceptance).


def test_expected_keys_convert(mode):
    body = golden("single", mode)
    assert {"@context", "@id", "@type"} <= set(body)
    assert len(to_triples(body)) == SINGLE_TRIPLES


def test_missing_key_converts(mode):
    # Without `population` and `continent` the response still converts: the continent link and its class go too.
    body = golden("single", mode)
    del body["population"]
    del body["continent"]
    assert len(to_triples(body)) == SINGLE_TRIPLES - 3


def test_partial_response_converts(mode):
    # The whole-schema context (9) of a response with only `_id` and `code` gives the two triples of the object.
    body = golden("single", mode)
    body = {key: body[key] for key in ("@context", "@id", "@type", "_type", "_id", "code")}
    assert len(to_triples(body)) == 2


def _with_id_mapping(body, mode, monkeypatch):
    """The body with `_id` mapped to `@id`, in the context of the body (inline) or the served document (URL)."""
    if mode == INLINE:
        body["@context"]["_id"] = "@id"
        return body
    documents = served_documents()
    documents[f"{CONTEXT_URL}/{COUNTRY}.json"]["@context"]["_id"] = "@id"
    monkeypatch.setattr(helpers, "served_documents", lambda: documents)
    return body


def test_colliding_keywords_fail(mode, monkeypatch):
    # Finding 2: `_id` maps to `@id`, and the object has its own `@id`. The processor refuses it.
    body = _with_id_mapping(golden("single", mode), mode, monkeypatch)
    with pytest.raises(CollidingKeywords, match="colliding keywords"):
        to_triples(body)


def test_rdflib_alone_does_not_detect_colliding_keywords():
    # Why the harness checks it itself: rdflib accepts the body and takes `_id` as the subject, without an error.
    body = {
        "@context": {"_id": "@id", "name": "https://schema.org/name"},
        "@id": f"{UR}/id/{COUNTRY}/{COUNTRY_ID}",
        "_id": COUNTRY_ID,
        "name": "Lithuania",
    }
    graph = Graph().parse(data=json.dumps(body), format="json-ld")
    assert len(graph) == 1
    assert not any(str(s) == body["@id"] for s, _, _ in graph)


def test_unserved_context_url_fails_without_http():
    body = {"@context": f"{CONTEXT_URL}/datasets/gov/vssa/demo/Nope.json", "@id": f"{UR}/id/x"}
    with pytest.raises(UnservedContext):
        to_triples(body)


def test_external_context_url_is_not_fetched():
    body = {"@context": "https://example.invalid/context.json", "@id": f"{UR}/id/x"}
    with pytest.raises(UnservedContext):
        to_triples(body)


def test_literals_compare_by_value():
    body = {
        "@context": {"schema": SCHEMA, "name": {"@id": "schema:name", "@container": "@language"}},
        "@id": f"{UR}/id/x",
        "name": {"EN": "Lithuania"},
    }
    body_plain = {
        "@context": {"schema": SCHEMA, "name": {"@id": "schema:name", "@type": "xsd:string"}},
        "@id": f"{UR}/id/x",
        "name": "Lithuania",
    }
    body_plain["@context"]["xsd"] = "http://www.w3.org/2001/XMLSchema#"
    assert to_triples(body) == {(URIRef(f"{UR}/id/x"), URIRef(SCHEMA + "name"), Literal("Lithuania", lang="en"))}
    assert to_triples(body_plain) == {(URIRef(f"{UR}/id/x"), URIRef(SCHEMA + "name"), Literal("Lithuania"))}


# The context documents (9.4: the URL of a generated context loads in the harness, and task 5 gives the same values).


def test_every_model_context_is_served_under_the_url_of_task_5(rc, tmp_path):
    path = tmp_path / "spec.csv"
    path.write_text(SPEC_CSV, encoding="utf-8")
    url_context, models = _load(rc, path, jsonld_context_url=CONTEXT_URL)
    inline_context, _ = _load(rc, path)
    documents = served_documents()

    assert set(bundle()) == set(models)
    for name in bundle():
        url = context_value(url_context, models[name])
        assert url == f"{CONTEXT_URL}/{name}.json"
        assert url in documents, url
        assert documents[url] == bundle()[name]
        assert context_value(inline_context, models[name]) == bundle()[name]["@context"]

    assert protocol_context_value(url_context) == PROTOCOL_URL
    assert protocol_context_value(inline_context) == PROTOCOL_CONTEXT
    assert documents[PROTOCOL_URL] == {"@context": PROTOCOL_CONTEXT}


def test_inline_mode_writes_the_documents_in_place_of_the_urls():
    inline = golden("single", INLINE)
    assert inline["@context"] == bundle()[COUNTRY]["@context"]
    assert inline["continent"]["@context"] == bundle()[CONTINENT]["@context"]
    assert golden("collection", INLINE)["@context"] == PROTOCOL_CONTEXT
    assert golden("single", URL)["@context"] == f"{CONTEXT_URL}/{COUNTRY}.json"


def test_each_golden_is_in_the_expected_shape():
    # The golden shapes that the renderers (tasks 7 to 14) are compared with.
    assert "@context" not in plain("single")
    assert golden("single")["@id"] == f"{UR}/id/{COUNTRY}/{COUNTRY_ID}"
    assert golden("single")["@type"] == SCHEMA + "Country"
    assert golden("collection")["@context"] == PROTOCOL_URL
    assert golden("all")["@context"] == PROTOCOL_URL
    assert golden("select")["_data"][0]["_id"] == COUNTRY_ID
    assert "_id" not in plain("select")["_data"][0]
