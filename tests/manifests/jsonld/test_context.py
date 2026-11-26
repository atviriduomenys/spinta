"""The JSON-LD context documents of a manifest (`spinta.manifests.jsonld`, `.tmp/specs/json-ld.md` section 3.3).

The goldens are the contexts of section 3.3 and the `Continent` of 5.1. The IRIs of every term are the `implied.uri`
values of the `simple` output of the same manifest, so the two outputs agree. Each row type of the mapping table has a
test on its own.
"""

import copy
import json
import logging

import pytest

from spinta.lib import Spinta
from tests.manifests.jsonld.helpers import GSP, SCHEMA, SPEC_CSV, UR, XSD, table

DEMO = "datasets/gov/vssa/demo"
PREFIX_NAMES = {"schema", "xsd", "gsp"}


def _copy(text: str, fmt: str = "jsonld", **options) -> str:
    return Spinta(uri=UR).copy(Spinta(uri=UR).copy(text.encode("utf-8"), "csv"), fmt, **options)


def _bundle(text: str = SPEC_CSV, **options) -> dict:
    return json.loads(_copy(text, **options))


def _contexts_iris(context: dict, path: str = "") -> dict[str, str]:
    """The IRI of each term of a context, by its path (`address.city`, `tags[]`), with compact IRIs expanded."""
    iris = {}
    for key, term in context.items():
        if key in PREFIX_NAMES:
            continue
        name = f"{path}.{key}" if path else key
        if isinstance(term, str):
            iris[name] = _expand(term)
            continue
        if "@container" in term and "@set" in _as_list(term["@container"]):
            name = f"{name}[]"
        iris[name] = _expand(term["@id"])
        if "@context" in term:
            iris.update(_contexts_iris(term["@context"], name.removesuffix("[]")))
    return iris


def _as_list(value):
    return value if isinstance(value, list) else [value]


def _expand(iri: str) -> str:
    if iri.startswith("schema:"):
        return SCHEMA + iri[len("schema:") :]
    return iri


def _implied_uris(text: str = SPEC_CSV) -> dict[str, str]:
    doc = Spinta(uri=UR).copy(text.encode("utf-8"), "csv")
    return {path: el["implied"]["uri"] for path, el in doc["property"].items()}


# The goldens of section 3.3 and 5.1.

COUNTRY_CONTEXT = {
    "gsp": GSP,
    "schema": SCHEMA,
    "xsd": XSD,
    "code": "schema:identifier",
    "name": {"@id": "schema:name", "@container": "@language"},
    "population": {"@id": "schema:population", "@type": "xsd:integer"},
    "founded": {"@id": "schema:foundingDate", "@type": "xsd:date"},
    "location": {"@id": "schema:geo", "@type": "gsp:wktLiteral"},
    "continent": "schema:containedInPlace",
    "address": {"@id": "schema:address", "@context": {"city": "schema:addressLocality"}},
    "tags": {"@id": "schema:keywords", "@container": "@set"},
}

CONTINENT_CONTEXT = {"schema": SCHEMA, "name": "schema:name"}


def test_country_context_is_the_golden_of_section_3_3():
    context = _bundle(model=f"{DEMO}/Country")
    assert context == {"@context": COUNTRY_CONTEXT}


def test_continent_context_is_the_golden_of_section_5_1():
    context = _bundle(model=f"{DEMO}/Continent")
    assert context == {"@context": CONTINENT_CONTEXT}


def test_flag_and_updated_are_private_so_they_have_no_term():
    terms = _bundle(model=f"{DEMO}/Country")["@context"]
    assert "flag" not in terms
    assert "updated" not in terms


def test_bundle_has_one_key_per_model_in_scope():
    # Every model of the example is public, so each has a context. Keys are model names, values are the documents.
    bundle = _bundle()
    assert sorted(bundle) == [
        "addresses/Continent",
        "addresses/Country",
        "addresses/Place",
        "addresses/Region",
        f"{DEMO}/Continent",
        f"{DEMO}/Country",
        f"{DEMO}/Region",
    ]
    assert bundle[f"{DEMO}/Country"] == {"@context": COUNTRY_CONTEXT}
    assert all(list(document) == ["@context"] for document in bundle.values())


def test_bundle_of_one_dataset_has_only_its_models():
    bundle = _bundle(dataset=DEMO)
    assert sorted(bundle) == [f"{DEMO}/Continent", f"{DEMO}/Country", f"{DEMO}/Region"]


def test_model_option_inside_dataset_option_gives_that_model():
    assert _bundle(dataset=DEMO, model=f"{DEMO}/Country") == {"@context": COUNTRY_CONTEXT}


def test_region_name_is_built_from_the_vocabulary_that_owns_the_concept():
    # `Region` has no `uri`. Its property is built from the vocabulary of its base (R3, 10.8).
    context = _bundle(model=f"{DEMO}/Region")["@context"]
    assert context == {"schema": SCHEMA, "name": f"{UR}/def/addresses/Region/name"}


def test_iris_of_every_term_are_the_implied_uris_of_the_same_manifest():
    implied = _implied_uris()
    checked = 0
    for model_name, document in _bundle().items():
        for path, iri in _contexts_iris(document["@context"]).items():
            assert iri == implied[f"{model_name}/{path}"], (model_name, path)
            checked += 1
    # Place 1, Continent 1, Country 9 (with `address.city`), Region 1; the same for the datasets, with Continent 1.
    assert checked == 1 + 1 + 9 + 1 + 1 + 9 + 1


def test_terms_of_arrays_take_the_iri_of_their_items():
    # `tags` is an array: its term is the IRI of `tags[]`, where `uri` is given.
    implied = _implied_uris()
    assert implied[f"{DEMO}/Country/tags[]"] == SCHEMA + "keywords"
    assert _bundle(model=f"{DEMO}/Country")["@context"]["tags"]["@id"] == "schema:keywords"


def test_context_is_the_same_bytes_each_time():
    assert _copy(SPEC_CSV) == _copy(SPEC_CSV)


def test_simple_input_with_and_without_implied_gives_the_same_output():
    # `implied` may be `null` in a `simple` document (a document written without it). The IRIs then come from the
    # nodes of the same call, so the output is the same.
    doc = Spinta(uri=UR).copy(SPEC_CSV.encode("utf-8"), "csv")
    given_only = copy.deepcopy(doc)
    for elements in given_only.values():
        if isinstance(elements, dict):
            for element in elements.values():
                if isinstance(element, dict) and "implied" in element:
                    element["implied"] = None
    spinta = Spinta(uri=UR)
    assert spinta.copy(doc, "jsonld") == spinta.copy(given_only, "jsonld")


def test_uri_option_gives_https_iris_and_no_uri_gives_relative_ones(caplog):
    doc = Spinta(uri=UR).copy(SPEC_CSV.encode("utf-8"), "csv")
    with_uri = Spinta(uri=UR).copy(doc, "jsonld", model=f"{DEMO}/Region")
    assert json.loads(with_uri)["@context"]["name"] == f"{UR}/def/addresses/Region/name"

    caplog.set_level(logging.WARNING, logger="spinta.manifests.jsonld.writer")
    without_uri = Spinta().copy(doc, "jsonld", model=f"{DEMO}/Region")
    assert json.loads(without_uri)["@context"]["name"] == "/def/addresses/Region/name"
    assert "`uri` is empty" in caplog.text


def test_empty_uri_warns_for_the_bundle_too(caplog):
    caplog.set_level(logging.WARNING, logger="spinta.manifests.jsonld.writer")
    Spinta().copy(Spinta().copy(SPEC_CSV.encode("utf-8"), "csv"), "jsonld")
    assert "`uri` is empty" in caplog.text


def test_model_option_refuses_unknown_model():
    with pytest.raises(ValueError, match="not found"):
        _copy(SPEC_CSV, model=f"{DEMO}/Nope")


def test_model_option_refuses_model_outside_dataset():
    with pytest.raises(ValueError, match="not in dataset"):
        _copy(SPEC_CSV, model=f"{DEMO}/Country", dataset="addresses")


@pytest.mark.parametrize("fmt", ["csv", "simple", "xlsx", "ascii", "rdf", "mermaid"])
def test_model_option_refuses_other_outputs(fmt):
    with pytest.raises(ValueError, match="does not apply"):
        Spinta(uri=UR).copy(Spinta(uri=UR).copy(SPEC_CSV.encode("utf-8"), "csv"), fmt, model=f"{DEMO}/Country")


def test_model_option_refuses_model_without_context():
    rows = [
        dict(dataset="ds", type="registry"),
        dict(type="prefix", ref="schema", uri=SCHEMA),
        dict(model="Secret", ref="id", visibility="private"),
        dict(property="id", type="integer", uri="schema:id"),
        dict(model="Open", ref="id", uri="schema:Open"),
        dict(property="id", type="integer", uri="schema:id"),
    ]
    text = table(rows)
    spinta = Spinta(uri=UR)
    doc = spinta.copy(text.encode("utf-8"), "csv")

    with pytest.raises(ValueError, match="has no context"):
        spinta.copy(doc, "jsonld", model="ds/Secret")
    # A private model is not in the bundle, and the public one is.
    assert sorted(json.loads(spinta.copy(doc, "jsonld"))) == ["ds/Open"]


def test_private_and_protected_model_refs_and_properties_have_no_term():
    rows = [
        dict(dataset="ds", type="registry"),
        dict(type="prefix", ref="schema", uri=SCHEMA),
        dict(model="Person", ref="id", uri="schema:Person"),
        dict(property="id", type="integer", uri="schema:id"),
        dict(property="name", type="string", uri="schema:name"),
        dict(property="hidden", type="string", uri="schema:hidden", visibility="private"),
        dict(property="guarded", type="string", uri="schema:guarded", visibility="protected"),
        dict(property="public", type="string", uri="schema:public", visibility="public"),
        dict(property="package", type="string", uri="schema:package", visibility="package"),
        dict(property="note", type="string", uri="schema:note"),
        dict(property="secret_ref", type="ref", ref="Secret", uri="schema:secretRef"),
        dict(property="open_ref", type="ref", ref="Open", uri="schema:openRef"),
        dict(model="Secret", ref="id", uri="schema:Secret", visibility="protected"),
        dict(property="id", type="integer"),
        dict(model="Open", ref="id", uri="schema:Open"),
        dict(property="id", type="integer"),
    ]
    spinta = Spinta(uri=UR)
    doc = spinta.copy(table(rows).encode("utf-8"), "csv")
    terms = json.loads(spinta.copy(doc, "jsonld", model="ds/Person"))["@context"]

    assert "hidden" not in terms
    assert "guarded" not in terms
    assert terms["public"] == "schema:public"
    assert terms["package"] == "schema:package"
    assert terms["note"] == "schema:note"
    assert "secret_ref" not in terms, "a ref to a protected model has no term (10.2)"
    assert terms["open_ref"] == "schema:openRef"


def test_each_row_type_of_the_mapping_table_has_its_term():
    # One property of each row type. The model is public and has the `schema` prefix of its dataset.
    rows = [
        dict(dataset="ds", type="registry"),
        dict(type="prefix", ref="schema", uri=SCHEMA),
        dict(model="Thing", ref="id", uri="schema:Thing"),
        dict(property="id", type="integer", uri="schema:id"),
        dict(property="title", type="text", uri="schema:name"),
        dict(property="count", type="integer", uri="schema:count"),
        dict(property="ratio", type="number", uri="schema:ratio"),
        dict(property="active", type="boolean", uri="schema:active"),
        dict(property="born", type="date", uri="schema:birthDate"),
        dict(property="created", type="datetime", uri="schema:dateCreated"),
        dict(property="opens", type="time", uri="schema:openingTime"),
        dict(property="area", type="geometry", uri="schema:geo"),
        dict(property="owner", type="ref", ref="Person", uri="schema:owner"),
        dict(property="address", type="object", uri="schema:address"),
        dict(property="address.city", type="string", uri="schema:addressLocality"),
        dict(property="photo", type="file", uri="schema:image"),
        dict(property="tags[]", type="string", uri="schema:keywords"),
        dict(property="scores[]", type="integer", uri="schema:score"),
        dict(property="labels[]", type="text", uri="schema:alternateName"),
        dict(property="owners[]", type="ref", ref="Person", uri="schema:owners"),
        dict(property="children[]", type="object", uri="schema:children"),
        dict(property="children[].first", type="string", uri="schema:givenName"),
        dict(model="Person", ref="id", uri="schema:Person"),
        dict(property="id", type="integer", uri="schema:id"),
    ]
    spinta = Spinta(uri=UR)
    doc = spinta.copy(table(rows).encode("utf-8"), "csv")
    context = json.loads(spinta.copy(doc, "jsonld", model="ds/Thing"))["@context"]

    assert context["schema"] == SCHEMA
    assert context["id"] == {"@id": "schema:id", "@type": "xsd:integer"}
    assert context["title"] == {"@id": "schema:name", "@container": "@language"}
    assert context["count"] == {"@id": "schema:count", "@type": "xsd:integer"}
    assert context["ratio"] == {"@id": "schema:ratio", "@type": "xsd:decimal"}
    assert context["active"] == {"@id": "schema:active", "@type": "xsd:boolean"}
    assert context["born"] == {"@id": "schema:birthDate", "@type": "xsd:date"}
    assert context["created"] == {"@id": "schema:dateCreated", "@type": "xsd:dateTime"}
    assert context["opens"] == {"@id": "schema:openingTime", "@type": "xsd:time"}
    assert context["area"] == {"@id": "schema:geo", "@type": "gsp:wktLiteral"}
    assert context["owner"] == "schema:owner"
    assert context["address"] == {"@id": "schema:address", "@context": {"city": "schema:addressLocality"}}
    assert context["photo"] == "schema:image"
    assert context["tags"] == {"@id": "schema:keywords", "@container": "@set"}
    assert context["scores"] == {"@id": "schema:score", "@type": "xsd:integer", "@container": "@set"}
    assert context["labels"] == {"@id": "schema:alternateName", "@container": ["@set", "@language"]}
    assert context["owners"] == {"@id": "schema:owners", "@container": "@set"}
    assert context["children"] == {
        "@id": "schema:children",
        "@container": "@set",
        "@context": {"first": "schema:givenName"},
    }
    assert {"xsd", "gsp"} <= set(context)


def test_nothing_of_the_protocol_has_a_term():
    terms = _bundle(model=f"{DEMO}/Country")["@context"]
    for name in ("_id", "_type", "_revision", "_data", "_page", "_content_type"):
        assert name not in terms


def test_datatype_prefixes_are_added_only_when_used():
    context = _bundle(model=f"{DEMO}/Continent")["@context"]
    assert "xsd" not in context
    assert "gsp" not in context
