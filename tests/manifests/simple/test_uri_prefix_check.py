"""The prefix of a CURIE in a `uri` or an `eli` must be declared (`spinta.dimensions.prefix.checks`).

The prefix is declared in the dataset of the element, or in the manifest, or it is one of `KNOWN_PREFIXES`. An IRI and a
value without a colon have no prefix, so they are not checked.
"""

import csv
import io

import pytest

from spinta.lib import Spinta

ADDR = "datasets/gov/rc/ar/uapi/addr"

DCT = "http://purl.org/dc/terms/"
LOCN = "http://www.w3.org/ns/locn#"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"
SKOS = "http://www.w3.org/2004/02/skos/core#"


def table(*rows: dict[str, str]) -> str:
    """A CSV manifest from rows given as dicts. The columns are the keys, in the order they first appear."""
    columns = list(dict.fromkeys(key for row in rows for key in row))
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def _check(spinta: Spinta, text: str) -> list[dict]:
    return spinta.check(spinta.copy(text.encode("utf-8"), "csv"))


def _codes(errors: list[dict]) -> list[str]:
    return [e["code"] for e in errors]


@pytest.fixture(scope="module")
def spinta():
    return Spinta()


def test_declared_prefixes_have_no_errors(spinta):
    """The first example of the issue: all the prefixes are declared, so the check passes."""
    errors = _check(
        spinta,
        table(
            dict(dataset=ADDR),
            dict(type="prefix", ref="dct", uri=DCT),
            dict(type="prefix", ref="rdfs", uri=RDFS),
            dict(type="prefix", ref="skos", uri=SKOS),
            dict(type="prefix", ref="locn", uri=LOCN),
            dict(model="City", ref="id", uri="locn:Location"),
            dict(property="id", type="integer", uri="dct:identifier"),
            dict(property="code", type="string", uri="skos:notation"),
            dict(property="name@lt", type="string", uri="rdfs:label"),
        ),
    )

    assert errors == []


def test_undeclared_prefixes_are_one_error_each(spinta):
    """The second example of the issue: none of the prefixes is declared, so each `uri` gives an error."""
    errors = _check(
        spinta,
        table(
            dict(dataset=ADDR),
            dict(model="City", ref="id", uri="locn:Location"),
            dict(property="id", type="integer", uri="dct:identifier"),
            dict(property="code", type="string", uri="skos:notation"),
            dict(property="name@lt", type="string", uri="rdfs:label"),
        ),
    )

    assert _codes(errors) == ["UndeclaredUriPrefix"] * 4
    assert [(e["context"]["prefix"], e["context"]["value"], e["key"]) for e in errors] == [
        ("locn", "locn:Location", "uri"),
        ("dct", "dct:identifier", "uri"),
        ("skos", "skos:notation", "uri"),
        ("rdfs", "rdfs:label", "uri"),
    ]
    assert errors[0]["message"] == (
        "Prefix 'locn' of model uri 'locn:Location' is not declared in the dataset or the manifest."
    )


def test_error_names_the_element_that_has_the_uri(spinta):
    errors = _check(
        spinta,
        table(
            dict(dataset=ADDR),
            dict(model="City", ref="id", uri="locn:Location"),
            dict(property="id", type="integer", uri="dct:identifier"),
        ),
    )

    assert [(e["context"]["model"], e["context"].get("property"), e["context"]["owner"]) for e in errors] == [
        (f"{ADDR}/City", None, "model"),
        (f"{ADDR}/City", "id", "property"),
    ]


def test_undeclared_eli_prefix_is_an_error(spinta):
    errors = _check(
        spinta,
        table(
            dict(dataset=ADDR),
            dict(model="City", ref="id", eli="eli:rc/city"),
            dict(property="id", type="integer"),
        ),
    )

    assert _codes(errors) == ["UndeclaredEliPrefix"]
    assert errors[0]["key"] == "eli"
    assert errors[0]["context"]["value"] == "eli:rc/city"


def test_declared_eli_prefix_has_no_error(spinta):
    errors = _check(
        spinta,
        table(
            dict(dataset=ADDR),
            dict(type="prefix", ref="eli", uri="http://data.europa.eu/eli/ontology#"),
            dict(model="City", ref="id", eli="eli:rc/city"),
            dict(property="id", type="integer"),
        ),
    )

    assert errors == []


def test_iri_and_relative_values_are_not_checked(spinta):
    """`https://...` and a value without a colon have no prefix, so they are not checked."""
    errors = _check(
        spinta,
        table(
            dict(dataset=ADDR),
            dict(model="City", ref="id", uri="https://example.com/City", eli="https://data.europa.eu/eli/x"),
            dict(property="id", type="integer", uri="/relative/id"),
        ),
    )

    assert errors == []


def test_known_prefix_needs_no_declaration(spinta):
    """`schema` is one of the `KNOWN_PREFIXES`, which a `uri` may use without declaring it."""
    errors = _check(
        spinta,
        table(
            dict(dataset=ADDR),
            dict(model="City", ref="id", uri="schema:Place"),
            dict(property="id", type="integer"),
        ),
    )

    assert errors == []


def test_manifest_prefix_is_declared_for_every_dataset(spinta):
    errors = _check(
        spinta,
        table(
            dict(type="prefix", ref="dct", uri=DCT),
            dict(dataset=ADDR),
            dict(model="City", ref="id", uri="dct:Thing"),
            dict(property="id", type="integer"),
        ),
    )

    assert errors == []


def test_prefix_of_another_dataset_is_not_declared(spinta):
    """A prefix is declared in its dataset only, so another dataset that uses it gives an error."""
    errors = _check(
        spinta,
        table(
            dict(dataset="datasets/gov/a"),
            dict(type="prefix", ref="ex", uri="http://example.com/"),
            dict(model="A", ref="id", uri="ex:A"),
            dict(property="id", type="integer"),
            dict(dataset="datasets/gov/b"),
            dict(model="B", ref="id", uri="ex:B"),
            dict(property="id", type="integer"),
        ),
    )

    assert _codes(errors) == ["UndeclaredUriPrefix"]
    assert errors[0]["context"]["model"] == "datasets/gov/b/B"


def test_scope_and_enum_item_uris_are_checked(spinta):
    errors = _check(
        spinta,
        table(
            dict(dataset=ADDR),
            dict(model="City", ref="id", eli="eli:rc/city"),
            dict(type="scope", ref="read", prepare="eq(id, 1)", eli="eli:rc/read"),
            dict(property="id", type="integer"),
            dict(property="kind", type="string"),
            dict(type="enum", source="s", prepare='"s"', uri="skos:small"),
        ),
    )

    assert [(e["code"], e["context"]["owner"]) for e in errors] == [
        ("UndeclaredEliPrefix", "model"),
        ("UndeclaredEliPrefix", "scope 'read'"),
        ("UndeclaredUriPrefix", "enum item 's'"),
    ]
    assert errors[2]["context"]["property"] == "kind"


def test_declared_prefix_of_a_model_with_no_dataset(spinta):
    """A model with no dataset has no dataset prefixes, the manifest prefixes are still declared."""
    errors = _check(
        spinta,
        table(
            dict(type="prefix", ref="dct", uri=DCT),
            dict(model="Thing", ref="id", uri="dct:Thing"),
            dict(property="id", type="integer"),
        ),
    )

    assert errors == []
