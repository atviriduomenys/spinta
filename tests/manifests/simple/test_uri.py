"""The URIs that a `simple` document gives its elements (`spinta.manifests.simple.uri`, see `.tmp/specs/uri.md`).

A model has a `concept` and an `object`. A property and an enum item have one `uri`, their concept. Each rule is
checked here: R1 the element's own `uri`, R2 the `uri` of the nearest base that sets one, R3 to R5 a URI built from the
dataset that owns the class. The `uri` configuration option is the base of every built URI.
"""

import csv
import io

import pytest

from spinta.lib import Spinta


def table(*rows: dict[str, str]) -> str:
    """A CSV manifest from rows given as dicts. The columns are the keys, in the order they first appear."""
    columns = list(dict.fromkeys(key for row in rows for key in row))
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def _simple(spinta: Spinta, text: str) -> dict:
    return spinta.copy(text.encode("utf-8"), "csv")


def _ascii(spinta: Spinta, text: str) -> dict:
    return spinta.copy(text.encode("utf-8"), "ascii")


def _implied(doc: dict, type_: str, path: str) -> dict:
    return doc[type_][path]["implied"]


@pytest.fixture(scope="module")
def spinta():
    return Spinta()


def test_r1_the_uri_of_the_element_is_used_with_its_prefix_expanded(spinta):
    doc = _simple(
        spinta,
        table(
            dict(dataset="ds"),
            dict(type="prefix", ref="ex", uri="http://example.com/"),
            dict(model="Country", ref="id", uri="ex:Country"),
            dict(property="id", type="integer", uri="ex:id"),
        ),
    )

    assert _implied(doc, "model", "ds/Country")["uri"] == {
        "concept": "http://example.com/Country",
        "object": "/id/ds/Country",
    }
    assert _implied(doc, "property", "ds/Country/id")["uri"] == "http://example.com/id"


def test_r5_without_base_or_uri_is_built_from_the_own_dataset(spinta):
    doc = _simple(
        spinta,
        table(
            dict(dataset="ds"),
            dict(model="City", ref="id"),
            dict(property="id", type="integer"),
            dict(property="name", type="string"),
        ),
    )

    assert _implied(doc, "model", "ds/City")["uri"] == {
        "concept": "/def/ds/City",
        "object": "/id/ds/City",
    }
    assert _implied(doc, "property", "ds/City/name")["uri"] == "/def/ds/City/name"


def test_r5_model_without_dataset_has_no_dataset_segment(spinta):
    doc = _simple(spinta, table(dict(model="Plain", ref="id"), dict(property="id", type="integer")))

    assert _implied(doc, "model", "Plain")["uri"] == {"concept": "/def/Plain", "object": "/id/Plain"}


def test_r3_concept_comes_from_the_registry_of_the_deepest_base(spinta):
    # A is in `ds`, based on B in `mid`, which is based on C in the registry `reg`. Nothing sets a uri.
    doc = _simple(
        spinta,
        table(
            dict(dataset="reg", type="registry"),
            dict(model="Country", ref="id"),
            dict(property="id", type="integer"),
            dict(dataset="mid"),
            dict(base="/reg/Country", ref="id"),
            dict(model="Country", ref="id"),
            dict(property="id", type="integer"),
            dict(dataset="ds"),
            dict(base="/mid/Country", ref="id"),
            dict(model="Country", ref="id"),
            dict(property="id", type="integer"),
        ),
    )

    assert _implied(doc, "model", "ds/Country")["uri"] == {
        "concept": "/def/reg/Country",
        "object": "/id/reg/Country",
    }
    assert _implied(doc, "property", "ds/Country/id")["uri"] == "/def/reg/Country/id"


def test_r4_without_registry_the_deepest_dataset_base_owns_the_class(spinta):
    # Same chain as above, but `reg` is a plain dataset. `mid` is nearer, but `reg` is the deepest one.
    doc = _simple(
        spinta,
        table(
            dict(dataset="reg"),
            dict(model="Country", ref="id"),
            dict(property="id", type="integer"),
            dict(dataset="mid"),
            dict(base="/reg/Country", ref="id"),
            dict(model="Country", ref="id"),
            dict(property="id", type="integer"),
            dict(dataset="ds"),
            dict(base="/mid/Country", ref="id"),
            dict(model="Country", ref="id"),
            dict(property="id", type="integer"),
        ),
    )

    assert _implied(doc, "model", "ds/Country")["uri"]["concept"] == "/def/reg/Country"
    assert _implied(doc, "model", "ds/Country")["uri"]["object"] == "/id/reg/Country"


def test_enum_of_a_property_is_a_concept_of_the_property_scheme(spinta):
    doc = _ascii(
        spinta,
        """\
d  | m       | property | type    | ref | source | prepare | title
ds |         |          |         |     |        |         |
   | Country |          |         | id  |        |         |
   |         | id       | integer |     |        |         |
   |         | kind     | string  |     |        |         |
   |         |          | enum    |     | s      | "s"     | Small
   |         |          |         |     | l      | "l"     | Large
""",
    )

    assert _implied(doc, "enum", "ds/Country/kind/s")["uri"] == "/def/ds/Country/kind/s"
    assert _implied(doc, "enum", "ds/Country/kind/l")["uri"] == "/def/ds/Country/kind/l"


def test_uri_option_is_the_base_of_every_built_uri(rc):
    # `Spinta` takes no options, so the configuration is a fork of the test configuration (3.6).
    spinta = Spinta._from_context({"rc": rc.fork({"uri": "https://data.gov.lt"})})
    doc = _simple(
        spinta,
        table(
            dict(dataset="ds"),
            dict(model="Country", ref="id"),
            dict(property="id", type="integer"),
        ),
    )

    assert _implied(doc, "model", "ds/Country")["uri"] == {
        "concept": "https://data.gov.lt/def/ds/Country",
        "object": "https://data.gov.lt/id/ds/Country",
    }
    assert _implied(doc, "property", "ds/Country/id")["uri"] == "https://data.gov.lt/def/ds/Country/id"
