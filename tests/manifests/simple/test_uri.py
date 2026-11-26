"""The URIs that a `simple` document gives its elements (`spinta.manifests.simple.uri`, see `.tmp/specs/uri.md`).

A model has a `concept` and an `object`. A property and an enum item have one `uri`, their concept. Each rule is
checked here: R1 the element's own `uri`, R2 the `uri` of the nearest base that sets one, R3 to R5 a URI built from the
dataset that owns the class. A vocabulary owns the concepts, a registry owns the concepts and the objects, and a
vocabulary never owns an object. The `uri` configuration option is the base of every built URI.
"""

import csv
import io

import pytest

from spinta import commands
from spinta.lib import Spinta
from spinta.manifests.simple.check import load_document
from spinta.manifests.simple.implied import uri_context


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


UR = "https://data.gov.lt"


def _with_uri(rc, uri: str = UR) -> Spinta:
    return Spinta._from_context({"rc": rc.fork({"uri": uri})})


def test_model_based_on_a_vocabulary_gets_the_vocabularys_concept(rc):
    # Region of `datasets/gov/vssa/demo` is based on the Region of the vocabulary `addresses`. The vocabulary owns the
    # class and its properties, the dataset owns the objects (uri.md, 10.8).
    doc = _simple(
        _with_uri(rc),
        table(
            dict(dataset="addresses", type="vocabulary"),
            dict(model="Region", ref="id"),
            dict(property="id", type="integer"),
            dict(property="name", type="string"),
            dict(dataset="datasets/gov/vssa/demo"),
            dict(base="/addresses/Region", ref="id"),
            dict(model="Region", ref="id"),
            dict(property="id", type="integer"),
            dict(property="name", type="string"),
        ),
    )

    assert _implied(doc, "model", "datasets/gov/vssa/demo/Region")["uri"] == {
        "concept": f"{UR}/def/addresses/Region",
        "object": f"{UR}/id/datasets/gov/vssa/demo/Region",
    }
    assert _implied(doc, "property", "datasets/gov/vssa/demo/Region/name")["uri"] == (f"{UR}/def/addresses/Region/name")


def test_model_of_a_vocabulary_without_base_is_built_from_its_own_dataset(rc):
    doc = _simple(
        _with_uri(rc),
        table(
            dict(dataset="addresses", type="vocabulary"),
            dict(model="Region", ref="id"),
            dict(property="id", type="integer"),
        ),
    )

    assert _implied(doc, "model", "addresses/Region")["uri"] == {
        "concept": f"{UR}/def/addresses/Region",
        "object": f"{UR}/id/addresses/Region",
    }


def test_vocabulary_deeper_than_registry_owns_the_concept_and_registry_owns_the_objects(spinta):
    # A is in `ds`, based on B in the registry `reg`, based on C in the vocabulary `voc`.
    doc = _simple(
        spinta,
        table(
            dict(dataset="voc", type="vocabulary"),
            dict(model="Country", ref="id"),
            dict(property="id", type="integer"),
            dict(dataset="reg", type="registry"),
            dict(base="/voc/Country", ref="id"),
            dict(model="Country", ref="id"),
            dict(property="id", type="integer"),
            dict(dataset="ds"),
            dict(base="/reg/Country", ref="id"),
            dict(model="Country", ref="id"),
            dict(property="id", type="integer"),
        ),
    )

    assert _implied(doc, "model", "ds/Country")["uri"] == {
        "concept": "/def/voc/Country",
        "object": "/id/reg/Country",
    }
    assert _implied(doc, "property", "ds/Country/id")["uri"] == "/def/voc/Country/id"


def test_object_owner_is_the_deepest_dataset_base_and_never_a_vocabulary(spinta):
    # A is in `ds`, based on B in the vocabulary `voc`, based on C in the dataset `base`. No registry.
    # The concept is the vocabulary's (R3). The objects go to `base`, the deepest dataset (R4 rule for objects).
    doc = _simple(
        spinta,
        table(
            dict(dataset="base"),
            dict(model="Country", ref="id"),
            dict(property="id", type="integer"),
            dict(dataset="voc", type="vocabulary"),
            dict(base="/base/Country", ref="id"),
            dict(model="Country", ref="id"),
            dict(property="id", type="integer"),
            dict(dataset="ds"),
            dict(base="/voc/Country", ref="id"),
            dict(model="Country", ref="id"),
            dict(property="id", type="integer"),
        ),
    )

    assert _implied(doc, "model", "ds/Country")["uri"] == {
        "concept": "/def/voc/Country",
        "object": "/id/base/Country",
    }


def test_an_explicit_uri_in_the_chain_gives_the_concept_and_only_the_concept(rc):
    doc = _simple(
        _with_uri(rc),
        table(
            dict(dataset="addresses", type="vocabulary"),
            dict(type="prefix", ref="schema", uri="https://schema.org/"),
            dict(model="Country", ref="id", uri="schema:Country"),
            dict(property="id", type="integer", uri="schema:identifier"),
            dict(property="name", type="string"),
            dict(dataset="datasets/gov/vssa/demo"),
            dict(base="/addresses/Country", ref="id"),
            dict(model="Country", ref="id"),
            dict(property="id", type="integer"),
            dict(property="name", type="string"),
            dict(property="flag", type="file", visibility="private"),
        ),
    )

    assert _implied(doc, "model", "datasets/gov/vssa/demo/Country")["uri"] == {
        "concept": "https://schema.org/Country",
        "object": f"{UR}/id/datasets/gov/vssa/demo/Country",
    }
    assert _implied(doc, "property", "datasets/gov/vssa/demo/Country/id")["uri"] == "https://schema.org/identifier"
    # `name` is in the vocabulary, so the vocabulary owns it (R3 for the property).
    assert _implied(doc, "property", "datasets/gov/vssa/demo/Country/name")["uri"] == (
        f"{UR}/def/addresses/Country/name"
    )
    # `flag` is in no base, so its own dataset owns it (R5). Its IRI is still computed.
    assert _implied(doc, "property", "datasets/gov/vssa/demo/Country/flag")["uri"] == (
        f"{UR}/def/datasets/gov/vssa/demo/Country/flag"
    )


def test_spinta_uri_option_gives_the_iris_of_the_configuration_fork(rc, monkeypatch):
    monkeypatch.setenv("SPINTA_URI", "https://env.example")
    text = table(
        dict(dataset="addresses", type="vocabulary"),
        dict(model="Region", ref="id"),
        dict(property="id", type="integer"),
        dict(dataset="ds"),
        dict(base="/addresses/Region", ref="id"),
        dict(model="Region", ref="id"),
        dict(property="id", type="integer"),
    )

    option = _simple(Spinta(uri=UR), text)
    fork = _simple(_with_uri(rc), text)

    assert option["model"] == fork["model"]
    assert option["property"] == fork["property"]
    assert _implied(option, "model", "ds/Region")["uri"]["object"] == f"{UR}/id/ds/Region"
    # The library reads no environment variable: without the option the URIs are relative.
    assert _implied(_simple(Spinta(), text), "model", "ds/Region")["uri"]["object"] == "/id/ds/Region"


def test_spinta_refuses_other_options_and_a_uri_that_is_not_a_string():
    with pytest.raises(ValueError, match="Unknown option"):
        Spinta(other="x")
    with pytest.raises(ValueError, match="`uri` takes a string"):
        Spinta(uri=None)


def test_instance_base_and_class_iri_are_the_implied_values(rc):
    text = table(
        dict(dataset="addresses", type="vocabulary"),
        dict(model="Region", ref="id"),
        dict(property="id", type="integer"),
        dict(dataset="ds"),
        dict(base="/addresses/Region", ref="id"),
        dict(model="Region", ref="id"),
        dict(property="id", type="integer"),
    )
    spinta = _with_uri(rc)
    doc = _simple(spinta, text)
    loaded = load_document(spinta._rc, doc, check_names=None, strict_references=False)
    uris = uri_context(loaded.context, loaded.manifest)
    model = commands.get_models(loaded.context, loaded.manifest)["ds/Region"]

    assert uris.class_iri(model) == doc["model"]["ds/Region"]["implied"]["uri"]["concept"]
    assert uris.instance_base(model) == doc["model"]["ds/Region"]["implied"]["uri"]["object"]
    assert uris.instance_id(model, "abc") == f"{UR}/id/ds/Region/abc"


def test_array_items_take_the_uri_of_the_same_item_in_their_base(rc):
    # `tags[]` of `ds/Country` has no uri, so it is the item of its base (R2). The items are found by their name in
    # the document, as the other properties are, so the base's `schema:keywords` is used, not a URI of `ds`.
    doc = _simple(
        _with_uri(rc),
        table(
            dict(dataset="addresses", type="vocabulary"),
            dict(type="prefix", ref="schema", uri="https://schema.org/"),
            dict(model="Country", ref="id"),
            dict(property="id", type="integer"),
            dict(property="tags[]", type="string", uri="schema:keywords"),
            dict(dataset="ds"),
            dict(base="/addresses/Country", ref="id"),
            dict(model="Country", ref="id"),
            dict(property="id", type="integer"),
            dict(property="tags[]", type="string"),
        ),
    )

    assert _implied(doc, "property", "ds/Country/tags[]")["uri"] == "https://schema.org/keywords"
    assert _implied(doc, "property", "addresses/Country/tags[]")["uri"] == "https://schema.org/keywords"
