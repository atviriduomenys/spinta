"""Inheritance of `status`, `visibility`, `access`, `uri`, `eli`, `title` and `description` (`spinta.manifests.inherit`).

An element that does not set a value takes it from its nearest base that sets it. A `simple` output fills in the
inherited values in `implied` and keeps `given` as written. Other outputs write the inherited values as if they
were given.
"""

import csv
import io

import pytest

from spinta.lib import Spinta
from spinta.manifests.simple.schema import validate_document

ADDR = "datasets/gov/rc/ar/uapi/addr"
COUNTRY = f"{ADDR}/Country"

# The example of the task, as an ASCII table: `base.txt` and `dsa.txt`.
BASE_TXT = """\
d | r | b | m | property     | type    | ref     | uri                                  | title
locn                         |         |         |                                      |
  |   |   |   |              | prefix  | locn    | http://www.w3.org/ns/locn#           |
  |   |   |   |              |         | dct     | http://purl.org/dc/terms/            |
  |   |   |   |              |         | skos    | http://www.w3.org/2004/02/skos/core# |
  |   |   | Country          |         | id      | locn:Country                         | Country
  |   |   |   | id           | integer |         | dct:identifier                       | Identifier
  |   |   |   | code         | string  |         | skos:notation                        | Code
"""

DSA_TXT = """\
d | r | b | m | property     | type    | ref
datasets/gov/rc/ar/uapi/addr |         |
  |   | /locn/Country        |         | id
  |   |   | Country          |         | id
  |   |   |   | id           | integer |
  |   |   |   | code         | string  |
  |   |   |   | name@lt      | string  |
"""


def table(*rows: dict[str, str]) -> str:
    """A CSV manifest from rows given as dicts. The columns are the keys, in the order they first appear."""
    columns = list(dict.fromkeys(key for row in rows for key in row))
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


LOCN = table(
    dict(dataset="locn"),
    dict(type="prefix", ref="locn", uri="http://www.w3.org/ns/locn#"),
    dict(type="prefix", ref="dct", uri="http://purl.org/dc/terms/"),
    dict(type="prefix", ref="skos", uri="http://www.w3.org/2004/02/skos/core#"),
    dict(model="Country", ref="id", uri="locn:Country", title="Country"),
    dict(property="id", type="integer", uri="dct:identifier", title="Identifier"),
    dict(property="code", type="string", uri="skos:notation", title="Code"),
)

DSA = table(
    dict(dataset=ADDR),
    dict(base="/locn/Country", ref="id"),
    dict(model="Country", ref="id"),
    dict(property="id", type="integer"),
    dict(property="code", type="string"),
    dict(property="name@lt", type="string"),
)

# The same model without its base row: it is matched to the default base (`copy -b`).
DSA_NO_BASE = table(
    dict(dataset=ADDR),
    dict(model="Country", ref="id"),
    dict(property="id", type="integer"),
    dict(property="code", type="string"),
    dict(property="name@lt", type="string"),
)


@pytest.fixture
def spinta():
    return Spinta()


def _simple(spinta, text: str) -> dict:
    return spinta.copy(text.encode("utf-8"), "csv")


def _ascii(spinta, text: str) -> dict:
    return spinta.copy(text.encode("utf-8"), "ascii")


def _csv_rows(spinta, docs) -> list[dict[str, str]]:
    """The rows of the CSV output of `docs`, each with the dataset and the model it belongs to."""
    data = spinta.copy(docs, "csv").decode("utf-8")
    rows = []
    dataset = model = None
    for row in csv.DictReader(io.StringIO(data)):
        if row["dataset"]:
            dataset, model = row["dataset"], None
        if row["model"]:
            model = row["model"]
        rows.append({**row, "_dataset": dataset, "_model": model})
    return rows


def _model_row(rows, dataset: str, model: str) -> dict[str, str]:
    [row] = [r for r in rows if r["_dataset"] == dataset and r["model"] == model]
    return row


def _property_row(rows, dataset: str, model: str, prop: str) -> dict[str, str]:
    [row] = [r for r in rows if r["_dataset"] == dataset and r["_model"] == model and r["property"] == prop]
    return row


def _prefixes(rows, dataset: str) -> dict[str, str]:
    """The prefixes of a dataset: the rows that give a `uri` and name no model or property."""
    return {
        r["ref"]: r["uri"]
        for r in rows
        if r["_dataset"] == dataset and r["uri"] and not r["model"] and not r["property"]
    }


def _implied(doc: dict, type_: str, path: str) -> dict:
    return doc[type_][path]["implied"]


def test_example_of_the_task_is_written_with_the_inherited_values(spinta):
    docs = [_ascii(spinta, DSA_TXT), _ascii(spinta, BASE_TXT)]
    rows = _csv_rows(spinta, docs)

    country = _model_row(rows, ADDR, "Country")
    assert country["uri"] == "locn:Country"
    assert country["title"] == "Country"
    assert _property_row(rows, ADDR, "Country", "id")["uri"] == "dct:identifier"
    assert _property_row(rows, ADDR, "Country", "id")["title"] == "Identifier"
    assert _property_row(rows, ADDR, "Country", "code")["uri"] == "skos:notation"
    assert _property_row(rows, ADDR, "Country", "code")["title"] == "Code"
    # A property that the base does not have inherits nothing.
    assert _property_row(rows, ADDR, "Country", "name@lt")["uri"] == ""
    assert _property_row(rows, ADDR, "Country", "name@lt")["title"] == ""
    # The prefixes that the inherited CURIEs use are added to the dataset, in the order they are needed.
    assert list(_prefixes(rows, ADDR)) == ["locn", "dct", "skos"]
    # The base of the model is still written, as it was given.
    assert any(r["base"] == "/locn/Country" for r in rows)


def test_simple_output_keeps_given_and_fills_in_implied(spinta):
    doc = spinta.copy([_simple(spinta, DSA), _simple(spinta, LOCN)], "simple")

    country = doc["model"][COUNTRY]
    assert country["given"].get("title") is None
    assert country["given"].get("uri") is None
    assert country["implied"]["title"] == "Country"
    assert country["implied"]["uri"]["concept"] == "http://www.w3.org/ns/locn#Country"
    assert _implied(doc, "property", f"{COUNTRY}/id")["uri"] == "http://purl.org/dc/terms/identifier"
    validate_document(doc)


def test_given_value_is_not_replaced_by_the_base(spinta):
    dsa = table(
        dict(dataset=ADDR),
        dict(base="/locn/Country", ref="id"),
        dict(model="Country", ref="id", title="Land"),
        dict(property="id", type="integer", uri="dct:identifier"),
    )
    doc = spinta.copy([_simple(spinta, dsa), _simple(spinta, LOCN)], "simple")

    assert _implied(doc, "model", COUNTRY)["title"] == "Land"
    assert _implied(doc, "model", COUNTRY)["uri"]["concept"] == "http://www.w3.org/ns/locn#Country"
    assert _implied(doc, "property", f"{COUNTRY}/id")["uri"] == "http://purl.org/dc/terms/identifier"


def test_nearest_base_that_sets_the_value_wins(spinta):
    # C sets a title and a uri. B sets a title. A sets nothing, so it takes the title of B, and the uri of C,
    # which B does not set and so gets from C.
    text = table(
        dict(dataset="ds"),
        dict(model="C", ref="id", uri="https://example.com/c", title="C"),
        dict(property="id", type="integer"),
        dict(base="C", ref="id"),
        dict(model="B", ref="id", title="B"),
        dict(property="id", type="integer"),
        dict(base="B", ref="id"),
        dict(model="A", ref="id"),
        dict(property="id", type="integer"),
    )
    doc = _simple(spinta, text)

    assert _implied(doc, "model", "ds/B")["title"] == "B"
    assert _implied(doc, "model", "ds/B")["uri"]["concept"] == "https://example.com/c"
    assert _implied(doc, "model", "ds/A")["title"] == "B"
    assert _implied(doc, "model", "ds/A")["uri"]["concept"] == "https://example.com/c"


def test_base_access_is_taken_before_the_access_of_the_owner(spinta):
    # A base row stays open for the models after it, so N comes before the base row of K.
    text = table(
        dict(dataset="ds", access="open"),
        dict(model="C", ref="id", access="public"),
        dict(property="id", type="integer"),
        dict(model="N", ref="id"),
        dict(property="id", type="integer"),
        dict(base="C", ref="id"),
        dict(model="K", ref="id"),
        dict(property="id", type="integer"),
    )
    doc = _simple(spinta, text)

    # K has no access and its base sets one: the base's access, not the access of the dataset.
    assert _implied(doc, "model", "ds/K")["access"] == "public"
    # N has no base: it takes the access of its dataset, as before.
    assert _implied(doc, "model", "ds/N")["access"] == "open"


def test_property_takes_status_visibility_and_eli_of_its_base_property(spinta):
    text = table(
        dict(dataset="ds"),
        dict(model="C", ref="id", status="completed", visibility="public", eli="https://example.com/eli/c"),
        dict(property="id", type="integer", visibility="package", eli="https://example.com/eli/id"),
        dict(property="name", type="string"),
        dict(base="C", ref="id"),
        dict(model="K", ref="id"),
        dict(property="id", type="integer"),
        dict(property="name", type="string"),
    )
    doc = _simple(spinta, text)

    kid = _implied(doc, "model", "ds/K")
    assert kid["status"] == "completed"
    assert kid["visibility"] == "public"
    assert kid["eli"] == "https://example.com/eli/c"
    assert _implied(doc, "property", "ds/K/id")["visibility"] == "package"
    assert _implied(doc, "property", "ds/K/id")["eli"] == "https://example.com/eli/id"
    assert _implied(doc, "property", "ds/K/name")["status"] is None


def test_dataset_and_resource_are_not_inherited(spinta):
    locn = table(
        dict(dataset="locn", access="open", title="Location", description="Location dataset"),
        dict(resource="locn_src", title="Locn source", access="open"),
        dict(model="Country", ref="id", title="Country"),
        dict(property="id", type="integer"),
    )
    dsa = table(
        dict(dataset=ADDR),
        dict(resource="addr_src"),
        dict(base="/locn/Country", ref="id"),
        dict(model="Country", ref="id"),
        dict(property="id", type="integer"),
    )
    doc = spinta.copy([_simple(spinta, dsa), _simple(spinta, locn)], "simple")

    # The model takes the title of its base, but the dataset and the resource take nothing from theirs.
    assert _implied(doc, "model", COUNTRY)["title"] == "Country"
    assert _implied(doc, "dataset", ADDR)["access"] == "private"
    assert "title" not in _implied(doc, "dataset", ADDR)
    assert "description" not in _implied(doc, "dataset", ADDR)
    assert _implied(doc, "resource", f"{ADDR}/addr_src")["access"] == "private"
    assert "title" not in _implied(doc, "resource", f"{ADDR}/addr_src")
    assert doc["resource"][f"{ADDR}/addr_src"]["given"] == {}


def test_existing_prefix_with_another_name_is_reused(spinta):
    dsa = table(
        dict(dataset=ADDR),
        dict(type="prefix", ref="loc", uri="http://www.w3.org/ns/locn#"),
        dict(base="/locn/Country", ref="id"),
        dict(model="Country", ref="id"),
        dict(property="id", type="integer"),
    )
    rows = _csv_rows(spinta, [_simple(spinta, dsa), _simple(spinta, LOCN)])

    assert _model_row(rows, ADDR, "Country")["uri"] == "loc:Country"
    # `locn` is not added again: the dataset has the same IRI under `loc`. Only `dct` is added for `id`.
    assert _prefixes(rows, ADDR) == {
        "loc": "http://www.w3.org/ns/locn#",
        "dct": "http://purl.org/dc/terms/",
    }


def test_prefix_name_taken_by_another_iri_gives_the_full_iri(spinta):
    dsa = table(
        dict(dataset=ADDR),
        dict(type="prefix", ref="dct", uri="http://example.com/other#"),
        dict(base="/locn/Country", ref="id"),
        dict(model="Country", ref="id"),
        dict(property="id", type="integer"),
    )
    rows = _csv_rows(spinta, [_simple(spinta, dsa), _simple(spinta, LOCN)])

    assert _property_row(rows, ADDR, "Country", "id")["uri"] == "http://purl.org/dc/terms/identifier"
    assert _prefixes(rows, ADDR)["dct"] == "http://example.com/other#"


def test_base_uri_with_an_undeclared_prefix_is_not_inherited(spinta):
    locn = table(
        dict(dataset="locn"),
        dict(model="Country", ref="id", uri="foo:Country", title="Country"),
        dict(property="id", type="integer"),
    )
    doc = spinta.copy([_simple(spinta, DSA), _simple(spinta, locn)], "simple")

    country = _implied(doc, "model", COUNTRY)
    assert country["title"] == "Country"
    assert country["uri"]["concept"] == "/def/locn/Country"


def test_default_base_values_are_inherited_and_written(spinta):
    doc = spinta.copy(_simple(spinta, DSA_NO_BASE), "simple", base=[_simple(spinta, LOCN)])

    assert _implied(doc, "model", COUNTRY)["title"] == "Country"
    assert _implied(doc, "model", COUNTRY)["uri"]["concept"] == "http://www.w3.org/ns/locn#Country"

    data = spinta.copy(_simple(spinta, DSA_NO_BASE), "csv", base=[_simple(spinta, LOCN)])
    rows = list(csv.DictReader(io.StringIO(data.decode("utf-8"))))
    [country] = [r for r in rows if r["model"] == "Country" and r["title"] == "Country"]
    assert country["uri"] == "locn:Country"
    # The prefix that the value uses is in the dataset that is written.
    assert _prefixes(_csv_rows(spinta, [_simple(spinta, DSA), _simple(spinta, LOCN)]), ADDR)["locn"] == (
        "http://www.w3.org/ns/locn#"
    )


def test_check_reports_the_inherited_values(spinta):
    doc = spinta._check_document([_simple(spinta, DSA), _simple(spinta, LOCN)])

    assert _implied(doc, "model", COUNTRY)["title"] == "Country"
