"""The example manifest of `.tmp/specs/json-ld.md` section 1: the `addresses` vocabulary, and `datasets/gov/vssa/demo`
based on it. Shared by the context tests and the CLI tests."""

import csv
import io

UR = "https://data.gov.lt"
SCHEMA = "https://schema.org/"
XSD = "http://www.w3.org/2001/XMLSchema#"
GSP = "http://www.opengis.net/ont/geosparql#"

# A base row (`base=...`) sets the base of the models below it, until a `/` row closes it.
SPEC_ROWS = [
    dict(dataset="addresses", type="vocabulary"),
    dict(type="prefix", ref="schema", uri=SCHEMA),
    dict(model="Place", ref="name", uri="schema:Place"),
    dict(property="name", type="string", uri="schema:name"),
    dict(base="/addresses/Place", ref="name"),
    dict(model="Continent", ref="name", uri="schema:Continent"),
    dict(property="name", type="string"),
    dict(base="/"),
    dict(model="Country", ref="code", uri="schema:Country"),
    dict(property="code", type="string", uri="schema:identifier"),
    dict(property="name", type="text", uri="schema:name"),
    dict(property="population", type="integer", uri="schema:population"),
    dict(property="founded", type="date", uri="schema:foundingDate"),
    dict(property="location", type="geometry", uri="schema:geo"),
    dict(property="continent", type="ref", ref="Continent", uri="schema:containedInPlace"),
    dict(property="address", type="object", uri="schema:address"),
    dict(property="address.city", type="string", uri="schema:addressLocality"),
    dict(property="tags[]", type="string", uri="schema:keywords"),
    dict(base="/"),
    dict(model="Region", ref="name"),
    dict(property="name", type="string"),
    dict(dataset="datasets/gov/vssa/demo"),
    dict(type="prefix", ref="schema", uri=SCHEMA),
    dict(base="/addresses/Continent", ref="name"),
    dict(model="Continent", ref="name"),
    dict(property="name", type="string"),
    dict(base="/addresses/Country", ref="code"),
    dict(model="Country", ref="code"),
    dict(property="code", type="string"),
    dict(property="name", type="text"),
    dict(property="population", type="integer"),
    dict(property="founded", type="date"),
    dict(property="location", type="geometry"),
    dict(property="continent", type="ref", ref="Continent"),
    dict(property="flag", type="file", visibility="private"),
    dict(property="address", type="object"),
    dict(property="address.city", type="string"),
    dict(property="tags[]", type="string"),
    dict(property="updated", type="datetime", visibility="private"),
    dict(base="/addresses/Region", ref="name"),
    dict(model="Region", ref="name"),
    dict(property="name", type="string"),
]


def table(rows: list[dict[str, str]]) -> str:
    """A CSV manifest from rows given as dicts. The columns are the keys, in the order they first appear."""
    columns = list(dict.fromkeys(key for row in rows for key in row))
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


SPEC_CSV = table(SPEC_ROWS)
