"""Reading a `simple` document (4.7) and its rows (4.8).

The document is the example of section 5.1 of `.tmp/specs/exchange.md`, written
out here, so the test checks the spec and not the code that produced it.
"""

import copy
import csv
import io
import json

import pytest

from spinta.manifests.simple.helpers import read_simple_rows, simple_to_rows
from spinta.manifests.simple.schema import validate_document
from spinta.manifests.simple.types import SimpleManifestError

COLUMNS = ["id", "dataset", "resource", "base", "model", "property", "type", "ref", "uri"]
DS = "datasets/gov/rc/ar/uapi/addr"
SOURCE = "dsa.csv"
MANIFEST = {"type": "manifest", "path": SOURCE}

# The CSV table of 5.1, as the user wrote it (blank rows are kept).
CSV_TABLE = """\
id,dataset,resource,base,model,property,type,ref,uri
1,datasets/gov/rc/ar/uapi/addr,,,,,,,
2,,,,,,prefix,dct,http://purl.org/dc/terms/
3,,,,,,,dcat,http://www.w3.org/ns/dcat#
4,,,,,,,foaf,http://xmlns.com/foaf/0.1/
5,,,,,,,rdfs,http://www.w3.org/2000/01/rdf-schema#
6,,,,,,,skos,http://www.w3.org/2004/02/skos/core#
7,,,,,,,cv,http://data.europa.eu/m8g
8,,,,Country,,,id,
9,,,,,id,integer,,
10,,,,,code,string,,
11,,,,,name@lt,string,,
12,,,,City,,,id,
13,,,,,id,integer,,
14,,,,,name@lt,string,,
15,,,,,country,ref,Country,
"""


def _el(index, parent, given, implied=None):
    return {
        "index": index,
        "parent": parent,
        "given": given,
        "implied": implied,
        "errors": [],
    }


def golden() -> dict:
    """The `simple` document of 5.1, `given` and `index` only."""
    doc = {
        "spinta": {"version": "2.2.0", "format": 1},
        "manifest": {
            SOURCE: {
                "index": None,
                "parent": None,
                "given": None,
                "implied": None,
                "errors": [],
                "type": "csv",
                "source": SOURCE,
                "columns": COLUMNS,
            },
        },
        "dataset": {DS: _el(1, MANIFEST, {"id": "1"})},
        "prefix": {},
        "model": {},
        "property": {},
    }
    dsp = {"type": "dataset", "path": DS}
    for i, (name, uri) in enumerate(
        [
            ("dct", "http://purl.org/dc/terms/"),
            ("dcat", "http://www.w3.org/ns/dcat#"),
            ("foaf", "http://xmlns.com/foaf/0.1/"),
            ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
            ("skos", "http://www.w3.org/2004/02/skos/core#"),
            ("cv", "http://data.europa.eu/m8g"),
        ],
        start=2,
    ):
        doc["prefix"][f"{DS}/{name}"] = _el(
            i,
            dsp,
            {"id": str(i), "uri": uri},
        )
    country = f"{DS}/Country"
    city = f"{DS}/City"
    doc["model"][country] = _el(8, dsp, {"id": "8", "ref": "id"})
    doc["model"][city] = _el(12, dsp, {"id": "12", "ref": "id"})
    mp = {"type": "model", "path": country}
    cp = {"type": "model", "path": city}
    doc["property"][f"{country}/id"] = _el(9, mp, {"id": "9", "type": "integer"})
    doc["property"][f"{country}/code"] = _el(10, mp, {"id": "10", "type": "string"})
    doc["property"][f"{country}/name"] = _el(None, mp, None)
    doc["property"][f"{country}/name@lt"] = _el(
        11,
        {"type": "property", "path": f"{country}/name"},
        {"id": "11", "type": "string"},
    )
    doc["property"][f"{city}/id"] = _el(13, cp, {"id": "13", "type": "integer"})
    doc["property"][f"{city}/name"] = _el(None, cp, None)
    doc["property"][f"{city}/name@lt"] = _el(
        14,
        {"type": "property", "path": f"{city}/name"},
        {"id": "14", "type": "string"},
    )
    doc["property"][f"{city}/country"] = _el(
        15,
        cp,
        {"id": "15", "type": "ref", "ref": "Country"},
    )
    return doc


def _csv_cells() -> list[list[str]]:
    return list(csv.reader(io.StringIO(CSV_TABLE)))


def _non_blank(rows):
    return [r for r in rows if any(r)]


def test_golden_document_is_valid():
    validate_document(golden(), COLUMNS)


def test_rows_equal_the_csv_cells():
    got = [cells for _line, cells in read_simple_rows(golden(), COLUMNS)]
    assert _non_blank(got) == _non_blank(_csv_cells())


def test_header_is_first_and_at_line_1():
    first_line, first_cells = next(iter(read_simple_rows(golden(), COLUMNS)))
    assert first_line == "1"
    assert first_cells == COLUMNS


def test_each_row_is_at_index_plus_one():
    lines = [(line, cells) for line, cells in read_simple_rows(golden(), COLUMNS)][1:]
    # CSV record number: the header is record 1, so element index 2 is record 3.
    by_id = {cells[0]: line for line, cells in lines}
    assert by_id["2"] == "3"
    assert by_id["15"] == "16"


def test_implied_elements_have_no_row():
    ids = [cells[0] for _line, cells in read_simple_rows(golden(), COLUMNS)][1:]
    # The implied text parent `City/name` has index null, so it has no id.
    assert "" not in ids
    assert len(ids) == 15  # the 15 data rows, the header excluded


def test_name_cell_is_rebuilt_from_path():
    rows = simple_to_rows(golden())
    by_id = {r["id"]: r for r in rows}
    assert by_id["1"]["dataset"] == DS
    assert by_id["8"]["model"] == "Country"
    assert by_id["9"]["property"] == "id"
    assert by_id["11"]["property"] == "name@lt"


def test_type_keyword_is_on_the_first_row_of_the_group_only():
    doc = golden()
    rows = simple_to_rows(doc)
    prefixes = [r for r in rows if r["ref"] in {"dct", "dcat", "foaf", "rdfs", "skos", "cv"}]
    assert [r["type"] for r in prefixes] == ["prefix", "", "", "", "", ""]


def test_type_keyword_restarts_for_a_new_owner():
    doc = golden()
    # A second prefix under a model starts a new group: it writes its own keyword.
    model = f"{DS}/Country"
    doc["prefix"][f"{model}/xsd"] = _el(
        16,
        {"type": "model", "path": model},
        {"id": "16", "uri": "http://www.w3.org/2001/XMLSchema#"},
    )
    rows = {r["id"]: r for r in simple_to_rows(doc)}
    assert rows["16"]["type"] == "prefix"


def test_keyword_is_restored_for_an_empty_type_cell_only():
    doc = golden()
    # The stored `given` never has the keyword (4.5). The reader writes it.
    for elem in doc["prefix"].values():
        elem["given"].pop("type", None)
    rows = {r["id"]: r for r in simple_to_rows(doc)}
    # The first row of the prefix group (`dct`, id 2) gets the keyword back.
    assert rows["2"]["ref"] == "dct"
    assert rows["2"]["type"] == "prefix"
    assert rows["3"]["type"] == ""


def test_element_without_index_is_skipped():
    doc = golden()
    implied = doc["property"][f"{DS}/Country/name"]
    assert implied["index"] is None
    ids = [cells[0] for _line, cells in read_simple_rows(doc, COLUMNS)]
    assert len(ids) == len(set(ids))


def test_rows_are_in_index_order():
    doc = golden()
    indexes = [int(line) for line, _cells in read_simple_rows(doc, COLUMNS)][1:]
    assert indexes == sorted(indexes)


def test_document_is_not_changed_by_reading():
    doc = golden()
    before = copy.deepcopy(doc)
    list(read_simple_rows(doc, COLUMNS))
    assert doc == before


def test_malformed_document_is_refused_before_reading():
    doc = golden()
    doc["widget"] = {}
    with pytest.raises(SimpleManifestError, match="Unknown element type"):
        list(read_simple_rows(doc, COLUMNS))


def test_json_round_trip_gives_the_same_rows():
    doc = golden()
    again = json.loads(json.dumps(doc))
    assert list(read_simple_rows(again, COLUMNS)) == list(read_simple_rows(doc, COLUMNS))
