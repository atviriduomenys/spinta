"""Building a `simple` document from table rows: read errors and where they go (3.1, 4.10)."""

import io

from spinta.manifests.simple.from_tabular import tabular_to_simple
from spinta.manifests.simple.schema import validate_document
from spinta.manifests.tabular.helpers import _read_ascii_tabular_manifest, _read_csv_manifest, striptable

CSV = """\
dataset,resource,base,model,property,type,ref,source
datasets/gov/example,,,,,dataset,,
,,,City,,,,
,,,,name,string,,cities
,,,,age,integer,,
"""

ASCII = striptable("""
    d | r | b | m | property                 | type    | ref | access | title
    example                                  |         |     |        |
                                             |         |     |        |
      |   |   | Language                     |         |     |        |
      |   |   |   | dialect                  | string  |     | open   |
      |   |   |   | meta.version             | string  |     | open   |
      |   |   |   | meta                     | integer |     | open   |
""")


def _csv_doc(text):
    return _doc(_read_csv_manifest(None, io.StringIO(text)), "csv")


def _ascii_doc(text):
    return _doc(_read_ascii_tabular_manifest(io.StringIO(text)), "ascii")


def _doc(rows, type_):
    doc = tabular_to_simple(rows, type=type_, version="2.2.0")
    validate_document(doc)
    return doc


def _manifest(doc):
    """The one `manifest` element of a document read from one table."""
    assert len(doc["manifest"]) == 1
    return next(iter(doc["manifest"].values()))


def _elements(doc):
    for type_, elements in doc.items():
        if type_ not in ("spinta", "manifest"):
            yield from elements.items()


def test_header_gives_the_columns_type_and_version():
    doc = _csv_doc(CSV)
    manifest = _manifest(doc)
    assert manifest["columns"] == ["dataset", "resource", "base", "model", "property", "type", "ref", "source"]
    assert manifest["type"] == "csv"
    assert manifest["errors"] == []
    assert doc["spinta"] == {"version": "2.2.0", "format": 1}


def test_manifest_is_the_input_file_and_has_no_parent():
    doc = _csv_doc(CSV)
    assert list(doc["manifest"]) == ["<memory>"]
    manifest = _manifest(doc)
    assert manifest["source"] == "<memory>"
    assert manifest["parent"] is None
    assert manifest["index"] is None
    assert manifest["given"] is None


def test_top_level_elements_point_to_the_manifest():
    doc = _csv_doc(CSV)
    dataset = doc["dataset"]["datasets/gov/example"]
    assert dataset["parent"] == {"type": "manifest", "path": "<memory>"}
    # A model has its dataset as parent, not the manifest.
    assert doc["model"]["datasets/gov/example/City"]["parent"] == {
        "type": "dataset",
        "path": "datasets/gov/example",
    }


def test_source_is_the_input_path_when_there_is_one():
    doc = tabular_to_simple(
        _read_csv_manifest(None, io.StringIO(CSV)),
        type="csv",
        version="2.2.0",
        source="datasets/dsa.csv",
    )
    assert list(doc["manifest"]) == ["datasets/dsa.csv"]
    assert doc["dataset"]["datasets/gov/example"]["parent"] == {"type": "manifest", "path": "datasets/dsa.csv"}
    assert _manifest(doc)["source"] == "datasets/dsa.csv"


def test_clean_table_has_no_errors():
    doc = _csv_doc(CSV)
    assert all(element["errors"] == [] for _path, element in _elements(doc))


def test_index_is_the_data_row_number():
    doc = _csv_doc(CSV)
    assert doc["dataset"]["datasets/gov/example"]["index"] == 1
    assert doc["model"]["datasets/gov/example/City"]["index"] == 2
    assert doc["property"]["datasets/gov/example/City/name"]["index"] == 3
    assert doc["property"]["datasets/gov/example/City/age"]["index"] == 4


def test_implied_is_null_until_it_is_rebuilt():
    doc = _csv_doc(CSV)
    assert all(element["implied"] is None for _path, element in _elements(doc))


def test_element_error_is_stored_on_its_element():
    doc = _ascii_doc(ASCII)
    assert _manifest(doc)["type"] == "ascii"
    meta = doc["property"]["example/Language/meta"]
    assert [e["code"] for e in meta["errors"]] == ["DataTypeCannotBeUsedForNesting"]
    assert meta["index"] == 6
    assert meta["given"] == {"type": "integer", "access": "open"}
    assert doc["property"]["example/Language/dialect"]["errors"] == []


def test_wrong_cell_count_is_a_document_error_and_keeps_the_other_rows():
    doc = _csv_doc(CSV.replace(",,,,age,integer,,", ",,,,age,integer"))
    errors = _manifest(doc)["errors"]
    assert [e["code"] for e in errors] == ["TabularManifestError"]
    assert errors[0]["index"] == 4
    assert "Number of row cells" in errors[0]["message"]
    assert "datasets/gov/example/City/name" in doc["property"]
    assert "datasets/gov/example/City/age" not in doc["property"]


def test_duplicate_property_keeps_the_first_and_reports_the_second_row():
    doc = _csv_doc(CSV + ",,,,name,integer,,\n")
    errors = _manifest(doc)["errors"]
    assert [e["index"] for e in errors] == [5]
    assert "already defined" in errors[0]["message"]
    assert doc["property"]["datasets/gov/example/City/name"]["index"] == 3
    assert doc["property"]["datasets/gov/example/City/name"]["given"]["type"] == "string"


def test_no_readable_header_gives_an_empty_document_with_the_error():
    doc = _csv_doc("foo,bar\n1,2\n")
    assert _manifest(doc)["columns"] == []
    assert list(_elements(doc)) == []
    assert len(_manifest(doc)["errors"]) == 1
    assert "Unknown columns" in _manifest(doc)["errors"][0]["message"]


def test_two_dimensions_in_one_row_are_a_document_error_for_now():
    # 3.1 asks for such a row to be an element with its error. Which element
    # type a row with two dimensions has is not decided yet (open question),
    # so the error goes to `spinta.errors` with its row, and no element is made.
    doc = _csv_doc(CSV.replace(",,,City,,,,", "datasets/gov/example,,,City,,,,"))
    errors = _manifest(doc)["errors"]
    assert errors[0]["index"] == 2
    assert "single dimension" in errors[0]["message"]
    assert "datasets/gov/example/City" not in doc.get("model", {})
    # The properties below the row have no model, so they are reported too.
    assert {e["message"] for e in errors[1:]} == {"Property is not inside a model."}
