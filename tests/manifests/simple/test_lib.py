"""The producer side of `spinta.lib.Spinta` (3.1, 3.3): copy from a raw table, errors.

The table is the CSV of 5.1 (`test_read.CSV_TABLE`). Every test compares rows
with `read_simple_rows`, so a test checks what a user reads back, not the
writer's own output.
"""

import csv
import io
import json
import threading

import openpyxl
import pytest

import spinta.lib as lib
from spinta.lib import Spinta
from spinta.manifests.simple.helpers import read_simple_rows
from spinta.manifests.simple.types import SimpleManifestError
from spinta.manifests.tabular.helpers import DATASET, TabularManifestError, striptable
from tests.manifests.simple.test_read import CSV_TABLE, golden


def _rows(doc):
    return [cells for _line, cells in read_simple_rows(doc)]


def _csv_rows(data: bytes) -> list[list[str]]:
    return [row for row in csv.reader(io.StringIO(data.decode("utf-8"))) if any(row)]


def _non_blank(rows):
    return [r for r in rows if any(r)]


def _manifest(doc):
    """The one `manifest` element of a document read from one input."""
    assert len(doc["manifest"]) == 1
    return next(iter(doc["manifest"].values()))


def _without_implied(doc):
    """The document with every `implied` removed. `copy` recomputes `implied` (4.6), so a test that compares
    documents compares what the user wrote, and `test_implied` checks the computed keys."""
    return {
        type_: (
            {path: {k: v for k, v in element.items() if k != "implied"} for path, element in elements.items()}
            if type_ != "spinta"
            else elements
        )
        for type_, elements in doc.items()
    }


@pytest.fixture
def spinta():
    return Spinta()


def test_threads_sharing_one_spinta_get_the_serial_results(spinta):
    data = CSV_TABLE.encode("utf-8")
    expected = spinta.copy(data, "csv")
    results = []
    errors = []

    def run():
        try:
            results.append(spinta.copy(data, "csv"))
        except Exception as e:  # noqa: BLE001 - reported below
            errors.append(e)

    threads = [threading.Thread(target=run) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    assert results == [expected] * 8


def test_version_is_the_running_spinta_version(spinta):
    import spinta as package

    assert spinta.version == package.__version__


def test_csv_source_copies_to_the_golden_document(spinta):
    doc = spinta.copy(CSV_TABLE.encode("utf-8"), "csv")
    expected = golden()
    assert _manifest(doc)["columns"] == _manifest(expected)["columns"]
    assert _manifest(doc)["type"] == "csv"
    assert _non_blank(_rows(doc)) == _non_blank(_rows(expected))


def test_path_is_detected_from_its_extension(spinta, tmp_path):
    path = tmp_path / "manifest.csv"
    path.write_text(CSV_TABLE, encoding="utf-8")
    doc = spinta.copy(path)
    assert _manifest(doc)["type"] == "csv"
    assert _manifest(doc)["source"] == str(path)
    assert _non_blank(_rows(doc)) == _non_blank(_rows(golden()))


def test_open_file_needs_a_format(spinta):
    with pytest.raises(ValueError, match="format"):
        spinta.copy(io.BytesIO(CSV_TABLE.encode("utf-8")))


def test_open_file_is_read_with_its_format(spinta):
    doc = spinta.copy(io.StringIO(CSV_TABLE), "csv")
    assert _non_blank(_rows(doc)) == _non_blank(_rows(golden()))


def test_csv_output_is_the_rendered_table(spinta):
    # Rendered from the nodes, as `spinta copy` writes it (D14): every column of the table, no BOM.
    doc = spinta.copy(CSV_TABLE.encode("utf-8"), "csv")
    data = spinta.copy(doc, "csv")
    assert isinstance(data, bytes)
    assert not data.startswith(b"\xef\xbb\xbf")
    rows = _non_blank(_csv_rows(data))
    assert rows[0][:2] == ["id", "dataset"]
    assert rows[1][1] == "datasets/gov/rc/ar/uapi/addr"


def test_rendered_csv_is_a_fixed_point(spinta):
    # Reading the rendered table and rendering it again gives the same bytes.
    table = spinta.copy(spinta.copy(CSV_TABLE.encode("utf-8"), "csv"), "csv")
    assert spinta.copy(spinta.copy(table, "csv"), "csv") == table


def test_rendered_ascii_is_a_fixed_point(spinta):
    doc = spinta.copy(CSV_TABLE.encode("utf-8"), "csv")
    text = spinta.copy(doc, "ascii")
    assert isinstance(text, str)
    assert spinta.copy(spinta.copy(text.encode("utf-8"), "ascii"), "ascii") == text


def test_ascii_source_is_read_with_its_format(spinta):
    table = striptable("""
        d | r | b | m | property | type    | ref | source
        example |  |  |  |  | dataset |  |
          |  |  | City |  |  |  |
          |  |  |  | name | string |  |
    """)
    doc = spinta.copy(table.encode("utf-8"), "ascii")
    assert _manifest(doc)["type"] == "ascii"
    assert _manifest(doc)["errors"] == []
    assert list(doc["model"]) == ["example/City"]
    assert list(doc["property"]) == ["example/City/name"]


def test_xlsx_source_is_read_with_the_standard_columns(spinta):
    # The XLSX reader always uses the standard columns (4.1), so the table is
    # written with them.
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    ws.append(list(DATASET))
    for cells in (
        {"dataset": "example", "type": "dataset"},
        {"model": "City"},
        {"property": "name", "type": "string"},
    ):
        ws.append([cells.get(column) for column in DATASET])
    buffer = io.BytesIO()
    wb.save(buffer)
    doc = spinta.copy(buffer.getvalue(), "xlsx")
    assert _manifest(doc)["type"] == "xlsx"
    assert _manifest(doc)["columns"] == list(DATASET)
    assert _manifest(doc)["errors"] == []
    assert list(doc["property"]) == ["example/City/name"]


def test_simple_text_is_read_and_validated(spinta):
    text = json.dumps(golden())
    doc = spinta.copy(text.encode("utf-8"), "simple")
    assert _without_implied(doc) == _without_implied(golden())


def test_simple_text_that_is_not_json_is_refused(spinta):
    with pytest.raises(SimpleManifestError):
        spinta.copy(b"{not json", "simple")


def test_simple_output_is_a_copy_in_document_order(spinta):
    doc = golden()
    out = spinta.copy(doc)
    assert _without_implied(out) == _without_implied(doc)
    assert out is not doc
    assert list(out["model"]) == list(doc["model"])


def test_simple_output_with_a_malformed_document_is_refused(spinta):
    doc = golden()
    doc["spinta"]["format"] = 99
    with pytest.raises(SimpleManifestError):
        spinta.copy(doc, "csv")


def test_unknown_format_is_a_value_error(spinta):
    with pytest.raises(ValueError, match="Unknown input format"):
        spinta.copy(CSV_TABLE.encode("utf-8"), "yaml")


@pytest.mark.parametrize("fmt", ["xml", "gsheets"])
def test_format_that_cannot_be_written_is_a_value_error(spinta, fmt):
    with pytest.raises(ValueError, match="Unknown output format"):
        spinta.copy(golden(), fmt)


def test_mermaid_cannot_be_read(spinta):
    with pytest.raises(ValueError, match="Unknown input format"):
        spinta.copy(b"classDiagram", "mermaid")


def test_format_that_is_not_implemented_yet_is_refused(spinta):
    with pytest.raises(NotImplementedError):
        spinta.copy(b"<xml/>", "xml")
    with pytest.raises(NotImplementedError):
        spinta.copy(CSV_TABLE.encode("utf-8"), "gsheets")


def test_unknown_extension_needs_a_format(spinta, tmp_path):
    path = tmp_path / "manifest.yaml"
    path.write_text(CSV_TABLE, encoding="utf-8")
    with pytest.raises(ValueError, match="format"):
        spinta.copy(path)


@pytest.mark.parametrize(
    "fmt, option",
    [
        ("csv", {"dataset": "datasets/gov/rc/ar/uapi/addr"}),
        ("csv", {"columns": ["id"]}),
        ("xlsx", {"columns": ["id"]}),
        ("ascii", {"dataset": "datasets/gov/rc/ar/uapi/addr"}),
        ("mermaid", {"source": False}),
        ("mermaid", {"access": "open"}),
        ("mermaid", {"columns": ["id"]}),
        ("rdf", {"source": False}),
    ],
)
def test_option_that_the_output_does_not_take_is_refused_not_ignored(spinta, fmt, option):
    with pytest.raises(ValueError, match="does not apply"):
        spinta.copy(golden(), fmt, **option)


def test_option_not_implemented_yet_is_refused(spinta):
    with pytest.raises(NotImplementedError):
        spinta.copy(golden(), "csv", include=[golden()])


def test_simple_result_refuses_renames_until_they_are_implemented(spinta):
    with pytest.raises(NotImplementedError):
        spinta.copy(golden(), "simple", format_names=True)


def test_simple_result_refuses_output_only_options(spinta):
    with pytest.raises(ValueError, match="does not apply to a `simple` result"):
        spinta.copy(golden(), "simple", access="open")


def test_mermaid_output_refuses_source_as_the_cli_always_did(spinta):
    with pytest.raises(ValueError, match="does not apply to `mermaid` output"):
        spinta.copy(golden(), "mermaid", source=False)


def test_list_of_documents_is_merged_before_it_is_written(spinta):
    # Identical elements are kept once, so the merged table is the table of one document.
    assert spinta.copy([golden(), golden()], "csv") == spinta.copy(golden(), "csv")


def test_empty_list_has_nothing_to_copy(spinta):
    with pytest.raises(ValueError):
        spinta.copy([], "csv")


def test_single_document_list_is_copied(spinta):
    assert _without_implied(spinta.copy([golden()])) == _without_implied(golden())


def test_config_is_refused():
    with pytest.raises(ValueError, match="`config`"):
        Spinta(config="spinta.yml")


def test_unknown_option_is_refused():
    with pytest.raises(ValueError, match="Unknown option"):
        Spinta(check_names=False)


def test_errors_are_the_stored_errors_in_document_order(spinta):
    doc = golden()
    doc["model"]["datasets/gov/rc/ar/uapi/addr/City"]["errors"] = [_err("A", index=12)]
    doc["property"]["datasets/gov/rc/ar/uapi/addr/City/id"]["errors"] = [_err("B", index=13)]
    _manifest(doc)["errors"] = [_err("C", index=None)]
    got = spinta.errors(doc)
    # The manifest is the first element of the document (4.1), so its errors come first.
    assert [e["code"] for e in got] == ["C", "A", "B"]


def test_errors_filter_by_one_code_or_a_list(spinta):
    doc = golden()
    doc["property"]["datasets/gov/rc/ar/uapi/addr/City/id"]["errors"] = [_err("A", index=13)]
    _manifest(doc)["errors"] = [_err("A", index=None), _err("B", index=None)]
    assert [e["code"] for e in spinta.errors(doc, code="A")] == ["A", "A"]
    assert [e["code"] for e in spinta.errors(doc, code=["B"])] == ["B"]
    assert [e["code"] for e in spinta.errors(doc, code=("A", "B"))] == ["A", "B", "A"]


def test_errors_is_empty_for_a_document_with_no_errors(spinta):
    assert spinta.errors(golden()) == []
    assert spinta.errors(golden(), code="UnresolvedReference") == []


def test_errors_of_a_list_are_the_errors_of_its_documents(spinta):
    first, second = golden(), golden()
    _manifest(first)["errors"] = [_err("A", index=None)]
    _manifest(second)["errors"] = [_err("B", index=None)]
    assert [e["code"] for e in spinta.errors([first, second])] == ["A", "B"]


def test_read_error_of_a_row_is_stored_on_its_element(spinta):
    table = striptable("""
        d | r | b | m | property                 | type    | ref | access | title
        example                                  |         |     |        |
                                                 |         |     |        |
          |   |   | Language                     |         |     |        |
          |   |   |   | dialect                  | string  |     | open   |
          |   |   |   | meta.version             | string  |     | open   |
          |   |   |   | meta                     | integer |     | open   |
    """)
    doc = spinta.copy(table.encode("utf-8"), "ascii")
    errors = spinta.errors(doc)
    assert [e["code"] for e in errors] == ["DataTypeCannotBeUsedForNesting"]
    assert doc["property"]["example/Language/meta"]["errors"] == errors
    assert errors[0]["index"] == 6


# A row whose nested property is an integer: it cannot be read, so the read stops there.
BAD_ROW_TABLE = striptable("""
    d | r | b | m | property                 | type    | ref | access | title
    example                                  |         |     |        |
                                             |         |     |        |
      |   |   | Language                     |         |     |        |
      |   |   |   | dialect                  | string  |     | open   |
      |   |   |   | meta.version             | string  |     | open   |
      |   |   |   | meta                     | integer |     | open   |
""")

# A reference to a model that is not in the manifest: a warning, the table is still written.
UNRESOLVED_TABLE = striptable("""
    d | r | b | m | property | type   | ref  | source | prepare | access
    datasets/gov/example     |        |      |        |         |
      | data                 | sql    |      |        |         |
                             |        |      |        |         |
      |   |   | City         |        | name |        |         |
      |   |   |   | name     | string |      |        |         | open
      |   |   |   | country  | ref    | Nope |        |         | open
""")


def test_on_error_gets_each_error_of_a_raw_source_and_marks_the_row_that_stopped_it(spinta):
    seen = []
    doc = spinta.copy(
        BAD_ROW_TABLE.encode("utf-8"),
        "ascii",
        on_error=lambda error, raised: seen.append((error, raised)),
    )
    assert seen == [(error, True) for error in spinta.errors(doc)]
    assert seen[0][0]["index"] == 6


def test_on_error_gets_the_stored_errors_of_a_document_and_does_not_stop(spinta):
    doc = spinta.copy(BAD_ROW_TABLE.encode("utf-8"), "ascii")
    seen = []
    assert spinta.copy(doc, "simple", on_error=lambda error, raised: seen.append((error, raised))) == doc
    assert seen == [(error, False) for error in spinta.errors(doc)]


def test_table_stopped_by_a_row_is_reported_as_raised_and_not_written(spinta):
    doc = spinta.copy(BAD_ROW_TABLE.encode("utf-8"), "ascii")
    seen = []
    result = spinta.copy(doc, "csv", on_error=lambda error, raised: seen.append((error["code"], raised)))
    assert result is None
    assert seen == [("TabularManifestError", True)]


def test_table_stopped_by_a_row_is_raised_without_on_error(spinta):
    doc = spinta.copy(BAD_ROW_TABLE.encode("utf-8"), "ascii")
    with pytest.raises(TabularManifestError):
        spinta.copy(doc, "csv")


def test_table_reports_its_warnings_and_is_still_written(spinta):
    doc = spinta.copy(UNRESOLVED_TABLE.encode("utf-8"), "ascii")
    seen = []
    data = spinta.copy(doc, "csv", on_error=lambda error, raised: seen.append((error["code"], raised)))
    assert isinstance(data, bytes)
    assert seen == [("UnresolvedReference", False)]


def test_simple_output_stores_the_unresolved_reference_as_a_warning(spinta):
    # The warning that a table output reports (3.4), stored on its property. The row is kept.
    doc = spinta.copy(UNRESOLVED_TABLE.encode("utf-8"), "ascii")
    errors = spinta.errors(doc)
    assert [(e["code"], e["severity"], e["key"]) for e in errors] == [("UnresolvedReference", "warning", "ref")]
    assert [el["errors"] for el in doc["property"].values() if el["errors"]] == [errors]
    assert _rows(doc)


def _err(code: str, *, index):
    return {
        "code": code,
        "severity": "error",
        "message": code,
        "template": code,
        "context": {},
        "index": index,
        "key": None,
    }


def test_lib_exports_the_documented_names():
    assert set(lib.__all__) == {
        "ELEMENT_TYPES",
        "FORMAT_VERSION",
        "ElementRef",
        "ManifestError",
        "Manifests",
        "RawSource",
        "ReadFormat",
        "SimpleManifest",
        "SimpleManifestError",
        "Spinta",
        "WriteFormat",
    }
    for name in lib.__all__:
        assert hasattr(lib, name)


def test_raw_source_to_simple_refuses_output_only_options(spinta):
    with pytest.raises(ValueError, match="does not apply to a `simple` result"):
        spinta.copy(CSV_TABLE.encode("utf-8"), "csv", columns=["id"])
