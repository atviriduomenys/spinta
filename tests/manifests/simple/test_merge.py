"""Merging of a list of `simple` documents (3.1)."""

import csv
import io

import pytest

from spinta.lib import Spinta
from spinta.manifests.simple.from_tabular import tabular_to_simple
from spinta.manifests.simple.merge import merge_documents
from spinta.manifests.simple.schema import document_columns
from spinta.manifests.simple.types import SimpleManifestError

HEADER = "dataset,resource,base,model,property,type,ref,source,prepare,access,uri\n"

FIRST = HEADER + (
    "datasets/gov/a,,,,,,,,,,\n,,,City,,,id,,,,\n,,,,id,integer,,,,,\n,,,,country,ref,/datasets/gov/b/Country,,,,\n"
)
SECOND = HEADER + ("datasets/gov/b,,,,,,,,,,\n,,,Country,,,id,,,,\n,,,,id,integer,,,,,\n,,,,name,string,,,,,\n")


@pytest.fixture(scope="module")
def spinta():
    return Spinta()


def _doc(spinta: Spinta, text: str, source: str | None = None) -> dict:
    rows = ((str(i), r) for i, r in enumerate(csv.reader(io.StringIO(text)), 1))
    return tabular_to_simple(rows, type="csv", source=source, version=spinta.version)


def test_each_input_file_is_a_manifest_of_its_own(spinta):
    merged = merge_documents(
        [_doc(spinta, FIRST, "first.csv"), _doc(spinta, SECOND, "second.csv")],
        version=spinta.version,
    )
    assert list(merged["manifest"]) == ["first.csv", "second.csv"]
    assert merged["dataset"]["datasets/gov/a"]["parent"] == {"type": "manifest", "path": "first.csv"}
    assert merged["dataset"]["datasets/gov/b"]["parent"] == {"type": "manifest", "path": "second.csv"}


def test_elements_of_all_documents_are_kept_in_order(spinta):
    merged = merge_documents([_doc(spinta, FIRST), _doc(spinta, SECOND)], version=spinta.version)
    assert list(merged["model"]) == ["datasets/gov/a/City", "datasets/gov/b/Country"]
    assert "datasets/gov/a" in merged["dataset"]
    assert "datasets/gov/b" in merged["dataset"]


def test_index_of_a_later_document_is_shifted_and_unique(spinta):
    first = _doc(spinta, FIRST)
    second = _doc(spinta, SECOND)
    merged = merge_documents([first, second], version=spinta.version)

    indexes = [
        element["index"]
        for type_, elements in merged.items()
        if type_ != "spinta"
        for element in elements.values()
        if element["index"] is not None
    ]
    assert len(indexes) == len(set(indexes))
    last_of_first = max(e["index"] for t, els in first.items() if t not in ("spinta", "manifest") for e in els.values())
    assert merged["model"]["datasets/gov/b/Country"]["index"] > last_of_first


def test_error_index_moves_with_its_element(spinta):
    # `Country/region` names a model that no document defines, so it is an error of the second document.
    second = SECOND + ",,,,region,ref,/datasets/gov/x/Region,,,,\n"
    checked = spinta.check([_doc(spinta, FIRST), _doc(spinta, second)])
    element = checked["property"]["datasets/gov/b/Country/region"]
    assert [e["code"] for e in element["errors"]] == ["UnresolvedReference"]
    assert element["errors"][0]["index"] == element["index"]
    first_indexes = [
        e["index"] for t, els in _doc(spinta, FIRST).items() if t not in ("spinta", "manifest") for e in els.values()
    ]
    assert element["index"] > max(first_indexes)


def test_same_path_with_equal_content_is_kept_once(spinta):
    first = _doc(spinta, FIRST)
    merged = merge_documents([first, _doc(spinta, FIRST)], version=spinta.version)
    assert list(merged["dataset"]) == ["datasets/gov/a"]
    assert list(merged["model"]) == ["datasets/gov/a/City"]


def test_same_path_with_different_content_raises(spinta):
    other = FIRST.replace(",,,City,,,id,", ",,,City,,,name,")
    with pytest.raises(SimpleManifestError):
        merge_documents([_doc(spinta, FIRST), _doc(spinta, other)], version=spinta.version)


def test_columns_are_the_union_in_the_order_of_the_first_document(spinta):
    first = _doc(spinta, FIRST, "first.csv")
    second = _doc(spinta, SECOND, "second.csv")
    second["manifest"]["second.csv"]["columns"] = ["model", "extra", "dataset"]
    merged = merge_documents([first, second], version=spinta.version)
    first_columns = first["manifest"]["first.csv"]["columns"]
    assert merged["manifest"]["first.csv"]["columns"] == first_columns
    assert document_columns(merged) == first_columns + ["extra"]


def test_same_input_in_two_documents_is_one_manifest_with_both_columns(spinta):
    first = _doc(spinta, FIRST)
    second = _doc(spinta, FIRST)
    second["manifest"]["<memory>"]["columns"] = ["extra"]
    merged = merge_documents([first, second], version=spinta.version)
    assert list(merged["manifest"]) == ["<memory>"]
    assert document_columns(merged) == first["manifest"]["<memory>"]["columns"] + ["extra"]


def test_document_errors_are_kept_in_order(spinta):
    bad = FIRST + ",,,,broken\n"
    first = _doc(spinta, bad, "first.csv")
    second = _doc(spinta, SECOND, "second.csv")
    merged = merge_documents([first, second], version=spinta.version)
    assert merged["manifest"]["first.csv"]["errors"] == first["manifest"]["first.csv"]["errors"]
    assert merged["manifest"]["first.csv"]["errors"], "the wrong cell count of the first document must be kept"


def test_merged_document_has_the_running_version(spinta):
    first = _doc(spinta, FIRST)
    first["spinta"]["version"] = "0.0.1"
    merged = merge_documents([first, _doc(spinta, SECOND)], version="9.9.9")
    assert merged["spinta"] == {"version": "9.9.9", "format": 1}


def test_empty_list_has_nothing_to_merge(spinta):
    with pytest.raises(ValueError):
        merge_documents([], version=spinta.version)
