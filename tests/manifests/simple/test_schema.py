import copy

import pytest

from spinta.manifests.simple.schema import validate_document
from spinta.manifests.simple.types import SimpleManifestError

COLUMNS = ["id", "dataset", "model", "property", "type", "ref", "source", "prepare"]
SOURCE = "dsa.csv"
MANIFEST = {"type": "manifest", "path": SOURCE}


def _element(index, parent, given, implied=None):
    return {
        "index": index,
        "parent": parent,
        "given": given,
        "implied": implied if implied is not None else {},
        "errors": [],
    }


def _manifest(columns=COLUMNS):
    return {
        "index": None,
        "parent": None,
        "given": None,
        "implied": None,
        "errors": [],
        "type": "csv",
        "source": SOURCE,
        "columns": list(columns),
    }


def _doc():
    return {
        "spinta": {"version": "2.2.0", "format": 1},
        "manifest": {SOURCE: _manifest()},
        "dataset": {
            "ds": _element(1, MANIFEST, {"id": "1"}, {"name": "ds"}),
        },
        "model": {
            "ds/Country": _element(2, {"type": "dataset", "path": "ds"}, {"id": "2"}),
        },
        "property": {
            "ds/Country/id": _element(3, {"type": "model", "path": "ds/Country"}, {"id": "3", "type": "integer"}),
            "ds/Country/name": _element(None, {"type": "model", "path": "ds/Country"}, None, {"type": "text"}),
            "ds/Country/name@lt": _element(
                4,
                {"type": "property", "path": "ds/Country/name"},
                {"id": "4", "type": "string"},
            ),
        },
    }


def test_valid_document_passes():
    validate_document(_doc(), COLUMNS)


def test_valid_document_passes_without_known_columns():
    validate_document(_doc())


def test_missing_spinta_key():
    doc = _doc()
    del doc["spinta"]
    with pytest.raises(SimpleManifestError, match="spinta"):
        validate_document(doc)


@pytest.mark.parametrize("key", ["version", "format"])
def test_missing_spinta_header_key(key):
    doc = _doc()
    del doc["spinta"][key]
    with pytest.raises(SimpleManifestError, match=f"spinta.{key}"):
        validate_document(doc)


def test_document_needs_a_manifest():
    doc = _doc()
    del doc["manifest"]
    with pytest.raises(SimpleManifestError, match="manifest"):
        validate_document(doc)


def test_manifest_has_the_input_metadata():
    doc = _doc()
    del doc["manifest"][SOURCE]["source"]
    with pytest.raises(SimpleManifestError, match="missing keys"):
        validate_document(doc)


def test_manifest_has_no_parent():
    doc = _doc()
    doc["manifest"][SOURCE]["parent"] = {"type": "dataset", "path": "ds"}
    with pytest.raises(SimpleManifestError, match="no parent"):
        validate_document(doc)


def test_top_level_element_must_point_to_a_manifest():
    doc = _doc()
    doc["dataset"]["ds"]["parent"] = None
    with pytest.raises(SimpleManifestError, match="only a manifest has no parent"):
        validate_document(doc)


def test_manifest_columns_must_be_known():
    doc = _doc()
    doc["manifest"][SOURCE]["columns"] = ["id", "bogus"]
    with pytest.raises(SimpleManifestError, match="unknown columns"):
        validate_document(doc, COLUMNS)


def test_manifest_type_must_be_a_format():
    doc = _doc()
    doc["manifest"][SOURCE]["type"] = ""
    with pytest.raises(SimpleManifestError, match="type must be"):
        validate_document(doc)


def test_columns_of_several_manifests_are_the_union():
    doc = _doc()
    doc["manifest"]["other.csv"] = _manifest(["id", "model", "extra"])
    doc["manifest"]["other.csv"]["source"] = "other.csv"
    doc["dataset"]["ds"]["given"]["extra"] = "x"
    validate_document(doc)


def test_newer_format_is_refused():
    doc = _doc()
    doc["spinta"]["format"] = 99
    with pytest.raises(SimpleManifestError, match="newer"):
        validate_document(doc)


def test_unknown_column_in_manifest_columns():
    doc = _doc()
    doc["manifest"][SOURCE]["columns"] = ["id", "bogus"]
    with pytest.raises(SimpleManifestError, match="unknown columns"):
        validate_document(doc, COLUMNS)


def test_unknown_element_type():
    doc = _doc()
    doc["widget"] = {}
    with pytest.raises(SimpleManifestError, match="Unknown element type"):
        validate_document(doc)


def test_element_missing_key():
    doc = _doc()
    del doc["model"]["ds/Country"]["errors"]
    with pytest.raises(SimpleManifestError, match="missing keys"):
        validate_document(doc)


def test_duplicate_index_is_refused():
    doc = _doc()
    doc["model"]["ds/Country"]["index"] = 1
    with pytest.raises(SimpleManifestError, match="used twice"):
        validate_document(doc)


def test_index_zero_is_refused():
    doc = _doc()
    doc["model"]["ds/Country"]["index"] = 0
    with pytest.raises(SimpleManifestError, match="index must be"):
        validate_document(doc)


def test_index_null_requires_null_given():
    doc = _doc()
    doc["model"]["ds/Country"]["index"] = None
    with pytest.raises(SimpleManifestError, match="null exactly"):
        validate_document(doc)


def test_given_null_requires_null_index():
    doc = _doc()
    doc["property"]["ds/Country/id"]["given"] = None
    with pytest.raises(SimpleManifestError, match="null exactly"):
        validate_document(doc)


def test_parent_must_exist():
    doc = _doc()
    doc["model"]["ds/Country"]["parent"] = {"type": "dataset", "path": "nope"}
    with pytest.raises(SimpleManifestError, match="does not exist"):
        validate_document(doc)


def test_parent_must_have_type_and_path():
    doc = _doc()
    doc["model"]["ds/Country"]["parent"] = {"type": "dataset"}
    with pytest.raises(SimpleManifestError, match="parent must be"):
        validate_document(doc)


def test_parent_cycle_is_refused():
    doc = copy.deepcopy(_doc())
    doc["dataset"]["ds"]["parent"] = {"type": "model", "path": "ds/Country"}
    with pytest.raises(SimpleManifestError, match="cycle"):
        validate_document(doc)


def test_given_key_must_be_a_column():
    doc = _doc()
    doc["model"]["ds/Country"]["given"]["bogus"] = "x"
    with pytest.raises(SimpleManifestError, match="not in the columns"):
        validate_document(doc)


def test_given_value_must_be_non_empty_string():
    doc = _doc()
    doc["model"]["ds/Country"]["given"]["model"] = ""
    with pytest.raises(SimpleManifestError, match="non-empty string"):
        validate_document(doc)


def test_given_value_must_not_be_a_number():
    doc = _doc()
    doc["model"]["ds/Country"]["given"]["id"] = 2
    with pytest.raises(SimpleManifestError, match="non-empty string"):
        validate_document(doc)


def test_unknown_implied_keys_are_ignored():
    doc = _doc()
    doc["model"]["ds/Country"]["implied"]["future_key"] = {"anything": True}
    validate_document(doc)


def test_error_must_have_all_keys():
    doc = _doc()
    doc["model"]["ds/Country"]["errors"] = [{"code": "UndefinedEnum"}]
    with pytest.raises(SimpleManifestError, match="missing keys"):
        validate_document(doc)


def test_error_severity_must_be_known():
    doc = _doc()
    doc["model"]["ds/Country"]["errors"] = [
        {
            "code": "X",
            "severity": "fatal",
            "message": "m",
            "template": "t",
            "context": {},
            "index": None,
            "key": None,
        }
    ]
    with pytest.raises(SimpleManifestError, match="severity"):
        validate_document(doc)


def test_append_must_target_its_owner():
    doc = _doc()
    doc["append"] = {
        "ds/Country/id#2": _element(
            5,
            {"type": "property", "path": "ds/Country/name"},
            {"source": "x"},
        ),
    }
    with pytest.raises(SimpleManifestError, match="parent must be its target"):
        validate_document(doc)


def test_append_path_needs_a_suffix():
    doc = _doc()
    doc["append"] = {
        "ds/Country/id": _element(5, {"type": "property", "path": "ds/Country/id"}, {"source": "x"}),
    }
    with pytest.raises(SimpleManifestError, match=r"path must be"):
        validate_document(doc)


@pytest.mark.parametrize(
    "type_, path, given",
    [
        ("dataset", "ds", {"dataset": "ds"}),
        ("model", "ds/Country", {"model": "Country"}),
        ("property", "ds/Country/id", {"property": "id"}),
    ],
)
def test_name_cell_must_not_be_in_given(type_, path, given):
    """4.5: the name cell is the path, so it is not stored in `given`."""
    doc = _doc()
    doc[type_][path]["given"] = {**doc[type_][path]["given"], **given}
    with pytest.raises(SimpleManifestError, match="name cell"):
        validate_document(doc, COLUMNS)


def test_dimension_keyword_must_not_be_in_given():
    """4.5: the `type` keyword of an extra dimension is not stored."""
    doc = _doc()
    doc["prefix"] = {"ds/dct": _element(5, {"type": "dataset", "path": "ds"}, {"id": "5", "type": "prefix"})}
    with pytest.raises(SimpleManifestError, match="keyword"):
        validate_document(doc, COLUMNS)


def test_manifest_columns_must_be_a_list_of_strings():
    doc = _doc()
    doc["manifest"][SOURCE]["columns"] = "id"
    with pytest.raises(SimpleManifestError, match="columns must be a list"):
        validate_document(doc)


def test_manifest_path_is_its_source():
    doc = _doc()
    doc["manifest"]["other.csv"] = doc["manifest"].pop(SOURCE)
    with pytest.raises(SimpleManifestError, match="must be its source"):
        validate_document(doc)
