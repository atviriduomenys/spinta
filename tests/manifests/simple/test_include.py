"""`include` of `copy` (3.2, `spinta.manifests.simple.include`).

The included documents are used only to resolve references. Pruning is plain
dict work, so these tests do not need a database.
"""

import csv
import io

import pytest

from spinta.lib import Spinta
from spinta.manifests.simple.include import include_documents
from spinta.manifests.simple.types import SimpleManifestError

MAIN = """\
dataset,resource,base,model,property,type,ref,uri
datasets/gov/rc/ar/uapi/addr,,,,,,,
,,/locn/City,,,,id,
,,,City,,,id,
,,,,id,integer,,
,,,,code,string,,
,,,,name@lt,string,,
,,,,country,ref,/locn/Country,
"""

# The example of 3.2: `locn` has `Country` and `City`, the main document needs `Country` and the `City` base.
INCLUDE = """\
dataset,resource,base,model,property,type,ref,uri
locn,,,,,,,
,,,Country,,,id,
,,,,id,integer,,
,,,,code,string,,
,,,City,,,id,
,,,,id,integer,,
,,,,code,string,,
"""


@pytest.fixture
def spinta():
    return Spinta()


def _doc(spinta: Spinta, text: str) -> dict:
    return spinta.copy(text.encode("utf-8"), "csv")


def _keys(doc: dict, type_: str) -> set[str]:
    return set(doc.get(type_, {}))


def _rows(data: bytes) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(data.decode("utf-8"))))


def _codes(spinta: Spinta, doc: dict, code: str) -> list[dict]:
    return spinta.errors(doc, code=[code])


def test_referenced_elements_are_included_and_nothing_else(spinta):
    doc = _doc(spinta, MAIN)
    include = _doc(spinta, INCLUDE)

    result = spinta.copy(doc, "simple", include=[include])

    assert "locn" in _keys(result, "dataset")
    assert _keys(result, "model") == {
        "datasets/gov/rc/ar/uapi/addr/City",
        "locn/Country",
        "locn/City",
    }
    # The key of each included model is included, the other properties are not.
    assert {"locn/Country/id", "locn/City/id"} <= _keys(result, "property")
    assert not {"locn/Country/code", "locn/City/code"} & _keys(result, "property")


def test_included_elements_are_implied_and_the_main_elements_keep_their_rows(spinta):
    doc = _doc(spinta, MAIN)
    include = _doc(spinta, INCLUDE)

    result = spinta.copy(doc, "simple", include=[include])

    country = result["model"]["locn/Country"]
    assert country["index"] is None
    assert country["given"] == include["model"]["locn/Country"]["given"]
    assert country["implied"]["name"] == "Country"
    assert country["implied"]["ref"] == ["locn/Country/id"]
    assert country["parent"] == {"type": "dataset", "path": "locn"}

    city = result["model"]["datasets/gov/rc/ar/uapi/addr/City"]
    assert city["index"] == doc["model"]["datasets/gov/rc/ar/uapi/addr/City"]["index"]
    assert city["given"] == doc["model"]["datasets/gov/rc/ar/uapi/addr/City"]["given"]


def test_reference_and_base_resolve_with_the_included_models(spinta):
    doc = _doc(spinta, MAIN)
    include = _doc(spinta, INCLUDE)

    result = spinta.copy(doc, "simple", include=[include])

    country = result["property"]["datasets/gov/rc/ar/uapi/addr/City/country"]
    assert country["implied"]["ref_model"] == "locn/Country"
    assert country["implied"]["type"] == "ref"
    assert result["model"]["datasets/gov/rc/ar/uapi/addr/City"]["implied"]["base"] == "locn/City"
    assert _codes(spinta, result, "UnresolvedReference") == []


def test_an_include_output_reads_back_with_its_included_elements(spinta):
    """The included elements keep their `given`, so the output of `copy` is a document that reads back (3.2)."""
    doc = _doc(spinta, MAIN)
    include = _doc(spinta, INCLUDE)

    result = spinta.copy(doc, "simple", include=[include])
    checked = spinta._check_document(result)

    assert _codes(spinta, checked, "UnresolvedReference") == []
    assert spinta.copy(result, "csv") == spinta.copy(doc, "csv", include=[include])


def test_without_include_the_references_are_unresolved(spinta):
    doc = _doc(spinta, MAIN)

    codes = sorted(error["code"] for error in spinta.errors(doc, code=["UnresolvedReference"]))

    assert codes == ["UnresolvedReference", "UnresolvedReference"]


def test_parent_chain_of_a_kept_element_is_kept(spinta):
    doc = _doc(spinta, MAIN)
    include = _doc(spinta, INCLUDE)

    result = spinta.copy(doc, "simple", include=[include])

    assert result["property"]["locn/Country/id"]["parent"] == {"type": "model", "path": "locn/Country"}
    assert result["dataset"]["locn"]["parent"] == {"type": "manifest", "path": "<memory>"}
    assert "<memory>" in result["manifest"]


def test_included_manifest_is_kept_as_the_input_of_its_elements(spinta):
    doc = _doc(spinta, MAIN)
    include = _doc(spinta, INCLUDE)

    result = spinta.copy(doc, "simple", include=[include])

    manifests = result["manifest"]
    assert set(manifests) == {"<memory>"}
    assert manifests["<memory>"]["index"] is None


def test_nothing_needed_gives_the_main_document(spinta):
    doc = _doc(
        spinta, "dataset,resource,base,model,property,type,ref,uri\nmain,,,,,,,\n,,,Thing,,,id,\n,,,,id,integer,,\n"
    )
    include = _doc(spinta, INCLUDE)

    plain = spinta.copy(doc, "simple")
    result = spinta.copy(doc, "simple", include=[include])

    assert _keys(result, "model") == _keys(plain, "model")
    assert _keys(result, "dataset") == _keys(plain, "dataset")


def test_the_target_of_a_kept_element_is_followed_transitively(spinta):
    # `Country` is kept for the reference, its key `code` refers to `Code`, and `Code` is kept for it, with its key.
    include = _doc(
        spinta,
        """\
dataset,resource,base,model,property,type,ref,uri
locn,,,,,,,
,,,Code,,,id,
,,,,id,integer,,
,,,Country,,,code,
,,,,code,ref,Code,
,,,,name,string,,
""",
    )
    doc = _doc(
        spinta,
        """\
dataset,resource,base,model,property,type,ref,uri
main,,,,,,,
,,,Order,,,id,
,,,,id,integer,,
,,,,country,ref,/locn/Country,
""",
    )

    result = spinta.copy(doc, "simple", include=[include])

    assert {"locn/Country", "locn/Code"} <= _keys(result, "model")
    assert "locn/Code/id" in _keys(result, "property")
    assert "locn/Country/name" not in _keys(result, "property")


def test_prefix_of_a_kept_curie_is_kept_and_only_that_prefix(spinta):
    include = _doc(
        spinta,
        """\
dataset,resource,base,model,property,type,ref,uri
locn,,,,,,,
,,,,,prefix,dct,http://purl.org/dc/terms/
,,,,,prefix,other,http://example.org/
,,,Country,,,id,dct:Place
,,,,id,integer,,
""",
    )
    doc = _doc(
        spinta,
        """\
dataset,resource,base,model,property,type,ref,uri
main,,,,,,,
,,,Order,,,id,
,,,,id,integer,,
,,,,country,ref,/locn/Country,
""",
    )

    result = spinta.copy(doc, "simple", include=[include])

    assert "locn/dct" in _keys(result, "prefix")
    assert "locn/other" not in _keys(result, "prefix")
    assert result["prefix"]["locn/dct"]["index"] is None


def test_enum_of_a_kept_property_is_kept_with_all_its_items(spinta):
    include = _doc(
        spinta,
        """\
dataset,resource,base,model,property,type,ref,source,prepare
datasets/gov/rc/ar/uapi/addr,,,,,,,,
,,,,,enum,sizes,,S
,,,,,,,,M
,,,,,,,,XL
,,,,,enum,colors,,red
,,,Shop,,,id,,
,,,,id,integer,,,
""",
    )
    doc = _doc(
        spinta,
        """\
dataset,resource,base,model,property,type,ref,source,prepare
datasets/gov/rc/ar/uapi/addr,,,,,,,,
,,,Order,,,id,,
,,,,id,integer,,,
,,,,size,string,sizes,,
""",
    )

    result = spinta.copy(doc, "simple", include=[include])

    enums = _keys(result, "enum")
    assert {
        "datasets/gov/rc/ar/uapi/addr/sizes/S",
        "datasets/gov/rc/ar/uapi/addr/sizes/M",
        "datasets/gov/rc/ar/uapi/addr/sizes/XL",
    } <= enums
    assert "datasets/gov/rc/ar/uapi/addr/colors/red" not in enums
    assert _codes(spinta, result, "UndefinedEnum") == []


def test_a_parent_that_differs_from_the_main_document_is_a_conflict(spinta):
    doc = _doc(
        spinta,
        """\
dataset,resource,base,model,property,type,ref,uri,level
locn,,,,,,,,3
,,,Order,,,id,,
,,,,id,integer,,,
,,,,country,ref,/locn/Country,,
""",
    )
    include = _doc(spinta, INCLUDE)

    with pytest.raises(SimpleManifestError, match="dataset 'locn' is defined in two documents"):
        spinta.copy(doc, "simple", include=[include])


def test_a_parent_that_is_the_same_in_the_main_document_is_not_added_twice(spinta):
    doc = _doc(
        spinta,
        """\
dataset,resource,base,model,property,type,ref,uri
locn,,,,,,,
,,,Order,,,id,
,,,,id,integer,,
,,,,country,ref,/locn/Country,
""",
    )
    include = _doc(spinta, INCLUDE)

    result = spinta.copy(doc, "simple", include=[include])

    assert _keys(result, "dataset") == {"locn"}
    assert result["dataset"]["locn"]["index"] == doc["dataset"]["locn"]["index"]


def test_included_rows_are_read_into_their_own_dataset(spinta):
    """A kept model is written after the last row of its dataset, not after the last row of the file."""
    doc = _doc(
        spinta,
        """\
dataset,resource,base,model,property,type,ref,uri
ds_a,,,,,,,
,,,City,,,id,
,,,,id,integer,,
,,,,country,ref,Country,
ds_b,,,,,,,
,,,Other,,,id,
,,,,id,integer,,
""",
    )
    include = _doc(
        spinta,
        """\
dataset,resource,base,model,property,type,ref,uri
ds_a,,,,,,,
,,,Country,,,id,
,,,,id,integer,,
""",
    )
    errors = []

    data = spinta.copy(
        doc,
        "csv",
        include=[include],
        on_error=lambda error, raised: errors.append(error),
    )

    rows = _rows(data)
    country = next(i for i, row in enumerate(rows) if row["model"] == "Country")
    other = next(i for i, row in enumerate(rows) if row["model"] == "Other")
    assert country < other
    assert [e["code"] for e in errors if e["code"] == "UnresolvedReference"] == []
    assert not any(row["model"] == "Country" and row["dataset"] == "ds_b" for row in rows)


def test_each_include_is_merged_in_the_order_of_the_list(spinta):
    doc = _doc(
        spinta,
        """\
dataset,resource,base,model,property,type,ref,uri
main,,,,,,,
,,,Order,,,id,
,,,,id,integer,,
,,,,country,ref,/locn/Country,
,,,,plant,ref,/places/Plant,
""",
    )
    locn = _doc(spinta, INCLUDE)
    places = _doc(
        spinta,
        """\
dataset,resource,base,model,property,type,ref,uri
places,,,,,,,
,,,Plant,,,id,
,,,,id,integer,,
""",
    )

    result = spinta.copy(doc, "simple", include=[places, locn])

    assert {"locn/Country", "places/Plant"} <= _keys(result, "model")
    indexes = {path: result["model"][path]["index"] for path in ("places/Plant", "locn/Country")}
    assert all(index is None for index in indexes.values())
    assert _codes(spinta, result, "UnresolvedReference") == []


def test_pruning_loads_nothing(spinta, monkeypatch):
    """`include_documents` links nothing and loads no context (3.2)."""
    main = _doc(spinta, MAIN)
    include = _doc(spinta, INCLUDE)

    def no_context(*args, **kwargs):
        raise AssertionError("pruning must not create a context")

    monkeypatch.setattr("spinta.manifests.simple.check.create_context", no_context)

    included = include_documents(main, [include], version=spinta.version)

    assert {"locn/Country", "locn/City"} <= {path for type_, path in included.keys if type_ == "model"}


def test_include_takes_simple_documents(spinta):
    doc = _doc(spinta, MAIN)

    with pytest.raises(ValueError, match="Read a file with `copy` first"):
        spinta.copy(doc, "simple", include=["locn.csv"])


def test_a_document_with_no_manifest_is_refused(spinta):
    doc = _doc(spinta, MAIN)
    include = _doc(spinta, INCLUDE)
    include.pop("manifest")

    with pytest.raises(ValueError, match="manifest"):
        spinta.copy(doc, "simple", include=[include])
