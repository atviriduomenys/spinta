"""Default bases (`copy -b`, see `spinta.manifests.simple.bases`).

A model that has no base is matched to the one model of the same name, without
its dataset, in the default bases. The match is written as the model's base:
a `base` row, `/locn/Country`, with the keys of the matched model. No match, or
more than one, is a `DefaultBaseNotFound` error of the model, and the output is
still written. The base manifests are not in the output.

`copy` of a raw source reads it to `simple`, so a CSV output is `copy(doc, "csv")`.
"""

import csv
import io

import pytest

from spinta.lib import Spinta
from spinta.manifests.simple.schema import validate_document
from spinta.manifests.simple.types import SimpleManifestError

LOCN = """\
dataset,resource,base,model,property,type,ref,uri
locn,,,,,,,
,,,,,prefix,locn,http://www.w3.org/ns/locn#
,,,Country,,,id,locn:Country
,,,,id,integer,,
,,,,code,string,,
"""

OTHER = """\
dataset,resource,base,model,property,type,ref,uri
other,,,,,,,
,,,Country,,,id,
,,,,id,integer,,
"""

AR = """\
dataset,resource,base,model,property,type,ref,uri,visibility
datasets/gov/rc/ar/uapi/addr,,,,,,,,
,,,Country,,,id,,
,,,,id,integer,,,
,,,,code,string,,,
,,,,name@lt,string,,,
"""

STANDALONE = """\
,,,Standalone,,,,,
,,,,n,string,,,
"""

PLACES = """\
dataset,resource,base,model,property,type,ref,uri
places,,,,,,,
,,,Place,,,id,
,,,,id,integer,,
"""


@pytest.fixture
def spinta():
    return Spinta()


@pytest.fixture
def locn(tmp_path):
    path = tmp_path / "locn.csv"
    path.write_text(LOCN, encoding="utf-8")
    return str(path)


def _doc(spinta, text: str) -> dict:
    """A `simple` document read from a CSV text (`copy` reads a raw source to `simple`)."""
    return spinta.copy(text.encode("utf-8"), "csv")


def _csv(data: bytes) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(data.decode("utf-8"))))


def _base_rows(data: bytes) -> list[dict[str, str]]:
    """The rows that name a base. The `/` row that ends a base is not one of them."""
    return [row for row in _csv(data) if row["base"] not in ("", "/")]


def _codes(element: dict) -> list[str]:
    return [error["code"] for error in element["errors"]]


def test_model_without_base_gets_the_model_of_its_name_as_base(spinta, locn):
    doc = spinta.copy(AR.encode("utf-8"), "csv", base=[locn])

    country = doc["model"]["datasets/gov/rc/ar/uapi/addr/Country"]
    assert country["implied"]["base"] == "locn/Country"
    assert country["errors"] == []


def test_match_is_written_as_a_base_row_before_the_model(spinta, locn):
    data = spinta.copy(_doc(spinta, AR), "csv", base=[locn])

    [base] = _base_rows(data)
    assert base["base"] == "/locn/Country"
    assert base["ref"] == "id"
    rows = _csv(data)
    model = next(i for i, row in enumerate(rows) if row["model"] == "Country")
    assert rows[model - 1]["base"] == "/locn/Country"


def test_base_manifest_is_not_written(spinta, locn):
    data = spinta.copy(_doc(spinta, AR), "csv", base=[locn])

    rows = _csv(data)
    assert not any(row["dataset"] == "locn" for row in rows)
    # The `uri` of the base is inherited as a CURIE, so its prefix is added to the dataset, where the CURIE is valid.
    assert any(row["type"] == "prefix" and row["ref"] == "locn" for row in rows)
    assert "locn:Country" in data.decode("utf-8")


def test_every_base_is_searched_for_a_match(spinta, locn):
    places = spinta.copy(PLACES.encode("utf-8"), "csv")

    doc = spinta.copy(AR.encode("utf-8"), "csv", base=[places, locn])

    assert doc["model"]["datasets/gov/rc/ar/uapi/addr/Country"]["implied"]["base"] == "locn/Country"


def test_same_model_given_by_two_bases_is_one_candidate(spinta, locn):
    doc = spinta.copy(AR.encode("utf-8"), "csv", base=[locn, locn])

    country = doc["model"]["datasets/gov/rc/ar/uapi/addr/Country"]
    assert country["implied"]["base"] == "locn/Country"
    assert country["errors"] == []


def test_model_with_no_match_is_reported_and_the_output_is_still_written(spinta, locn):
    text = AR + STANDALONE
    reported = []

    data = spinta.copy(
        _doc(spinta, text),
        "csv",
        base=[locn],
        on_error=lambda error, raised: reported.append((error, raised)),
    )

    assert data is not None
    [(error, raised)] = reported
    assert raised is False
    assert error["code"] == "DefaultBaseNotFound"
    assert error["severity"] == "error"
    assert error["context"]["model"] == "datasets/gov/rc/ar/uapi/addr/Standalone"
    assert "no model named 'Standalone'" in error["message"]
    rows = _csv(data)
    assert [row["model"] for row in rows if row["model"]] == ["Country", "Standalone"]
    assert [row["base"] for row in _base_rows(data)] == ["/locn/Country"]


def test_no_match_is_stored_on_the_model_of_a_simple_document(spinta, locn):
    doc = spinta.copy(_doc(spinta, AR + STANDALONE), "simple", base=[locn])

    standalone = doc["model"]["datasets/gov/rc/ar/uapi/addr/Standalone"]
    assert _codes(standalone) == ["DefaultBaseNotFound"]
    assert standalone["implied"]["base"] is None
    validate_document(doc)


def test_name_with_two_models_in_the_bases_is_no_match(spinta, locn, tmp_path):
    other = tmp_path / "other.csv"
    other.write_text(OTHER, encoding="utf-8")

    doc = spinta.copy(AR.encode("utf-8"), "csv", base=[locn, str(other)])

    country = doc["model"]["datasets/gov/rc/ar/uapi/addr/Country"]
    assert country["implied"]["base"] is None
    [error] = country["errors"]
    assert error["code"] == "DefaultBaseNotFound"
    assert "more than one model named 'Country'" in error["message"]
    assert "'locn/Country'" in error["message"]
    assert "'other/Country'" in error["message"]


def test_without_base_nothing_is_matched_and_nothing_is_an_error(spinta):
    doc = spinta.copy(AR.encode("utf-8"), "csv")

    assert doc["model"]["datasets/gov/rc/ar/uapi/addr/Country"]["implied"]["base"] is None
    assert spinta.errors(doc, code="DefaultBaseNotFound") == []


def test_model_with_its_own_base_is_not_matched(spinta, locn):
    text = """\
dataset,resource,base,model,property,type,ref,uri,visibility
datasets/gov/rc/ar/uapi/addr,,,,,,,,
,,,Country,,,id,,
,,,,id,integer,,,
,,Country,,,,id,,
,,,Kid,,,,,
,,,,id,inherit,,,
"""
    doc = spinta.copy(text.encode("utf-8"), "csv", base=[locn])

    kid = doc["model"]["datasets/gov/rc/ar/uapi/addr/Kid"]
    assert kid["implied"]["base"] == "datasets/gov/rc/ar/uapi/addr/Country"
    assert kid["errors"] == []


def test_base_visibility_limits_the_errors_to_its_visibilities(spinta, locn):
    text = (
        AR
        + """\
,,,Hidden,,,,,private
,,,,n,string,,,
"""
    )
    everything = spinta.copy(_doc(spinta, text), "simple", base=[locn])
    assert _codes(everything["model"]["datasets/gov/rc/ar/uapi/addr/Hidden"]) == ["DefaultBaseNotFound"]

    public = spinta.copy(_doc(spinta, text), "simple", base=[locn], base_visibility=["public", "package"])
    assert public["model"]["datasets/gov/rc/ar/uapi/addr/Hidden"]["errors"] == []
    # The visibility limits the errors only: a model with a visibility outside the list is still matched.
    assert public["model"]["datasets/gov/rc/ar/uapi/addr/Country"]["implied"]["base"] == "locn/Country"


def test_model_without_visibility_is_checked_by_default(spinta, locn):
    text = AR + STANDALONE

    checked = spinta.copy(_doc(spinta, text), "simple", base=[locn])
    assert _codes(checked["model"]["datasets/gov/rc/ar/uapi/addr/Standalone"]) == ["DefaultBaseNotFound"]

    unchecked = spinta.copy(_doc(spinta, text), "simple", base=[locn], base_visibility=["public"])
    assert unchecked["model"]["datasets/gov/rc/ar/uapi/addr/Standalone"]["errors"] == []


@pytest.mark.parametrize("base_visibility", [["open"], ["absent"], ["public", "secret"], [], "public"])
def test_unknown_base_visibility_is_refused(spinta, locn, base_visibility):
    with pytest.raises(ValueError):
        spinta.copy(AR.encode("utf-8"), "csv", base=[locn], base_visibility=base_visibility)


def test_base_visibility_needs_base(spinta):
    with pytest.raises(ValueError, match="needs `base`"):
        spinta.copy(AR.encode("utf-8"), "csv", base_visibility=["public"])


@pytest.mark.parametrize("base", ["locn.csv", {"not": "a document"}, 12, b"locn"])
def test_base_is_a_list_of_paths_or_documents(spinta, base):
    with pytest.raises(ValueError):
        spinta.copy(AR.encode("utf-8"), "csv", base=base)


def test_base_document_is_validated(spinta):
    with pytest.raises(SimpleManifestError):
        spinta.copy(AR.encode("utf-8"), "csv", base=[{"spinta": {"version": "2.2.0", "format": 1}}])


def test_refused_output_option_is_raised_before_a_base_is_read(spinta, tmp_path):
    # `columns` does not apply to `csv`. The option is refused before the base, which does not exist, is read.
    with pytest.raises(ValueError, match="does not apply"):
        spinta.copy(
            _doc(spinta, AR),
            "csv",
            base=[str(tmp_path / "missing.csv")],
            columns=["d"],
        )


def test_simple_output_keeps_the_match_as_implied(spinta, locn):
    doc = spinta.copy(_doc(spinta, AR), "simple", base=[locn])

    assert doc == spinta.copy(_doc(spinta, AR), "simple", base=[locn])
    assert doc["model"]["datasets/gov/rc/ar/uapi/addr/Country"]["implied"]["base"] == "locn/Country"


def test_base_of_a_simple_document_is_written_again_with_the_same_base(spinta, locn):
    # The match is implied, so a document written with `-b` gives the same manifest with the same `-b`.
    doc = spinta.copy(_doc(spinta, AR), "simple", base=[locn])

    assert spinta.copy(doc, "csv", base=[locn]) == spinta.copy(_doc(spinta, AR), "csv", base=[locn])


def test_base_is_not_written_without_base_manifests(spinta, locn):
    # A match is derived from `-b`, not given: without `-b` the base row is not written again.
    doc = spinta.copy(_doc(spinta, AR), "simple", base=[locn])

    assert _base_rows(spinta.copy(doc, "csv")) == []


def test_mermaid_and_rdf_outputs_take_a_default_base(spinta, locn):
    doc = _doc(spinta, AR)

    mermaid = spinta.copy(doc, "mermaid", base=[locn])
    assert "locn_x2f_Country" in mermaid

    rdf = spinta.copy(doc, "rdf", base=[locn])
    assert "Country" in rdf


def test_model_of_a_manifest_without_dataset_is_matched(spinta, locn):
    text = """\
dataset,resource,base,model,property,type,ref,uri,visibility
,,,Country,,,id,,
,,,,id,integer,,,
"""
    doc = spinta.copy(text.encode("utf-8"), "csv", base=[locn])

    assert doc["model"]["Country"]["implied"]["base"] == "locn/Country"
    assert doc["model"]["Country"]["errors"] == []


def test_simple_output_writes_the_matched_base_as_an_element(spinta, locn):
    # The match is an element of the output, with no `index`, and the model points to it, as a model row does.
    doc = spinta.copy(_doc(spinta, AR), "simple", base=[locn])

    key = "datasets/gov/rc/ar/uapi/addr//locn/Country"
    assert doc["base"][key] == {
        "index": None,
        "parent": {"type": "dataset", "path": "datasets/gov/rc/ar/uapi/addr"},
        "given": {"ref": "id"},
        "implied": doc["base"][key]["implied"],
        "errors": [],
    }
    assert doc["base"][key]["implied"]["model"] == "locn/Country"
    assert doc["model"]["datasets/gov/rc/ar/uapi/addr/Country"]["base"] == key


def test_matched_base_survives_include_of_the_base_manifest(spinta, locn):
    # The two-step copy of a linked document: the matched base is kept by `include`, so `check` finds it.
    doc = spinta.copy(_doc(spinta, AR), "simple", base=[locn])
    included = spinta.copy(doc, "simple", include=[spinta.copy(locn, "csv")])

    assert "datasets/gov/rc/ar/uapi/addr//locn/Country" in included["base"]
    assert included["model"]["locn/Country"]
    assert spinta.check(included) == []


def test_matched_base_without_include_is_not_a_base_of_a_loaded_model(spinta, locn):
    # Without `include` the base model is not in the document, so the base is dropped on read, as any other.
    doc = spinta.copy(_doc(spinta, AR), "simple", base=[locn])

    assert spinta.check(doc) == []


def test_one_call_with_include_and_base_gives_the_two_calls(spinta, locn):
    # `include` and `base` in one copy give what the two copies give: the match is made before `include` looks
    # for the targets of the references, so the matched base is kept.
    locn_doc = spinta.copy(locn, "csv")
    main = _doc(spinta, AR)

    one = spinta.copy(main, "simple", include=[locn_doc], base=[locn])
    two = spinta.copy(spinta.copy(main, "simple", base=[locn]), "simple", include=[locn_doc])

    assert one == two
    assert "locn/Country" in one["model"]
    assert "datasets/gov/rc/ar/uapi/addr//locn/Country" in one["base"]
    assert spinta.check(one) == []


def test_included_elements_get_no_default_base(spinta, locn):
    # `locn/Country` comes from `include`. It is not a model of the document, so it is not matched to a base.
    one = spinta.copy(_doc(spinta, AR), "simple", include=[spinta.copy(locn, "csv")], base=[locn])

    assert [key for key in one["base"] if key.startswith("locn")] == []
    assert one["model"]["locn/Country"]["implied"]["base"] is None


def test_one_call_keeps_the_no_match_error_of_a_model(spinta, locn):
    # A model that the bases do not match gets its `DefaultBaseNotFound` error, also when `include` is given.
    reported = []
    doc = spinta.copy(
        _doc(spinta, AR + STANDALONE),
        "simple",
        include=[spinta.copy(locn, "csv")],
        base=[locn],
        on_error=lambda error, raised: reported.append((error["code"], error["context"]["model"], raised)),
    )

    assert reported == [("DefaultBaseNotFound", "datasets/gov/rc/ar/uapi/addr/Standalone", False)]
    assert _codes(doc["model"]["datasets/gov/rc/ar/uapi/addr/Standalone"]) == ["DefaultBaseNotFound"]
    assert _codes(doc["model"]["datasets/gov/rc/ar/uapi/addr/Country"]) == []
    assert "locn/Country" in doc["model"]


def test_second_copy_with_base_keeps_one_base_element(spinta, locn):
    first = spinta.copy(_doc(spinta, AR), "simple", base=[locn])
    again = spinta.copy(first, "simple", base=[locn])

    assert list(again["base"]) == list(first["base"])
    assert spinta.check(again) == []
