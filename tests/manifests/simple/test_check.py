"""`Spinta.check` of a `simple` document: link and check, errors on their elements (3.3, 4.10, 5.3, 5.4)."""

import csv
import io
import threading

import pytest

from spinta.exceptions import BaseError
from spinta.lib import Spinta
from spinta.manifests.simple.check import _with_errors
from spinta.manifests.simple.errors import element_of, manifest_error
from spinta.manifests.simple.from_tabular import tabular_to_simple
from spinta.manifests.simple.types import SimpleManifestError
from spinta.manifests.tabular.helpers import TabularReadLog
from tests.manifests.simple.test_read import CSV_TABLE

HEADER = "dataset,resource,base,model,property,type,ref,source,prepare,access,uri\n"

# 5.3: `location` names a model of another dataset, which is not in the document.
CITIES = HEADER + (
    "datasets/gov/cities,,,,,,,,,,\n"
    ",,,City,,,id,,,,\n"
    ",,,,id,integer,,,,,\n"
    ",,,,location,ref,/datasets/gov/locn/Location,,,,\n"
)

# 5.4: `country` names a model that is not in the document, `sizes` an enum
# that is not defined.
EXAMPLE = HEADER + (
    "datasets/gov/example,,,,,,,,,,\n"
    ",,,City,,,id,,,,\n"
    ",,,,id,integer,,,,,\n"
    ",,,,country,ref,Country,COUNTRY_ID,,,\n"
    ",,,,sizes,string,missing_enum,,,,\n"
)


def _doc(spinta: Spinta, text: str) -> dict:
    rows = ((str(i), r) for i, r in enumerate(csv.reader(io.StringIO(text)), 1))
    return tabular_to_simple(rows, type="csv", version=spinta.version)


def _element(doc, type_, path):
    return doc[type_][path]


def _manifest(doc):
    """The one `manifest` element of a document read from one table."""
    assert len(doc["manifest"]) == 1
    return next(iter(doc["manifest"].values()))


def _codes(doc):
    """The codes of the errors on the elements, not on the manifest."""
    return [
        e["code"]
        for type_, elements in doc.items()
        if type_ not in ("spinta", "manifest")
        for el in elements.values()
        for e in el["errors"]
    ]


@pytest.fixture(scope="module")
def spinta():
    return Spinta()


def test_valid_table_has_no_errors(spinta):
    checked = spinta.check(_doc(spinta, CSV_TABLE))
    assert spinta.errors(checked) == []


def test_unresolved_reference_is_stored_on_its_property(spinta):
    checked = spinta.check(_doc(spinta, CITIES))
    prop = _element(checked, "property", "datasets/gov/cities/City/location")
    assert [e["code"] for e in prop["errors"]] == ["UnresolvedReference"]
    error = prop["errors"][0]
    assert error["severity"] == "error"
    assert error["index"] == 4
    assert error["key"] == "ref"
    assert error["context"] == {
        "dataset": "datasets/gov/cities",
        "model": "datasets/gov/cities/City",
        "property": "location",
        "type": "ref",
        "ref": "datasets/gov/locn/Location",
    }
    assert error["message"] == (
        "Model 'datasets/gov/locn/Location' referenced by 'ref' property 'location' was not found in the manifest."
    )


def test_error_context_has_no_element_key(spinta):
    checked = spinta.check(_doc(spinta, CITIES))
    error = _element(checked, "property", "datasets/gov/cities/City/location")["errors"][0]
    assert "element" not in error


def test_strict_references_is_the_default(spinta):
    checked = spinta.check(_doc(spinta, CITIES))
    assert [e["severity"] for e in spinta.errors(checked, code="UnresolvedReference")] == ["error"]


def test_downgraded_reference_is_a_warning(spinta):
    checked = spinta.check(_doc(spinta, CITIES), strict_references=False)
    assert [e["severity"] for e in spinta.errors(checked, code="UnresolvedReference")] == ["warning"]


def test_relative_reference_gets_the_dataset_of_its_model(spinta):
    checked = spinta.check(_doc(spinta, EXAMPLE))
    error = _element(checked, "property", "datasets/gov/example/City/country")["errors"][0]
    assert error["code"] == "UnresolvedReference"
    assert error["context"]["ref"] == "datasets/gov/example/Country"


def test_many_errors_in_one_manifest_are_each_on_their_element(spinta):
    checked = spinta.check(_doc(spinta, EXAMPLE))
    country = _element(checked, "property", "datasets/gov/example/City/country")
    sizes = _element(checked, "property", "datasets/gov/example/City/sizes")
    assert [e["code"] for e in country["errors"]] == ["UnresolvedReference"]
    assert [e["code"] for e in sizes["errors"]] == ["UndefinedEnum"]
    assert sizes["errors"][0]["key"] == "ref"
    assert sizes["errors"][0]["context"]["name"] == "missing_enum"
    # The element without a problem still has its own, empty, list.
    assert _element(checked, "property", "datasets/gov/example/City/id")["errors"] == []


def test_element_errors_do_not_contain_internal_context(spinta):
    checked = spinta.check(_doc(spinta, EXAMPLE))
    sizes = _element(checked, "property", "datasets/gov/example/City/sizes")
    assert set(sizes["errors"][0]["context"]) == {"dataset", "model", "property", "name"}


def test_check_does_not_raise_for_content_and_returns_the_document(spinta):
    checked = spinta.check(_doc(spinta, EXAMPLE))
    assert set(checked) == {"spinta", "manifest", "dataset", "model", "property"}


def test_input_document_is_not_changed(spinta):
    doc = _doc(spinta, EXAMPLE)
    before = repr(doc)
    spinta.check(doc)
    assert repr(doc) == before


def test_repeated_check_adds_no_duplicates(spinta):
    checked = spinta.check(_doc(spinta, EXAMPLE))
    again = spinta.check(spinta.check(checked))
    assert len(spinta.errors(again)) == len(spinta.errors(checked)) == 2
    assert _codes(again) == _codes(checked)


def test_fixing_the_reference_clears_the_error(spinta):
    checked = spinta.check(_doc(spinta, CITIES))
    fixed = _doc(spinta, CITIES.replace("/datasets/gov/locn/Location", "/datasets/gov/cities/City"))
    # `location` now names the model of the document, which has no error.
    assert spinta.errors(spinta.check(fixed)) == []
    assert spinta.errors(checked) != []


def test_errors_of_the_document_are_in_document_order(spinta):
    checked = spinta.check(_doc(spinta, EXAMPLE))
    assert [e["code"] for e in spinta.errors(checked)] == ["UnresolvedReference", "UndefinedEnum"]


def test_read_error_is_reproduced_by_the_check_and_stays_on_its_element(spinta):
    # A bad formula is in `given`, so the rebuilt row reproduces its error.
    text = CITIES + ",,,,bad,string,,,(,,\n"
    checked = spinta.check(_doc(spinta, text))
    bad = _element(checked, "property", "datasets/gov/cities/City/bad")
    assert bad["errors"] != []


def test_edited_element_loses_its_old_read_errors(spinta):
    doc = _doc(spinta, CITIES + ",,,,bad,string,,,(,,\n")
    key = "datasets/gov/cities/City/bad"
    # The reader stored the error when it read the table.
    assert _element(doc, "property", key)["errors"] != []
    checked = spinta.check(doc)
    assert _element(checked, "property", key)["errors"] != []
    # The user fixes the row: its `given` changes, so the error is gone.
    checked["property"][key]["given"]["prepare"] = "1"
    fixed = spinta.check(checked)
    assert _element(fixed, "property", key)["errors"] == []


def test_read_problem_of_the_document_is_kept_once(spinta):
    # A duplicate property is found again by the rebuilt rows, but only once.
    text = CITIES + ",,,,id,integer,,,,,\n"
    doc = _doc(spinta, text)
    assert len(_manifest(doc)["errors"]) == 1
    checked = spinta.check(doc)
    assert [e["index"] for e in _manifest(checked)["errors"]] == [5]
    again = spinta.check(checked)
    assert len(_manifest(again)["errors"]) == 1


def test_wrong_cell_count_is_a_document_error_and_survives_the_check(spinta):
    # Its cells are not in `given`, so the rebuilt row does not reproduce it.
    # The problem is kept, because the document no longer has its source.
    doc = _doc(spinta, CITIES.replace(",,,,id,integer,,,,,", ",,,,id,integer"))
    problem = _manifest(doc)["errors"][0]
    assert problem["code"] == "TabularManifestError"
    checked = spinta.check(doc)
    assert problem in _manifest(checked)["errors"]
    # The row of `City/id` is gone, so the model's key names nothing. The error
    # names a property that is not in the document, so it goes to the manifest.
    assert [e["code"] for e in _manifest(checked)["errors"]] == ["TabularManifestError", "PropertyNotFound"]
    assert _manifest(checked)["errors"][1]["context"] == {"model": "datasets/gov/cities/City", "property": "id"}
    assert _codes(checked) == []


def test_error_without_an_element_goes_to_the_manifest(spinta):
    class Stray(BaseError):
        template = "Something is wrong with nothing in particular."

    doc = _doc(spinta, CITIES)
    out = _with_errors(doc, TabularReadLog(), [Stray()])
    assert [e["code"] for e in _manifest(out)["errors"]] == ["Stray"]
    assert _codes(out) == []


def test_element_of_maps_the_context_to_the_element():
    class Named(BaseError):
        template = "x"

    error = Named()
    error.context = {"model": "a/B", "property": "c"}
    assert element_of(error) == ("property", "a/B/c")
    error.context = {"model": "a/B"}
    assert element_of(error) == ("model", "a/B")
    error.context = {"dataset": "d", "resource": "r"}
    assert element_of(error) == ("resource", "d/r")
    error.context = {"dataset": "d"}
    assert element_of(error) == ("dataset", "d")
    error.context = {}
    assert element_of(error) is None


def test_manifest_error_has_the_keys_of_3_3(spinta):
    checked = spinta.check(_doc(spinta, CITIES))
    error = _element(checked, "property", "datasets/gov/cities/City/location")["errors"][0]
    assert set(error) == {"code", "severity", "message", "template", "context", "index", "key"}


def test_check_of_a_malformed_document_raises(spinta):
    doc = _doc(spinta, CITIES)
    doc["spinta"]["format"] = 99
    with pytest.raises(SimpleManifestError):
        spinta.check(doc)


def test_check_of_a_list_of_one_document(spinta):
    doc = _doc(spinta, CITIES)
    assert spinta.check([doc]) == spinta.check(doc)


# `ORDER` refers to `Customer`, which only `CUSTOMERS` defines (3.1, merged first).
ORDER = HEADER + (
    "datasets/gov/shop,,,,,,,,,,\n,,,Order,,,id,,,,\n,,,,id,integer,,,,,\n,,,,customer,ref,Customer,,,,\n"
)
CUSTOMERS = HEADER + ("datasets/gov/shop,,,,,,,,,,\n,,,Customer,,,id,,,,\n,,,,id,integer,,,,,\n")


def test_check_of_several_documents_checks_them_as_one(spinta):
    assert spinta.errors(spinta.check(_doc(spinta, ORDER)), code="UnresolvedReference")
    checked = spinta.check([_doc(spinta, ORDER), _doc(spinta, CUSTOMERS)])
    assert spinta.errors(checked, code="UnresolvedReference") == []
    assert "datasets/gov/shop/Customer" in checked["model"]
    assert "datasets/gov/shop/Order" in checked["model"]


def test_check_of_documents_that_define_one_path_differently_raises(spinta):
    other = HEADER + ("datasets/gov/shop,,,,,,,,,,\n,,,Customer,,,name,,,,\n,,,,name,string,,,,,\n")
    with pytest.raises(SimpleManifestError):
        spinta.check([_doc(spinta, CUSTOMERS), _doc(spinta, other)])


def test_check_with_check_names_off_is_accepted(spinta):
    checked = spinta.check(_doc(spinta, CITIES), check_names=False)
    assert spinta.errors(checked, code="UnresolvedReference")


def test_threads_share_one_instance_and_get_serial_results():
    spinta = Spinta()
    doc = _doc(spinta, EXAMPLE)
    expected = spinta.check(doc)
    results = []

    def run():
        results.append(spinta.check(doc))

    threads = [threading.Thread(target=run) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(results) == 4
    assert all(result == expected for result in results)


def test_manifest_error_of_a_base_error_uses_its_severity():
    class Warn(BaseError):
        template = "x"
        severity = "warning"

    assert manifest_error(Warn())["severity"] == "warning"
