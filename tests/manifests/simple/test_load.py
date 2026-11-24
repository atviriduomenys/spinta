"""Loading a `simple` document through the manifest loader (4.7).

A `simple` document loads to the same manifest as the CSV it was written
from. Both are compared as the tabular rendering of the loaded manifest.
"""

import json
from io import StringIO

import pytest

from spinta import commands
from spinta.cli.helpers.store import load_store
from spinta.core.config import configure_rc
from spinta.manifests.components import ManifestPath
from spinta.manifests.simple.schema import validate_document
from spinta.manifests.simple.types import SimpleManifestError
from spinta.testing.context import create_test_context
from spinta.testing.manifest import compare_manifest
from tests.manifests.simple.test_read import COLUMNS, CSV_TABLE, golden


def _load(rc, paths):
    rc = configure_rc(rc, paths, manifest_type="inline", ensure_backends=False)
    context = create_test_context(rc)
    store = load_store(context, verbose=False)
    commands.load(context, store.manifest, load_internal=False, full_load=True)
    commands.link(context, store.manifest)
    return context, store.manifest


def _csv_manifest(rc):
    return _load(rc, [ManifestPath(type="csv", file=StringIO(CSV_TABLE))])


def _simple_manifest(rc, doc):
    return _load(rc, [ManifestPath(type="simple", file=StringIO(json.dumps(doc)))])


def _render(context, manifest):
    actual, _ = compare_manifest(manifest, "", context=context)
    return actual


def test_simple_document_loads_like_its_csv(rc):
    csv_ctx, csv_manifest = _csv_manifest(rc)
    simple_ctx, simple_manifest = _simple_manifest(rc, golden())
    assert _render(simple_ctx, simple_manifest) == _render(csv_ctx, csv_manifest)


def test_simple_document_loads_the_same_models(rc):
    csv_ctx, csv_manifest = _csv_manifest(rc)
    simple_ctx, simple_manifest = _simple_manifest(rc, golden())
    csv_models = commands.get_models(csv_ctx, csv_manifest)
    simple_models = commands.get_models(simple_ctx, simple_manifest)
    assert sorted(simple_models) == sorted(csv_models)


def test_simple_document_loads_the_same_property_types(rc):
    csv_ctx, csv_manifest = _csv_manifest(rc)
    simple_ctx, simple_manifest = _simple_manifest(rc, golden())
    csv_models = commands.get_models(csv_ctx, csv_manifest)
    simple_models = commands.get_models(simple_ctx, simple_manifest)
    for name, model in csv_models.items():
        if name.startswith("_"):
            continue
        csv_props = {p: model.flatprops[p].dtype.name for p in model.flatprops if not p.startswith("_")}
        simple_props = {
            p: simple_models[name].flatprops[p].dtype.name
            for p in simple_models[name].flatprops
            if not p.startswith("_")
        }
        assert simple_props == csv_props, name


def test_bad_document_is_refused_before_loading(rc):
    doc = golden()
    doc["widget"] = {}
    with pytest.raises(SimpleManifestError, match="Unknown element type"):
        _simple_manifest(rc, doc)


def test_document_is_valid_before_loading(rc):
    validate_document(golden(), COLUMNS)


def test_invalid_json_is_a_manifest_error(rc):
    rc = configure_rc(rc, [ManifestPath(type="simple", file=StringIO("{not json"))], manifest_type="inline")
    context = create_test_context(rc)
    store = load_store(context, verbose=False)
    with pytest.raises(SimpleManifestError, match="<memory>"):
        commands.load(context, store.manifest, load_internal=False, full_load=True)


def test_comparison_sees_a_changed_cell(rc):
    """The rendering comparison must fail when a cell differs (guards against a vacuous test)."""
    from tests.manifests.simple.test_read import DS

    doc = golden()
    doc["property"][f"{DS}/Country/code"]["given"]["type"] = "integer"
    csv_ctx, csv_manifest = _csv_manifest(rc)
    simple_ctx, simple_manifest = _simple_manifest(rc, doc)
    assert _render(simple_ctx, simple_manifest) != _render(csv_ctx, csv_manifest)
