"""Reading generated shapes, `{uri_prefix}shape/{dataset}/{model}`, back into models.

A generated class owned by one model must map to that model's name, so the
reader does not create a second model for the same class. A non-owner
generated shape, for example `dct:Location` on `City`, must not create a model.
"""

from pathlib import Path
from types import SimpleNamespace

from spinta.manifests.rdf.helpers import read_rdf_manifest

PREFIXES = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix dct: <http://purl.org/dc/terms/> .
"""

# `Place` owns its class. `City` is a `dct:Location` shape with `sh:node` to `Place`,
# so `dct:Location` is not a model. `Capital` inherits `City`'s class.
EXAMPLE_B = """
<https://example.com/shape/datasets/gov/places/Place> a sh:NodeShape ;
    sh:targetClass <https://example.com/id/datasets/gov/places/Place> ;
    sh:property [ sh:path <https://example.com/id/datasets/gov/places/Place/id> ; sh:datatype xsd:integer ] .

<https://example.com/id/datasets/gov/places/Place> a rdfs:Class .

<https://example.com/shape/datasets/gov/cities/City> a sh:NodeShape ;
    sh:targetClass dct:Location ;
    sh:node <https://example.com/shape/datasets/gov/places/Place> ;
    sh:property [ sh:path <https://example.com/id/datasets/gov/cities/City/id> ; sh:datatype xsd:integer ] .

<https://example.com/shape/datasets/gov/capitals/Capital> a sh:NodeShape ;
    sh:targetClass dct:Location ;
    sh:node <https://example.com/shape/datasets/gov/cities/City> ;
    sh:property [ sh:path <https://example.com/id/datasets/gov/capitals/Capital/id> ; sh:datatype xsd:integer ] .
"""


def _read(tmp_path: Path, ttl: str) -> list[dict]:
    path = tmp_path / "main.ttl"
    path.write_text(PREFIXES + ttl, encoding="utf-8")
    manifests = [SimpleNamespace(path=str(path), format="turtle")]
    return [row for _, row in read_rdf_manifest(manifests, dataset_name="rdf_main")]


def _models(rows: list[dict]) -> dict[str, dict]:
    return {row["name"]: row for row in rows if row["type"] == "model"}


def test_generated_shapes_read_as_models_named_after_shape(tmp_path):
    models = _models(_read(tmp_path, EXAMPLE_B))

    assert set(models) == {"datasets/gov/places/Place", "datasets/gov/cities/City", "datasets/gov/capitals/Capital"}


def test_non_owner_generated_class_does_not_create_model(tmp_path):
    models = _models(_read(tmp_path, EXAMPLE_B))

    assert not [name for name in models if name.endswith("Location")]


def test_base_of_generated_model_is_read_by_shape_name(tmp_path):
    models = _models(_read(tmp_path, EXAMPLE_B))

    assert models["datasets/gov/cities/City"]["base"]["parent"] == "datasets/gov/places/Place"
    assert models["datasets/gov/capitals/Capital"]["base"]["parent"] == "datasets/gov/cities/City"


def test_owner_class_iri_is_uri_of_its_model(tmp_path):
    models = _models(_read(tmp_path, EXAMPLE_B))

    assert models["datasets/gov/places/Place"]["uri"] == "https://example.com/id/datasets/gov/places/Place"
    assert models["datasets/gov/cities/City"]["uri"] == "dct:Location"
