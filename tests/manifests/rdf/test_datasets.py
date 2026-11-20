import itertools
from pathlib import Path
from types import SimpleNamespace

from spinta.manifests.rdf.helpers import read_rdf_manifest

PREFIXES = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix dct: <http://purl.org/dc/terms/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix ex: <http://example.com/ns#> .
"""

# `dcat:Dataset` and `dct:Location` are prefixed, `<http://other.example.com/Thing>`
# has no declared prefix, so it stays in the main dataset.
DATASET_SHAPE = """
dcat:DatasetShape a sh:NodeShape ;
    sh:targetClass dcat:Dataset ;
    sh:property [ sh:path dct:title ; sh:datatype xsd:string ] ;
    sh:property [ sh:path dcat:catalog ; sh:class dcat:Catalog ; sh:maxCount 1 ] ;
    sh:property [ sh:path dct:publisher ; sh:class dct:Location ; sh:maxCount 1 ] .
"""

CATALOG_SHAPE = """
dcat:CatalogShape a sh:NodeShape ;
    sh:targetClass dcat:Catalog ;
    sh:property [ sh:path dct:title ; sh:datatype xsd:string ] .
"""

LOCATION_SHAPE = """
dct:LocationShape a sh:NodeShape ;
    sh:targetClass dct:Location ;
    sh:property [ sh:path dct:title ; sh:datatype xsd:string ] .
"""

THING_SHAPE = """
ex:ThingShape a sh:NodeShape ;
    sh:targetClass <http://other.example.com/Thing> ;
    sh:property [ sh:path dct:title ; sh:datatype xsd:string ] .
"""

SHAPES = DATASET_SHAPE + CATALOG_SHAPE + LOCATION_SHAPE + THING_SHAPE

# A property whose values are restricted to a concept scheme, used by a
# class with a prefix.
COLOR_SCHEME = """
<http://example.com/vocab/colors> a skos:ConceptScheme .
ex:Red a skos:Concept ; skos:inScheme <http://example.com/vocab/colors> ; skos:prefLabel "Red"@en .
ex:Blue a skos:Concept ; skos:inScheme <http://example.com/vocab/colors> .

dcat:DatasetShape a sh:NodeShape ;
    sh:targetClass dcat:Dataset ;
    sh:property [
        sh:path ex:color ;
        sh:maxCount 1 ;
        sh:class skos:Concept ;
        sh:node [
            sh:property [ sh:path skos:inScheme ; sh:hasValue <http://example.com/vocab/colors> ]
        ]
    ] .
"""


def _read(tmp_path: Path, ttl: str) -> list[dict]:
    path = tmp_path / "main.ttl"
    path.write_text(PREFIXES + ttl, encoding="utf-8")
    manifests = [SimpleNamespace(path=str(path), format="turtle")]
    return [row for _, row in read_rdf_manifest(manifests, dataset_name="rdf_main")]


def _datasets(rows: list[dict]) -> dict[str, dict]:
    return {row["name"]: row for row in rows if row["type"] == "dataset"}


def _models(rows: list[dict]) -> dict[str, dict]:
    return {row["name"]: row for row in rows if row["type"] == "model"}


def test_models_are_grouped_into_datasets_by_prefix(tmp_path):
    rows = _read(tmp_path, SHAPES)

    assert set(_datasets(rows)) == {"rdf_main", "dcat", "dct"}
    assert {name: model["external"]["dataset"] for name, model in _models(rows).items()} == {
        "dcat/Dataset": "dcat",
        "dcat/Catalog": "dcat",
        "dct/Location": "dct",
        "rdf_main/Thing": "rdf_main",
    }


def test_main_dataset_is_left_out_when_all_models_have_prefix(tmp_path):
    rows = _read(tmp_path, DATASET_SHAPE + CATALOG_SHAPE + LOCATION_SHAPE)

    assert set(_datasets(rows)) == {"dcat", "dct"}


def test_dataset_rows_come_first_and_models_are_grouped_by_dataset(tmp_path):
    rows = [row for row in _read(tmp_path, SHAPES) if row["type"] in ("dataset", "model")]

    types = [row["type"] for row in rows]
    assert types == sorted(types, key=lambda t: t != "dataset")

    model_datasets = [row["external"]["dataset"] for row in rows if row["type"] == "model"]
    groups = [key for key, _ in itertools.groupby(model_datasets)]
    assert len(groups) == len(set(groups))


def test_reference_to_model_of_other_dataset_uses_full_name(tmp_path):
    rows = _read(tmp_path, SHAPES)

    dataset = _models(rows)["dcat/Dataset"]["properties"]
    assert dataset["publisher"]["type"] == "ref"
    assert dataset["publisher"]["model"] == "dct/Location"
    assert dataset["catalog"]["model"] == "dcat/Catalog"


def test_enum_belongs_to_dataset_of_property_model(tmp_path):
    rows = _read(tmp_path, COLOR_SCHEME + CATALOG_SHAPE + LOCATION_SHAPE + THING_SHAPE)

    datasets = _datasets(rows)
    assert set(datasets["dcat"]["enums"]) == {"colors"}
    assert "enums" not in datasets["dct"]
    assert "enums" not in datasets["rdf_main"]
