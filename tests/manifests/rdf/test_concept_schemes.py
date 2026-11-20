"""Concept schemes that no property uses are enums of the main dataset.

A SKOS file with only a concept scheme and its concepts, for example
`https://publications.europa.eu/resource/authority/atu`, has no shapes, so
its scheme is written as an enum of the main dataset. Concepts are
dereferenced when `rdf_fetch_remote` is enabled, the same way as definitions.
"""

from pathlib import Path
from types import SimpleNamespace

from rdflib import Graph

from spinta.manifests.rdf import helpers
from spinta.manifests.rdf.helpers import read_rdf_manifest

PREFIXES = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix ex: <http://example.com/ns#> .
"""

# Only the scheme and its concepts, no shapes. Blue has no label in the file,
# it is dereferenced.
SCHEME_ONLY = """
<http://example.com/vocab/colors> a skos:ConceptScheme ; skos:prefLabel "Colors"@en .
<http://example.com/vocab/colors/Red> a skos:Concept ; skos:inScheme <http://example.com/vocab/colors> ;
    skos:prefLabel "Red"@en .
<http://example.com/vocab/colors/Blue> skos:inScheme <http://example.com/vocab/colors> .
"""

REMOTE_BLUE = """
<http://example.com/vocab/colors/Blue> a skos:Concept ;
    skos:prefLabel "Blue"@en ;
    skos:definition "The colour of the sky."@en ;
    skos:notation "BLU" .
"""

# The scheme is used by a property, so it is an enum of the property only.
SCHEME_PROPERTY = """
ex:DatasetShape a sh:NodeShape ;
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


def _read(tmp_path: Path, ttl: str, fetch_remote: bool = False) -> list[dict]:
    path = tmp_path / "main.ttl"
    path.write_text(PREFIXES + ttl, encoding="utf-8")
    manifests = [SimpleNamespace(path=str(path), format="turtle")]
    return [row for _, row in read_rdf_manifest(manifests, dataset_name="rdf_main", fetch_remote=fetch_remote)]


def _datasets(rows: list[dict]) -> dict[str, dict]:
    return {row["name"]: row for row in rows if row["type"] == "dataset"}


def test_scheme_without_shapes_is_enum_of_main_dataset(tmp_path):
    datasets = _datasets(_read(tmp_path, SCHEME_ONLY))

    assert list(datasets) == ["rdf_main"]
    assert datasets["rdf_main"]["enums"] == {
        "colors": {
            "Blue": {
                "source": "Blue",
                "prepare": "Blue",
                "title": "",
                "description": "",
                "uri": "colors:Blue",
            },
            "Red": {
                "source": "Red",
                "prepare": "Red",
                "title": "Red",
                "description": "",
                "uri": "colors:Red",
            },
        }
    }


def test_scheme_concepts_are_dereferenced_with_remote_fetching(tmp_path, monkeypatch):
    remote = {"http://example.com/vocab/colors/Blue": Graph(bind_namespaces="none")}
    remote["http://example.com/vocab/colors/Blue"].parse(data=PREFIXES + REMOTE_BLUE, format="turtle")
    monkeypatch.setattr(helpers, "_fetch_graph", lambda iri: remote.get(iri))

    items = _datasets(_read(tmp_path, SCHEME_ONLY, fetch_remote=True))["rdf_main"]["enums"]["colors"]

    # Title, description and notation come from the dereferenced concept.
    assert items["Blue"]["title"] == "Blue"
    assert items["Blue"]["description"] == "The colour of the sky."
    assert items["Blue"]["prepare"] == "BLU"
    assert items["Red"]["prepare"] == "Red"


def test_scheme_used_by_property_is_not_duplicated(tmp_path):
    rows = _read(tmp_path, SCHEME_PROPERTY + SCHEME_ONLY)

    enums = [name for row in rows if row["type"] == "dataset" for name in row.get("enums", {})]
    assert enums == ["colors"]
