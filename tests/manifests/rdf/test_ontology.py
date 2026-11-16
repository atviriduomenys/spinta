from pathlib import Path
from types import SimpleNamespace

from rdflib import Graph, URIRef

from spinta import commands
from spinta.core.config import RawConfig
from spinta.manifests.rdf import helpers
from spinta.manifests.rdf.helpers import (
    _common_superclass,
    _shacl_rows,
    get_dataset_name,
    read_rdf_graph,
    read_rdf_manifest,
)
from spinta.testing.manifest import load_manifest_and_context

PREFIXES = """
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://example.com/ns#> .
"""

# Classes and properties of an ontology, which is imported by the shapes.
ONTOLOGY = """
@prefix unused: <http://example.com/unused#> .

<http://example.com/ns> a owl:Ontology ;
    rdfs:label "Imported ontology"@en .

ex:Resource a owl:Class ;
    rdfs:label "Resource"@en ;
    rdfs:comment "A resource."@en .

ex:Dataset a owl:Class ;
    rdfs:label "Dataset"@en ;
    rdfs:subClassOf ex:Resource .

ex:Service a owl:Class ;
    rdfs:label "Service"@en ;
    rdfs:subClassOf ex:Resource .

ex:Unrelated a owl:Class ;
    rdfs:label "Unrelated"@en .

ex:title a owl:DatatypeProperty ;
    rdfs:label "Title"@en ;
    rdfs:domain ex:Resource ;
    rdfs:range xsd:string .

ex:unusedProperty a owl:DatatypeProperty ;
    rdfs:domain ex:Unrelated ;
    rdfs:range xsd:string .

unused:Thing a owl:Class ; rdfs:label "Unused"@en .
"""

SHAPES = """
ex:DatasetShape a sh:NodeShape ;
    sh:targetClass ex:Dataset ;
    rdfs:label "Dataset"@en ;
    sh:property [
        sh:path ex:identifier ;
        sh:datatype xsd:string ;
        sh:maxCount 1 ;
    ] .
"""


def _manifest(path: Path) -> SimpleNamespace:
    """Only `path` and `format` are used by the reader, `RdfManifest` is not needed."""
    return SimpleNamespace(path=str(path), format="turtle")


def _write(tmp_path: Path, name: str, content: str) -> Path:
    path = tmp_path / name
    path.write_text(content, encoding="utf-8")
    return path


def _schemas(manifests: list) -> dict:
    """Return model rows of the dataset read from the given files, keyed by model name."""
    rows = {}
    for _, row in read_rdf_manifest(manifests, dataset_name="datasets/onto"):
        if row["type"] == "model":
            rows[row["name"].rsplit("/", 1)[-1]] = row
    return rows


def test_imports_are_followed(tmp_path):
    main = _write(tmp_path, "main.ttl", PREFIXES + SHAPES + "<> a owl:Ontology ; owl:imports <onto.ttl> .")
    _write(tmp_path, "onto.ttl", PREFIXES + ONTOLOGY)

    graph, sources = read_rdf_graph([_manifest(main)])

    assert (URIRef("http://example.com/ns#Resource"), None, None) in graph
    assert (URIRef("http://example.com/ns#Resource"), None, None) not in sources


def test_imports_are_loaded_once_with_cycles(tmp_path):
    main = _write(tmp_path, "main.ttl", PREFIXES + SHAPES + "<> a owl:Ontology ; owl:imports <a.ttl> .")
    _write(tmp_path, "a.ttl", PREFIXES + ONTOLOGY + "<> owl:imports <b.ttl> .")
    _write(tmp_path, "b.ttl", PREFIXES + "ex:B a owl:Class . <> owl:imports <a.ttl>, <main.ttl> .")

    graph, _ = read_rdf_graph([_manifest(main)])

    assert (URIRef("http://example.com/ns#B"), None, None) in graph


def _remote_ontology(monkeypatch, fetched: list[str]) -> None:
    remote = Graph(bind_namespaces="none")
    remote.parse(data=PREFIXES + ONTOLOGY, format="turtle")

    def fetch(iri):
        fetched.append(iri)
        return remote if iri == "https://example.com/onto.ttl" else None

    monkeypatch.setattr(helpers, "_fetch_graph", fetch)


def test_remote_import_is_fetched(tmp_path, monkeypatch):
    fetched = []
    _remote_ontology(monkeypatch, fetched)
    main = _write(tmp_path, "main.ttl", PREFIXES + SHAPES + "<> owl:imports <https://example.com/onto.ttl> .")

    graph, _ = read_rdf_graph([_manifest(main)], fetch_remote=True)

    assert fetched == ["https://example.com/onto.ttl"]
    assert (URIRef("http://example.com/ns#Resource"), None, None) in graph


def test_remote_import_is_not_fetched_by_default(tmp_path, monkeypatch):
    fetched = []
    _remote_ontology(monkeypatch, fetched)
    main = _write(tmp_path, "main.ttl", PREFIXES + SHAPES + "<> owl:imports <https://example.com/onto.ttl> .")

    graph, _ = read_rdf_graph([_manifest(main)])

    assert fetched == []
    assert (URIRef("http://example.com/ns#Resource"), None, None) not in graph


def test_unavailable_import_is_skipped(tmp_path):
    # The autouse fixture makes every web import unavailable.
    main = _write(tmp_path, "main.ttl", PREFIXES + SHAPES + "<> owl:imports <https://example.com/missing.ttl> .")

    rows = _schemas([_manifest(main)])

    assert "Dataset" in rows


def test_imported_superclass_is_base_model(tmp_path):
    main = _write(tmp_path, "main.ttl", PREFIXES + SHAPES + "<> owl:imports <onto.ttl> .")
    _write(tmp_path, "onto.ttl", PREFIXES + ONTOLOGY)

    rows = _schemas([_manifest(main)])

    assert rows["Dataset"]["base"] == {"name": "Resource", "parent": "ex/Resource"}
    assert rows["Dataset"]["title"] == "Dataset"
    assert rows["Resource"]["title"] == "Resource"
    assert rows["Resource"]["description"] == "A resource."
    assert rows["Resource"]["uri"] == "ex:Resource"


def test_imported_classes_and_properties_are_used_only_when_needed(tmp_path):
    main = _write(tmp_path, "main.ttl", PREFIXES + SHAPES + "<> owl:imports <onto.ttl> .")
    _write(tmp_path, "onto.ttl", PREFIXES + ONTOLOGY)

    rows = _schemas([_manifest(main)])

    # Not used by shapes, and not a superclass of a used class.
    assert "Unrelated" not in rows
    assert "Service" not in rows
    # Properties of imported ontologies are not part of the profile.
    assert "title" not in rows["Resource"]["properties"]
    assert "identifier" in rows["Dataset"]["properties"]


def test_ontology_in_given_file_defines_classes_and_properties(tmp_path):
    main = _write(tmp_path, "onto.ttl", PREFIXES + ONTOLOGY)

    rows = _schemas([_manifest(main)])

    assert rows["Dataset"]["base"] == {"name": "Resource", "parent": "ex/Resource"}
    assert rows["Resource"]["properties"]["title"]["type"] == "string"
    assert rows["Resource"]["properties"]["title"]["uri"] == "ex:title"
    assert rows["Resource"]["properties"]["title"]["title"] == "Title"
    assert rows["Unrelated"]["properties"]["unused_property"]["uri"] == "ex:unusedProperty"
    # Classes declared in the given file are models, even if nothing uses them.
    assert "Unrelated" in rows
    assert "Thing" in rows


def test_property_with_class_range_is_ref(tmp_path):
    main = _write(
        tmp_path,
        "main.ttl",
        PREFIXES
        + ONTOLOGY
        + """
        ex:owner a owl:ObjectProperty ;
            rdfs:domain ex:Dataset ;
            rdfs:range ex:Service .
        """,
    )

    rows = _schemas([_manifest(main)])

    owner = rows["Dataset"]["properties"]["owner"]
    assert owner["type"] == "ref"
    assert owner["model"] == "ex/Service"


def test_union_of_subclasses_is_ref_to_superclass():
    graph = Graph(bind_namespaces="none")
    graph.parse(
        data=PREFIXES
        + ONTOLOGY
        + """
        ex:DatasetShape a sh:NodeShape ;
            sh:targetClass ex:Dataset ;
            sh:property [
                sh:path ex:about ;
                sh:or ( [ sh:class ex:Dataset ] [ sh:class ex:Service ] )
            ] .
        """,
        format="turtle",
    )

    assert _common_superclass(
        graph, {URIRef("http://example.com/ns#Dataset"), URIRef("http://example.com/ns#Service")}
    ) == (URIRef("http://example.com/ns#Resource"))
    rows = {row["property"]: row for row in _shacl_rows(graph) if row["property"]}
    assert rows[URIRef("http://example.com/ns#about")]["type"] == URIRef("http://example.com/ns#Resource")


def test_union_without_common_superclass_is_string():
    graph = Graph(bind_namespaces="none")
    graph.parse(
        data=PREFIXES
        + ONTOLOGY
        + """
        ex:DatasetShape a sh:NodeShape ;
            sh:targetClass ex:Dataset ;
            sh:property [
                sh:path ex:about ;
                sh:or ( [ sh:class ex:Dataset ] [ sh:class ex:Undeclared ] )
            ] .
        """,
        format="turtle",
    )

    rows = {row["property"]: row for row in _shacl_rows(graph) if row["property"]}
    assert rows[URIRef("http://example.com/ns#about")]["type"] is None


def test_only_declared_or_used_prefixes_are_kept(tmp_path):
    main = _write(tmp_path, "main.ttl", PREFIXES + SHAPES + "<> owl:imports <onto.ttl> .")
    _write(tmp_path, "onto.ttl", PREFIXES + ONTOLOGY)

    datasets = [
        row for _, row in read_rdf_manifest([_manifest(main)], dataset_name="datasets/onto") if row["type"] == "dataset"
    ]
    prefixes = {name for dataset in datasets for name in dataset["prefixes"]}

    # Classes of `ex:` are in the `ex` dataset, which uses the `ex` prefix.
    assert "ex" in next(d for d in datasets if d["name"] == "ex")["prefixes"]
    assert "unused" not in prefixes


def test_dataset_name_ignores_imported_ontology_title(tmp_path):
    main = _write(tmp_path, "main.ttl", PREFIXES + SHAPES + "<> owl:imports <onto.ttl> .")
    _write(tmp_path, "onto.ttl", PREFIXES + ONTOLOGY)

    graph, sources = read_rdf_graph([_manifest(main)])

    assert get_dataset_name([_manifest(main)], None, sources) == "main"
    assert get_dataset_name([_manifest(main)], None, graph) == "imported_ontology"


def test_ontology_is_loaded_as_manifest(rc: RawConfig, tmp_path: Path):
    main = _write(tmp_path, "main.ttl", PREFIXES + SHAPES + "<> owl:imports <onto.ttl> .")
    _write(tmp_path, "onto.ttl", PREFIXES + ONTOLOGY)

    context, manifest = load_manifest_and_context(rc, main)

    dataset = commands.get_model(context, manifest, "ex/Dataset")
    assert dataset.base.parent.name == "ex/Resource"
    assert dataset.base.level is None
