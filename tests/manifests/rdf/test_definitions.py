from types import SimpleNamespace

from rdflib import Graph, URIRef

from spinta.manifests.rdf import helpers
from spinta.manifests.rdf.helpers import (
    _concept_uri,
    _get_schemas,
    _prepare_data,
    _shacl_rows,
    get_dataset_name,
)
from tests.manifests.rdf.test_shacl import PREFIXES, _prefixes

REMOTE_PROPERTY = """
<http://purl.org/dc/terms/title> rdfs:label "Title"@en ;
    rdfs:comment "A name given to the resource."@en .
"""

REMOTE_CONCEPT = """
ex:Blue a skos:Concept ; skos:prefLabel "Blue"@en ; skos:definition "The colour of the sky."@en .
"""

SOURCE = """
ex:DatasetShape a sh:NodeShape ;
    sh:targetClass dcat:Dataset ;
    rdfs:label "Dataset"@en ;
    sh:property [
        sh:path dct:title ;
        sh:minCount 1 ;
        sh:datatype xsd:string ;
    ] ;
    sh:property [
        sh:path ex:color ;
        sh:maxCount 1 ;
        sh:class skos:Concept ;
        sh:node [
            sh:property [ sh:path skos:inScheme ; sh:hasValue ex:colors ]
        ]
    ] .

ex:colors a skos:ConceptScheme .
ex:Blue a skos:Concept ; skos:inScheme ex:colors .
"""


def _load(monkeypatch, remote: dict[str, Graph], fetch_remote: bool = True):
    """Load SOURCE, IRIs in `remote` are looked up in their graphs when `fetch_remote` is enabled."""
    looked_up = []

    def fetch(iri):
        looked_up.append(iri)
        return remote.get(iri)

    monkeypatch.setattr(helpers, "_fetch_graph", fetch)
    graph = Graph(bind_namespaces="none")
    graph.parse(data=PREFIXES + SOURCE, format="turtle")
    dataset = {"name": "datasets/shacl", "prefixes": _prefixes(graph)}
    models = _prepare_data(graph, _shacl_rows(graph), dataset)
    rows = {
        row["name"]: row
        for _, row in _get_schemas(
            graph, list(models.values()), dataset, list(models.keys()), fetch_remote=fetch_remote
        )
        if row["type"] == "model"
    }
    return dataset, rows, looked_up


def _remote_graph(ttl: str) -> Graph:
    graph = Graph(bind_namespaces="none")
    graph.parse(data=PREFIXES + ttl, format="turtle")
    return graph


def test_definitions_missing_in_sources_are_looked_up(monkeypatch):
    remote = {
        "http://purl.org/dc/terms/title": _remote_graph(REMOTE_PROPERTY),
        "http://example.com/ns#Blue": _remote_graph(REMOTE_CONCEPT),
    }
    dataset, models, looked_up = _load(monkeypatch, remote)

    title = models["dcat/Dataset"]["properties"]["title"]
    assert title["title"] == "Title"
    assert title["description"] == "A name given to the resource."

    assert dataset["enums"]["colors"]["Blue"]["title"] == "Blue"
    assert dataset["enums"]["colors"]["Blue"]["description"] == "The colour of the sky."
    # Every IRI without a complete definition in the sources is looked up once.
    assert "http://example.com/ns#Blue" in looked_up
    assert "http://purl.org/dc/terms/title" in looked_up
    assert len(looked_up) == len(set(looked_up))


def test_definitions_not_available_are_empty(monkeypatch):
    dataset, models, looked_up = _load(monkeypatch, {})
    title = models["dcat/Dataset"]["properties"]["title"]
    assert title["title"] == ""
    assert title["description"] == ""
    assert dataset["enums"]["colors"]["Blue"]["title"] == ""


def test_definitions_are_not_looked_up_when_remote_fetching_is_disabled(monkeypatch):
    remote = {
        "http://purl.org/dc/terms/title": _remote_graph(REMOTE_PROPERTY),
        "http://example.com/ns#Blue": _remote_graph(REMOTE_CONCEPT),
    }
    dataset, models, looked_up = _load(monkeypatch, remote, fetch_remote=False)

    assert looked_up == []
    assert models["dcat/Dataset"]["properties"]["title"]["title"] == ""
    assert dataset["enums"]["colors"]["Blue"]["title"] == ""


def test_concept_uri_declares_namespace_prefix():
    dataset = {"prefixes": {}}
    concept = URIRef("http://publications.europa.eu/resource/authority/dataset-type/APROF")

    assert _concept_uri(dataset, concept) == "dataset-type:APROF"
    assert dataset["prefixes"]["dataset-type"] == {
        "type": "prefix",
        "eid": 0,
        "name": "dataset-type",
        "uri": "http://publications.europa.eu/resource/authority/dataset-type/",
    }
    # The namespace is declared now, so it is reused.
    assert _concept_uri(dataset, URIRef("http://publications.europa.eu/resource/authority/dataset-type/OP_DATPRO")) == (
        "dataset-type:OP_DATPRO"
    )
    assert len(dataset["prefixes"]) == 1


def test_concept_uri_uses_declared_prefix():
    dataset = {"prefixes": {"ex": {"type": "prefix", "eid": 0, "name": "ex", "uri": "http://example.com/ns#"}}}
    assert _concept_uri(dataset, URIRef("http://example.com/ns#Blue")) == "ex:Blue"


def test_concept_uri_not_shortened_when_code_is_not_a_name():
    dataset = {"prefixes": {}}
    concept = URIRef("http://example.com/vocab/1st%20place")
    assert _concept_uri(dataset, concept) == "http://example.com/vocab/1st%20place"
    assert dataset["prefixes"] == {}


def test_dataset_name_explicit_name_wins():
    manifests = [SimpleNamespace(path="deprecateduris.ttl")]
    assert get_dataset_name(manifests, "my_dataset", Graph()) == "my_dataset"


def test_dataset_name_from_ontology_title():
    graph = Graph(bind_namespaces="none")
    graph.parse(
        data="""
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix dct: <http://purl.org/dc/terms/> .
        <http://example.com/onto> a owl:Ontology ; dct:title "DCAT Application Profile"@en .
        """,
        format="turtle",
    )
    manifests = [SimpleNamespace(path="/some/dir/imports.ttl")]
    assert get_dataset_name(manifests, None, graph) == "dcat_application_profile"


def test_dataset_name_from_common_directory():
    graph = Graph(bind_namespaces="none")
    manifests = [
        SimpleNamespace(path="/some/shacl/deprecateduris.ttl"),
        SimpleNamespace(path="/some/shacl/shapes.ttl"),
    ]
    assert get_dataset_name(manifests, None, graph) == "shacl"


def test_dataset_name_from_single_file_name():
    graph = Graph(bind_namespaces="none")
    manifests = [SimpleNamespace(path="/some/dir/deprecateduris.ttl")]
    assert get_dataset_name(manifests, None, graph) == "deprecateduris"


def test_dataset_name_from_file_name_with_dots():
    manifests = [SimpleNamespace(path="mdr-vocabularies.shape.ttl")]
    assert get_dataset_name(manifests) == "mdr_vocabularies_shape"


def test_definitions_host_not_available_is_not_looked_up_again(monkeypatch):
    calls = []

    def fetch(iri):
        calls.append(iri)
        raise helpers._Unavailable("503 Service Unavailable")

    monkeypatch.setattr(helpers, "_fetch_graph", fetch)
    definitions = helpers._Definitions(Graph(bind_namespaces="none"), "en")
    for i in range(5):
        assert definitions._lookup(f"http://bad.example.com/{i}") == ("", "")

    assert len(calls) == helpers._HOST_FAILURE_LIMIT
