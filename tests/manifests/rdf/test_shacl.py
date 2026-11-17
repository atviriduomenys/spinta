from rdflib import Graph, URIRef

from spinta.manifests.rdf import helpers
from spinta.manifests.rdf.helpers import (
    _get_schemas,
    _prepare_data,
    _scheme_name,
    _shacl_rows,
    to_snake_case,
)

PREFIXES = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix dct: <http://purl.org/dc/terms/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix ex: <http://example.com/ns#> .
"""


def _prefixes(graph: Graph) -> dict:
    """Prefixes of a dataset, declared in the graph, as `read_rdf_manifest` collects them."""
    return {
        name: {"type": "prefix", "eid": i, "name": name, "uri": str(uri)}
        for i, (name, uri) in enumerate(graph.namespaces())
        if name
    }


def _load(ttl: str, fetch_remote: bool = False) -> tuple[dict, dict]:
    graph = Graph(bind_namespaces="none")
    graph.parse(data=PREFIXES + ttl, format="turtle")
    dataset = {"name": "datasets/shacl", "prefixes": _prefixes(graph)}
    models = _prepare_data(graph, _shacl_rows(graph), dataset)
    rows = {
        row["name"]: row
        for _, row in _get_schemas(
            graph, list(models.values()), dataset, list(models.keys()), fetch_remote=fetch_remote
        )
        if row["type"] == "model"
    }
    return dataset, rows


def _models(ttl: str) -> dict:
    return _load(ttl)[1]


def test_shacl_model_and_properties():
    models = _models("""
        ex:DatasetShape a sh:NodeShape ;
            sh:targetClass dcat:Dataset ;
            rdfs:label "Dataset"@en ;
            rdfs:comment "A dataset."@en ;
            sh:property [
                sh:path dct:title ;
                sh:minCount 1 ;
                sh:maxCount 1 ;
                sh:datatype xsd:string ;
                sh:name "Title"@en ;
                sh:description "Dataset title."@en
            ] ;
            sh:property [
                sh:path dcat:accessURL ;
                sh:maxCount 1 ;
                sh:datatype xsd:anyURI
            ] ;
            sh:property [
                sh:path dct:issued ;
                sh:maxCount 1 ;
                sh:or ( [ sh:datatype xsd:date ] [ sh:datatype xsd:dateTime ] )
            ] .
    """)

    model = models["dcat/Dataset"]
    assert model["title"] == "Dataset"
    assert model["description"] == "A dataset."
    assert model["uri"] == "dcat:Dataset"
    title = model["properties"]["title"]
    assert title["type"] == "string"
    assert title["title"] == "Title"
    assert title["description"] == "Dataset title."
    assert title["uri"] == "dct:title"
    assert title["required"] is True
    assert "items" not in title
    assert model["properties"]["access_url"]["type"] == "uri"
    assert model["properties"]["access_url"]["uri"] == "dcat:accessURL"
    assert model["properties"]["issued"]["type"] == "datetime"
    assert model["properties"]["issued"]["uri"] == "dct:issued"


def test_shacl_title_ignores_deprecation_shapes():
    # Shapes that only report deprecated properties have their own labels,
    # they must not rename the class.
    models = _models("""
        ex:DatasetShape a sh:NodeShape ;
            sh:targetClass dcat:Dataset ;
            rdfs:label "Dataset"@en ;
            sh:property [ sh:path dct:title ; sh:datatype xsd:string ] .

        ex:DeprecatedShape a sh:NodeShape ;
            sh:targetClass dcat:Dataset ;
            rdfs:label "Deprecated property"@en ;
            sh:property [
                sh:path dct:issued ;
                sh:severity sh:Warning ;
                sh:message "Deprecated."
            ] .
    """)

    model = models["dcat/Dataset"]
    assert model["title"] == "Dataset"
    assert "issued" not in model["properties"]
    assert "title" in model["properties"]


def test_shacl_title_is_the_most_common_label():
    models = _models("""
        ex:AShape a sh:NodeShape ; sh:targetClass dcat:Dataset ;
            rdfs:label "Dataset"@en ; sh:property [ sh:path dct:title ; sh:datatype xsd:string ] .
        ex:BShape a sh:NodeShape ; sh:targetClass dcat:Dataset ;
            rdfs:label "Dataset Series"@en ; sh:property [ sh:path dct:issued ; sh:datatype xsd:date ] .
        ex:CShape a sh:NodeShape ; sh:targetClass dcat:Dataset ;
            rdfs:label "Dataset"@en ; sh:property [ sh:path dct:modified ; sh:datatype xsd:date ] .
    """)

    assert models["dcat/Dataset"]["title"] == "Dataset"


def test_shacl_deactivated_shape_is_ignored():
    models = _models("""
        ex:OldShape a sh:NodeShape ;
            sh:targetClass dcat:Dataset ;
            sh:deactivated true ;
            sh:property [ sh:path dct:issued ; sh:datatype xsd:date ] .

        ex:DatasetShape a sh:NodeShape ;
            sh:targetClass dcat:Dataset ;
            sh:property [ sh:path dct:title ; sh:datatype xsd:string ] .
    """)

    assert list(models["dcat/Dataset"]["properties"]) == ["title"]


def test_shacl_deactivated_property_shape_is_ignored():
    models = _models("""
        ex:DatasetShape a sh:NodeShape ;
            sh:targetClass dcat:Dataset ;
            sh:property [ sh:path dct:title ; sh:datatype xsd:string ] ;
            sh:property [
                sh:deactivated true ;
                sh:path dct:issued ;
                sh:minCount 1 ;
                sh:datatype xsd:integer
            ] .
    """)

    assert list(models["dcat/Dataset"]["properties"]) == ["title"]


def test_shacl_node_reference_is_a_ref_to_the_target_class():
    # The value of `dct:publisher` must conform to a shape that targets
    # `foaf:Agent`, so the property is a reference to `Agent`, a single value
    # is not an array.
    models = _models("""
        ex:AgentShape a sh:NodeShape ;
            sh:targetClass foaf:Agent ;
            rdfs:label "Agent"@en ;
            sh:property [ sh:path foaf:name ; sh:datatype xsd:string ] .

        ex:DatasetShape a sh:NodeShape ;
            sh:targetClass dcat:Dataset ;
            sh:property [
                sh:path dct:publisher ;
                sh:maxCount 1 ;
                sh:node ex:AgentShape
            ] .
    """)

    publisher = models["dcat/Dataset"]["properties"]["publisher"]
    assert publisher["type"] == "ref"
    assert publisher["model"] == "foaf/Agent"
    assert "items" not in publisher
    assert "model" not in models["dcat/Dataset"]["properties"]["publisher"].get("items", {})


def test_shacl_multi_valued_reference_is_an_array_of_refs():
    # An array must not carry the `model` of its items, otherwise it is
    # mapped as an intermediate table of a many-to-many relation.
    models = _models("""
        ex:ConceptShape a sh:NodeShape ;
            sh:targetClass skos:Concept ;
            sh:property [ sh:path skos:prefLabel ; sh:datatype xsd:string ] .

        ex:DatasetShape a sh:NodeShape ;
            sh:targetClass dcat:Dataset ;
            sh:property [
                sh:path dcat:theme ;
                sh:class skos:Concept
            ] .
    """)

    theme = models["dcat/Dataset"]["properties"]["theme"]
    assert theme["type"] == "array"
    assert "model" not in theme
    assert theme["items"]["type"] == "ref"
    assert theme["items"]["model"] == "skos/Concept"
    assert theme["items"]["given_name"] == "theme[]"


def test_shacl_reporting_property_without_definitions_is_ignored():
    models = _models("""
        ex:DatasetShape a sh:NodeShape ;
            sh:targetClass dcat:Dataset ;
            sh:property [ sh:path dct:title ; sh:datatype xsd:string ] ;
            sh:property [ sh:path dct:issued ; sh:severity sh:Warning ; sh:message "Recommended." ] .
    """)

    assert list(models["dcat/Dataset"]["properties"]) == ["title"]


def test_to_snake_case_keeps_acronyms_together():
    assert to_snake_case("accessURL") == "access_url"
    assert to_snake_case("downloadURL") == "download_url"
    assert to_snake_case("hasPart") == "has_part"
    assert to_snake_case("spatialResolutionInMeters") == "spatial_resolution_in_meters"
    assert to_snake_case("DCATVersion") == "dcat_version"


# Controlled vocabularies.

COLOR_SCHEME = """
    <http://example.com/vocab/colors> a skos:ConceptScheme .
    ex:Red a skos:Concept ; skos:inScheme <http://example.com/vocab/colors> ; skos:prefLabel "Red"@en .
    ex:Blue a skos:Concept ; skos:inScheme <http://example.com/vocab/colors> .
"""

# A property whose values are restricted to the `colors` scheme, the same way
# DCAT-AP does it with a `sh:node` to a shape with `skos:inScheme` `sh:hasValue`.
COLOR_PROPERTY = """
    ex:DatasetShape a sh:NodeShape ;
        sh:targetClass dcat:Dataset ;
        sh:property [
            sh:path ex:color ;
            %s
            sh:class skos:Concept ;
            sh:node [
                sh:property [ sh:path skos:inScheme ; sh:hasValue <http://example.com/vocab/colors> ]
            ]
        ] .
"""


def _no_fetch(scheme):
    raise AssertionError(f"{scheme} should not be fetched")


def test_shacl_concept_scheme_is_an_enum(monkeypatch):
    # Concepts defined in the sources are used, the scheme is not fetched.
    monkeypatch.setattr(helpers, "_fetch_vocabulary", _no_fetch)
    dataset, models = _load(COLOR_SCHEME + COLOR_PROPERTY % "sh:maxCount 1 ;")

    color = models["dcat/Dataset"]["properties"]["color"]
    assert color["type"] == "string"
    assert color["enum"] == "colors"
    assert "model" not in color
    assert dataset["enums"] == {
        "colors": {
            "Blue": {
                "source": "Blue",
                "prepare": "Blue",
                "title": "",
                "description": "",
                "uri": "ex:Blue",
            },
            "Red": {
                "source": "Red",
                "prepare": "Red",
                "title": "Red",
                "description": "",
                "uri": "ex:Red",
            },
        }
    }


def test_shacl_multi_valued_concept_scheme_is_an_array_of_enum_items(monkeypatch):
    monkeypatch.setattr(helpers, "_fetch_vocabulary", _no_fetch)
    _, models = _load(COLOR_SCHEME + COLOR_PROPERTY % "")

    color = models["dcat/Dataset"]["properties"]["color"]
    assert color["type"] == "array"
    assert "enum" not in color
    assert color["items"]["type"] == "string"
    assert color["items"]["enum"] == "colors"
    assert color["items"]["given_name"] == "color[]"


def test_shacl_concept_scheme_is_fetched_when_not_in_sources(monkeypatch):
    remote = Graph(bind_namespaces="none")
    remote.parse(data=PREFIXES + COLOR_SCHEME, format="turtle")
    fetched = []

    def fetch(scheme):
        fetched.append(scheme)
        return remote

    monkeypatch.setattr(helpers, "_fetch_vocabulary", fetch)
    dataset, _ = _load(COLOR_PROPERTY % "sh:maxCount 1 ;", fetch_remote=True)

    assert fetched == [URIRef("http://example.com/vocab/colors")]
    assert set(dataset["enums"]["colors"]) == {"Red", "Blue"}
    # Titles come from the fetched scheme too.
    assert dataset["enums"]["colors"]["Red"]["title"] == "Red"


def test_shacl_concept_scheme_is_not_fetched_by_default(monkeypatch):
    monkeypatch.setattr(helpers, "_fetch_vocabulary", _no_fetch)
    dataset, models = _load(COLOR_PROPERTY % "sh:maxCount 1 ;")

    color = models["dcat/Dataset"]["properties"]["color"]
    assert color["type"] == "string"
    assert "enums" not in dataset


def test_shacl_concept_scheme_not_available_is_a_string(monkeypatch):
    monkeypatch.setattr(helpers, "_fetch_vocabulary", lambda scheme: None)
    dataset, models = _load(COLOR_PROPERTY % "sh:maxCount 1 ;")

    color = models["dcat/Dataset"]["properties"]["color"]
    assert color["type"] == "string"
    assert "enum" not in color
    assert "enums" not in dataset


# Two schemes with the same local name, `status`, but different IRIs.
STATUS_SCHEMES = """
    <https://a.example/status> a skos:ConceptScheme .
    ex:Open a skos:Concept ; skos:inScheme <https://a.example/status> .
    <https://b.example/status> a skos:ConceptScheme .
    ex:Closed a skos:Concept ; skos:inScheme <https://b.example/status> .
"""


def test_shacl_schemes_with_the_same_local_name_are_separate_enums(monkeypatch):
    # Both schemes are named `status`, but they are different vocabularies, so
    # each property must get its own enum with the concepts of its own scheme.
    monkeypatch.setattr(helpers, "_fetch_vocabulary", _no_fetch)
    dataset, models = _load(
        STATUS_SCHEMES
        + """
        ex:DatasetShape a sh:NodeShape ;
            sh:targetClass dcat:Dataset ;
            sh:property [
                sh:path ex:a_status ; sh:maxCount 1 ; sh:class skos:Concept ;
                sh:node [ sh:property [ sh:path skos:inScheme ; sh:hasValue <https://a.example/status> ] ]
            ] ;
            sh:property [
                sh:path ex:b_status ; sh:maxCount 1 ; sh:class skos:Concept ;
                sh:node [ sh:property [ sh:path skos:inScheme ; sh:hasValue <https://b.example/status> ] ]
            ] .
        """
    )

    props = models["dcat/Dataset"]["properties"]
    a_enum = props["a_status"]["enum"]
    b_enum = props["b_status"]["enum"]
    assert a_enum != b_enum
    assert set(dataset["enums"][a_enum]) == {"Open"}
    assert set(dataset["enums"][b_enum]) == {"Closed"}


def test_shacl_properties_with_the_same_scheme_share_one_enum(monkeypatch):
    monkeypatch.setattr(helpers, "_fetch_vocabulary", _no_fetch)
    dataset, models = _load(
        STATUS_SCHEMES
        + """
        ex:DatasetShape a sh:NodeShape ;
            sh:targetClass dcat:Dataset ;
            sh:property [
                sh:path ex:a_status ; sh:maxCount 1 ; sh:class skos:Concept ;
                sh:node [ sh:property [ sh:path skos:inScheme ; sh:hasValue <https://a.example/status> ] ]
            ] ;
            sh:property [
                sh:path ex:other_status ; sh:maxCount 1 ; sh:class skos:Concept ;
                sh:node [ sh:property [ sh:path skos:inScheme ; sh:hasValue <https://a.example/status> ] ]
            ] .
        """
    )

    props = models["dcat/Dataset"]["properties"]
    assert props["a_status"]["enum"] == props["other_status"]["enum"]
    assert list(dataset["enums"]) == [props["a_status"]["enum"]]


def test_shacl_concept_without_scheme_is_a_ref():
    models = _models("""
        ex:DatasetShape a sh:NodeShape ;
            sh:targetClass dcat:Dataset ;
            sh:property [ sh:path ex:kind ; sh:maxCount 1 ; sh:class skos:Concept ] .
    """)

    kind = models["dcat/Dataset"]["properties"]["kind"]
    assert kind["type"] == "ref"
    assert kind["model"] == "skos/Concept"
    assert "enum" not in kind


def test_scheme_name():
    assert _scheme_name(URIRef("http://publications.europa.eu/resource/authority/dataset-type")) == "dataset_type"
    assert _scheme_name(URIRef("http://purl.org/adms/status/1.0")) == "status"
