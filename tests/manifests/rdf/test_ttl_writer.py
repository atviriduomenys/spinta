import csv
import pathlib
from types import SimpleNamespace

import pyshacl
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.collection import Collection
from rdflib.compare import isomorphic
from rdflib.namespace import DCTERMS, OWL, RDF, RDFS, SH, SKOS, XSD

from spinta.manifests.rdf.writer import _code
from spinta.testing.cli import SpintaCliRunner

PREFIX = "https://data.gov.lt/id/"
DATASET = "datasets/gov/example"

COLUMNS = [
    "id",
    "dataset",
    "resource",
    "base",
    "model",
    "property",
    "type",
    "ref",
    "source",
    "prepare",
    "level",
    "access",
    "uri",
    "title",
    "description",
]


def _write_manifest(path: pathlib.Path, rows: list[dict[str, str]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _manifest_rows() -> list[dict[str, str]]:
    return [
        {"dataset": DATASET},
        {"type": "prefix", "ref": "dct", "uri": "http://purl.org/dc/terms/"},
        {"type": "prefix", "ref": "foaf", "uri": "http://xmlns.com/foaf/0.1/"},
        {"type": "enum", "ref": "Status", "prepare": "'ACTIVE'", "title": "Aktyvus"},
        {"type": "enum", "prepare": "'DONE'", "title": "Baigtas"},
        # Model without `uri`, its definitions are exported.
        {"model": "Country", "title": "Šalis", "access": "open"},
        {"property": "name", "type": "string", "access": "open", "uri": "dct:title", "title": "Pavadinimas"},
        {"property": "code", "type": "string", "access": "open", "title": "Kodas"},
        {"property": "status", "type": "string", "ref": "Status", "access": "open"},
        {"property": "kind", "type": "string", "access": "open"},
        {"type": "enum", "prepare": "'CITY'", "title": "Miestas"},
        {"type": "enum", "prepare": "'TOWN'", "title": "Miestelis"},
        {"property": "tags", "type": "array", "access": "open"},
        # Model with `uri`, it is only referenced.
        {"model": "Organization", "uri": "foaf:Organization", "title": "Organizacija", "access": "open"},
        {"property": "title", "type": "string", "access": "open", "uri": "dct:title"},
        {"property": "org", "type": "ref", "ref": "Organization", "access": "open"},
    ]


def _copy(rc, cli: SpintaCliRunner, tmp_path: pathlib.Path, rows, output: str = "result.ttl", fail: bool = True):
    _write_manifest(tmp_path / "manifest.csv", rows)
    rc = rc.fork({"uri_prefix": PREFIX})
    return cli.invoke(
        rc,
        ["copy", "--access", "open", "-o", tmp_path / output, tmp_path / "manifest.csv"],
        fail=fail,
    )


def _graph(path: pathlib.Path) -> Graph:
    graph = Graph()
    graph.parse(path, format="turtle")
    return graph


def _uri(path: str) -> URIRef:
    return URIRef(PREFIX + path)


def test_copy_to_ttl_writes_sections_in_order(rc, cli: SpintaCliRunner, tmp_path):
    _copy(rc, cli, tmp_path, _manifest_rows())

    text = (tmp_path / "result.ttl").read_text(encoding="utf-8")
    markers = ["# Prefixes", "# Classes", "# Properties", "# Concepts", "# SHACL shapes"]
    positions = [text.index(marker) for marker in markers]
    assert positions == sorted(positions)


def test_copy_to_ttl_declares_prefixes(rc, cli: SpintaCliRunner, tmp_path):
    _copy(rc, cli, tmp_path, _manifest_rows())

    graph = _graph(tmp_path / "result.ttl")
    namespaces = dict(graph.namespaces())
    assert namespaces["dct"] == URIRef("http://purl.org/dc/terms/")
    assert namespaces["foaf"] == URIRef("http://xmlns.com/foaf/0.1/")
    assert namespaces["sh"] == URIRef(str(SH))
    assert namespaces["skos"] == URIRef(str(SKOS))


def test_copy_to_ttl_defines_class_without_uri(rc, cli: SpintaCliRunner, tmp_path):
    _copy(rc, cli, tmp_path, _manifest_rows())

    graph = _graph(tmp_path / "result.ttl")
    country = _uri(f"id/{DATASET}/Country")
    assert (country, RDF.type, RDFS.Class) in graph
    assert (country, RDFS.label, Literal("Šalis")) in graph


def test_copy_to_ttl_does_not_define_class_with_uri(rc, cli: SpintaCliRunner, tmp_path):
    _copy(rc, cli, tmp_path, _manifest_rows())

    graph = _graph(tmp_path / "result.ttl")
    organization = URIRef("http://xmlns.com/foaf/0.1/Organization")
    assert (organization, RDF.type, RDFS.Class) not in graph
    assert (organization, RDF.type, None) not in graph
    # But it is referenced by SHACL shape.
    assert (None, SH.targetClass, organization) in graph


def test_copy_to_ttl_defines_property_without_uri(rc, cli: SpintaCliRunner, tmp_path):
    _copy(rc, cli, tmp_path, _manifest_rows())

    graph = _graph(tmp_path / "result.ttl")
    kind = _uri(f"id/{DATASET}/Country/kind")
    assert (kind, RDF.type, RDF.Property) in graph
    assert (kind, RDFS.domain, _uri(f"id/{DATASET}/Country")) in graph
    # Values of `kind` are concepts of its inline enum.
    assert (kind, RDFS.range, SKOS.Concept) in graph


def test_copy_to_ttl_references_property_with_uri(rc, cli: SpintaCliRunner, tmp_path):
    _copy(rc, cli, tmp_path, _manifest_rows())

    graph = _graph(tmp_path / "result.ttl")
    # `name` has `dct:title`, so its definition is not exported, only referenced.
    assert (DCTERMS.title, RDF.type, RDF.Property) not in graph
    assert (DCTERMS.title, RDF.type, None) not in graph
    assert (None, SH.path, DCTERMS.title) in graph
    assert (_uri(f"id/{DATASET}/Country/name"), RDF.type, None) not in graph


def test_copy_to_ttl_creates_scheme_for_global_enum(rc, cli: SpintaCliRunner, tmp_path):
    _copy(rc, cli, tmp_path, _manifest_rows())

    graph = _graph(tmp_path / "result.ttl")
    scheme = _uri(f"def/{DATASET}/Status")
    active = _uri(f"def/{DATASET}/Status/ACTIVE")
    assert (scheme, RDF.type, SKOS.ConceptScheme) in graph
    assert (active, RDF.type, SKOS.Concept) in graph
    assert (active, SKOS.inScheme, scheme) in graph
    assert (active, SKOS.prefLabel, Literal("Aktyvus")) in graph


def test_copy_to_ttl_names_inline_enum_after_model_and_property(rc, cli: SpintaCliRunner, tmp_path):
    _copy(rc, cli, tmp_path, _manifest_rows())

    graph = _graph(tmp_path / "result.ttl")
    scheme = _uri(f"def/{DATASET}/Country_kind")
    assert (scheme, RDF.type, SKOS.ConceptScheme) in graph
    # Concepts are `{uri_prefix}{dataset}/{enum}/{code}`.
    assert (_uri(f"def/{DATASET}/Country_kind/CITY"), RDF.type, SKOS.Concept) in graph
    assert (_uri(f"def/{DATASET}/Country_kind/TOWN"), SKOS.inScheme, scheme) in graph


def test_copy_to_ttl_concepts_of_enums_with_same_code_are_different(rc, cli: SpintaCliRunner, tmp_path):
    rows = [
        {"dataset": DATASET},
        {"type": "enum", "ref": "Status", "prepare": "'ACTIVE'", "access": "open", "title": "Aktyvus"},
        {"type": "enum", "ref": "Kind", "prepare": "'ACTIVE'", "access": "open", "title": "Aktyvi"},
        {"model": "Country", "access": "open"},
        {"property": "status", "type": "string", "ref": "Status", "access": "open"},
        {"property": "kind", "type": "string", "ref": "Kind", "access": "open"},
    ]
    _copy(rc, cli, tmp_path, rows)

    graph = _graph(tmp_path / "result.ttl")
    status = _uri(f"def/{DATASET}/Status/ACTIVE")
    kind = _uri(f"def/{DATASET}/Kind/ACTIVE")
    assert status != kind
    assert (status, SKOS.inScheme, _uri(f"def/{DATASET}/Status")) in graph
    assert (kind, SKOS.inScheme, _uri(f"def/{DATASET}/Kind")) in graph
    assert (status, SKOS.prefLabel, Literal("Aktyvus")) in graph
    assert (kind, SKOS.prefLabel, Literal("Aktyvi")) in graph


def test_copy_to_ttl_property_and_enum_with_same_name_are_different(rc, cli: SpintaCliRunner, tmp_path):
    rows = [
        {"dataset": DATASET},
        {"type": "enum", "ref": "status", "prepare": "'ACTIVE'", "access": "open", "title": "Aktyvus"},
        {"model": "Country", "access": "open"},
        {"property": "status", "type": "string", "ref": "status", "access": "open", "title": "Būsena"},
    ]
    _copy(rc, cli, tmp_path, rows)

    graph = _graph(tmp_path / "result.ttl")
    prop = _uri(f"id/{DATASET}/Country/status")
    scheme = _uri(f"def/{DATASET}/status")
    assert (prop, RDF.type, RDF.Property) in graph
    assert (prop, RDF.type, SKOS.ConceptScheme) not in graph
    assert (prop, RDFS.label, Literal("Būsena")) in graph
    assert (prop, SKOS.prefLabel, None) not in graph
    assert (scheme, RDF.type, SKOS.ConceptScheme) in graph
    assert (scheme, RDF.type, RDF.Property) not in graph
    assert (scheme, SKOS.prefLabel, Literal("status")) in graph
    assert (scheme, RDFS.label, None) not in graph
    assert (_uri(f"def/{DATASET}/status/ACTIVE"), SKOS.inScheme, scheme) in graph


def test_copy_to_ttl_defines_class_with_uri_under_uri_prefix(rc, cli: SpintaCliRunner, tmp_path):
    rows = [
        {"dataset": DATASET},
        {"model": "Country", "uri": PREFIX + f"{DATASET}/Country", "title": "Šalis", "access": "open"},
        {"model": "Region", "uri": PREFIX + "Region", "title": "Regionas", "access": "open"},
    ]
    _copy(rc, cli, tmp_path, rows)

    graph = _graph(tmp_path / "result.ttl")
    # Given `uri` under `uri_prefix` is used as is and defined.
    assert (_uri(f"{DATASET}/Country"), RDF.type, RDFS.Class) in graph
    assert (_uri(f"{DATASET}/Country"), RDFS.label, Literal("Šalis")) in graph
    # Relative `uri` is relative to `uri_prefix`.
    assert (_uri("Region"), RDF.type, RDFS.Class) in graph
    assert (None, SH.targetClass, _uri("Region")) in graph


def test_copy_to_ttl_defines_class_with_prefixed_uri_under_uri_prefix(rc, cli: SpintaCliRunner, tmp_path):
    rows = [
        {"dataset": DATASET},
        {"type": "prefix", "ref": "ex", "uri": PREFIX},
        {"model": "Country", "uri": "ex:Country", "access": "open"},
    ]
    _copy(rc, cli, tmp_path, rows)

    graph = _graph(tmp_path / "result.ttl")
    assert (_uri("Country"), RDF.type, RDFS.Class) in graph
    assert (None, SH.targetClass, _uri("Country")) in graph


def test_copy_to_ttl_defines_property_with_uri_under_uri_prefix(rc, cli: SpintaCliRunner, tmp_path):
    rows = [
        {"dataset": DATASET},
        {"model": "Country", "uri": "https://other.example.com/Country", "access": "open"},
        {"property": "code", "type": "string", "access": "open", "uri": PREFIX + "code", "title": "Kodas"},
        {"property": "name", "type": "string", "access": "open", "uri": "relative/name"},
        {"property": "org", "type": "string", "access": "open", "uri": "https://other.example.com/org"},
    ]
    _copy(rc, cli, tmp_path, rows)

    graph = _graph(tmp_path / "result.ttl")
    # Property under `uri_prefix` is defined, with the domain of its model.
    assert (_uri("code"), RDF.type, RDF.Property) in graph
    assert (_uri("code"), RDFS.label, Literal("Kodas")) in graph
    assert (_uri("code"), RDFS.domain, URIRef("https://other.example.com/Country")) in graph
    # Relative property `uri` is relative to `uri_prefix` and is defined too.
    assert (_uri("relative/name"), RDF.type, RDF.Property) in graph
    # Properties with an external `uri` are only referenced.
    assert (URIRef("https://other.example.com/org"), RDF.type, None) not in graph
    assert (None, SH.path, URIRef("https://other.example.com/org")) in graph


def test_copy_to_ttl_writes_union_domain_of_property_shared_by_models(rc, cli: SpintaCliRunner, tmp_path):
    # Properties with the same `uri` are one property, several `rdfs:domain` triples would mean an intersection.
    rows = [
        {"dataset": DATASET},
        {"model": "Country", "access": "open"},
        {"property": "code", "type": "string", "access": "open", "uri": "relative/code"},
        {"model": "Organization", "access": "open"},
        {"property": "code", "type": "string", "access": "open", "uri": "relative/code"},
    ]
    _copy(rc, cli, tmp_path, rows)

    graph = _graph(tmp_path / "result.ttl")
    code = _uri("relative/code")
    assert (code, RDFS.domain, None) in graph
    assert (code, RDFS.domain, _uri(f"id/{DATASET}/Country")) not in graph
    assert (code, RDFS.domain, _uri(f"id/{DATASET}/Organization")) not in graph
    unions = list(graph.objects(code, RDFS.domain))
    assert len(unions) == 1
    union = unions[0]
    assert (union, RDF.type, OWL.Class) in graph
    members = set(Collection(graph, graph.value(union, OWL.unionOf)))
    assert members == {_uri(f"id/{DATASET}/Country"), _uri(f"id/{DATASET}/Organization")}


def test_copy_to_ttl_property_is_defined_for_each_model(rc, cli: SpintaCliRunner, tmp_path):
    # Generated properties are named after their model, so the same name in two models is two properties.
    rows = [
        {"dataset": DATASET},
        {"model": "Country", "access": "open"},
        {"property": "code", "type": "string", "access": "open"},
        {"model": "Organization", "access": "open"},
        {"property": "code", "type": "string", "access": "open"},
    ]
    _copy(rc, cli, tmp_path, rows)

    graph = _graph(tmp_path / "result.ttl")
    country_code = _uri(f"id/{DATASET}/Country/code")
    organization_code = _uri(f"id/{DATASET}/Organization/code")
    assert (country_code, RDF.type, RDF.Property) in graph
    assert (country_code, RDFS.domain, _uri(f"id/{DATASET}/Country")) in graph
    assert (organization_code, RDF.type, RDF.Property) in graph
    assert (organization_code, RDFS.domain, _uri(f"id/{DATASET}/Organization")) in graph
    assert (_uri(f"id/{DATASET}/code"), RDF.type, None) not in graph


def test_copy_to_ttl_defines_concept_with_uri_under_uri_prefix(rc, cli: SpintaCliRunner, tmp_path):
    rows = [
        {"dataset": DATASET},
        {"type": "enum", "ref": "Status", "prepare": "'ACTIVE'", "uri": PREFIX + "status/active", "title": "Aktyvus"},
        {
            "type": "enum",
            "ref": "Status",
            "prepare": "'DONE'",
            "uri": "https://other.example.com/done",
            "title": "Baigtas",
        },
        {"model": "Country", "access": "open"},
        {"property": "status", "type": "string", "ref": "Status", "access": "open"},
    ]
    _copy(rc, cli, tmp_path, rows)

    graph = _graph(tmp_path / "result.ttl")
    # Concept under `uri_prefix` is defined with the given URI.
    active = _uri("status/active")
    assert (active, RDF.type, SKOS.Concept) in graph
    assert (active, SKOS.prefLabel, Literal("Aktyvus")) in graph
    assert (active, SKOS.inScheme, _uri(f"def/{DATASET}/Status")) in graph
    # Concept with an external URI is only referenced.
    done = URIRef("https://other.example.com/done")
    assert (done, RDF.type, SKOS.Concept) not in graph
    assert (done, SKOS.prefLabel, None) not in graph
    assert (done, SKOS.inScheme, _uri(f"def/{DATASET}/Status")) in graph


def test_copy_to_ttl_concept_uri_is_percent_encoded(rc, cli: SpintaCliRunner, tmp_path):
    rows = [
        {"dataset": DATASET},
        {"model": "Country", "access": "open"},
        {"property": "kind", "type": "string", "access": "open"},
        {"type": "enum", "prepare": "'CITY/TOWN'", "title": "Miestas"},
        {"type": "enum", "prepare": "'ÄŽ'", "title": "Raidė"},
    ]
    _copy(rc, cli, tmp_path, rows)

    graph = _graph(tmp_path / "result.ttl")
    assert (_uri(f"def/{DATASET}/Country_kind/CITY%2FTOWN"), SKOS.prefLabel, Literal("Miestas")) in graph
    assert (_uri(f"def/{DATASET}/Country_kind/%C3%84%C5%BD"), SKOS.prefLabel, Literal("Raidė")) in graph


def _copy_back(rc, cli: SpintaCliRunner, tmp_path: pathlib.Path) -> list[dict[str, str]]:
    """Copy `result.ttl` back to a manifest, and return its rows."""
    rc = rc.fork({"uri_prefix": PREFIX})
    cli.invoke(rc, ["copy", "--access", "open", "-o", tmp_path / "back.csv", tmp_path / "result.ttl"])
    with open(tmp_path / "back.csv", newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def test_copy_to_ttl_round_trip_keeps_prepared_values_that_differ_from_source(rc, cli: SpintaCliRunner, tmp_path):
    rows = [
        {"dataset": DATASET},
        # Source `l` is prepared as `left`, the concept is `/l`, so its prepared value must be kept.
        {"type": "enum", "ref": "Side", "source": "l", "prepare": "'left'", "access": "open", "title": "Kairė"},
        {"type": "enum", "ref": "Side", "source": "r", "prepare": "'right'", "access": "open", "title": "Dešinė"},
        # Source-only item, its source is its prepared value.
        {"type": "enum", "ref": "Side", "source": "c", "access": "open"},
        {"model": "Country", "access": "open"},
        {"property": "side", "type": "string", "ref": "Side", "access": "open"},
    ]
    _copy(rc, cli, tmp_path, rows)

    # Only the first item of an enum has `type` and `ref`, the other items have only `source` and `prepare`.
    enums = {row["source"]: row["prepare"] for row in _copy_back(rc, cli, tmp_path) if row["source"]}
    assert enums == {"l": "'left'", "r": "'right'", "c": "'c'"}


def test_copy_to_ttl_writes_notation_only_when_prepared_value_differs_from_source(rc, cli: SpintaCliRunner, tmp_path):
    rows = [
        {"dataset": DATASET},
        {"type": "enum", "ref": "Side", "source": "l", "prepare": "'left'", "access": "open"},
        {"type": "enum", "ref": "Side", "source": "ACTIVE", "prepare": "'ACTIVE'", "access": "open"},
        {"type": "enum", "ref": "Side", "source": "c", "access": "open"},
        {"type": "enum", "ref": "Flag", "source": "True", "prepare": "true", "access": "open"},
        {"model": "Country", "access": "open"},
        {"property": "side", "type": "string", "ref": "Side", "access": "open"},
        {"property": "flag", "type": "boolean", "ref": "Flag", "access": "open"},
    ]
    _copy(rc, cli, tmp_path, rows)

    graph = _graph(tmp_path / "result.ttl")
    assert (_uri(f"def/{DATASET}/Side/l"), SKOS.notation, Literal("left")) in graph
    # Prepared value of the same text as the source is not repeated.
    assert (_uri(f"def/{DATASET}/Side/ACTIVE"), SKOS.notation, None) not in graph
    # Source-only item has no notation.
    assert (_uri(f"def/{DATASET}/Side/c"), SKOS.notation, None) not in graph
    # Boolean code is `true`, its prepared value is the boolean itself.
    assert (_uri(f"def/{DATASET}/Flag/true"), SKOS.notation, Literal(True)) in graph


def test_copy_to_ttl_uri_prefix_trailing_slash_is_optional(rc, cli: SpintaCliRunner, tmp_path):
    _write_manifest(tmp_path / "manifest.csv", _manifest_rows())

    outputs = []
    for prefix in (PREFIX, PREFIX.rstrip("/")):
        cli.invoke(
            rc.fork({"uri_prefix": prefix}),
            ["copy", "--access", "open", "-o", tmp_path / f"{len(outputs)}.ttl", tmp_path / "manifest.csv"],
        )
        outputs.append(_graph(tmp_path / f"{len(outputs)}.ttl"))

    assert isomorphic(outputs[0], outputs[1])


def test_copy_to_ttl_shapes_constrain_values(rc, cli: SpintaCliRunner, tmp_path):
    _copy(rc, cli, tmp_path, _manifest_rows())

    graph = _graph(tmp_path / "result.ttl")
    shapes = list(graph.subjects(RDF.type, SH.NodeShape))
    assert shapes

    code = _uri(f"id/{DATASET}/Country/code")
    code_shapes = [shape for shape in graph.objects(None, SH.property) if (shape, SH.path, code) in graph]
    assert len(code_shapes) == 1
    assert (code_shapes[0], SH.datatype, XSD.string) in graph
    assert (code_shapes[0], SH.maxCount, Literal(1)) in graph

    tags = _uri(f"id/{DATASET}/Country/tags")
    tag_shapes = [shape for shape in graph.objects(None, SH.property) if (shape, SH.path, tags) in graph]
    assert len(tag_shapes) == 1
    # Array properties have no maximum count.
    assert (tag_shapes[0], SH.maxCount, None) not in graph


def test_copy_to_ttl_ref_to_model_with_uri_uses_its_uri(rc, cli: SpintaCliRunner, tmp_path):
    _copy(rc, cli, tmp_path, _manifest_rows())

    graph = _graph(tmp_path / "result.ttl")
    org = URIRef("http://xmlns.com/foaf/0.1/Organization")
    shapes = [
        shape
        for shape in graph.objects(None, SH.property)
        if (shape, SH.path, _uri(f"id/{DATASET}/Organization/org")) in graph
    ]
    assert len(shapes) == 1
    assert (shapes[0], SH["class"], org) in graph


def test_copy_to_ttl_uses_default_uri_prefix(rc, cli: SpintaCliRunner, tmp_path):
    _write_manifest(tmp_path / "manifest.csv", _manifest_rows())

    cli.invoke(
        rc,
        ["copy", "--access", "open", "-o", tmp_path / "result.ttl", tmp_path / "manifest.csv"],
    )

    graph = _graph(tmp_path / "result.ttl")
    default = "https://example.com/id/"
    assert (None, SH.targetClass, URIRef(f"{default}id/{DATASET}/Country")) in graph
    assert (None, SH.path, URIRef(f"{default}id/{DATASET}/Country/code")) in graph


def test_copy_to_ttl_without_generated_uris_does_not_need_uri_prefix(rc, cli: SpintaCliRunner, tmp_path):
    rows = [
        {"dataset": DATASET},
        {"type": "prefix", "ref": "dct", "uri": "http://purl.org/dc/terms/"},
        {"type": "prefix", "ref": "foaf", "uri": "http://xmlns.com/foaf/0.1/"},
        {"model": "Organization", "uri": "foaf:Organization", "title": "Organizacija", "access": "open"},
        {"property": "title", "type": "string", "access": "open", "uri": "dct:title"},
    ]
    _write_manifest(tmp_path / "manifest.csv", rows)

    cli.invoke(
        rc,
        ["copy", "--access", "open", "-o", tmp_path / "result.ttl", tmp_path / "manifest.csv"],
    )

    graph = _graph(tmp_path / "result.ttl")
    assert (None, SH.targetClass, URIRef("http://xmlns.com/foaf/0.1/Organization")) in graph


def test_copy_to_ttl_array_properties_keep_their_uri(rc, cli: SpintaCliRunner, tmp_path):
    rows = [
        {"dataset": DATASET},
        {"type": "prefix", "ref": "dcat", "uri": "http://www.w3.org/ns/dcat#"},
        {"model": "Catalog", "uri": "dcat:Catalog", "access": "open"},
        {"property": "dataset[]", "type": "ref", "ref": "Dataset", "access": "open", "uri": "dcat:dataset"},
        {"property": "tags[]", "type": "string", "access": "open", "uri": "dcat:keyword"},
        {"model": "Dataset", "uri": "dcat:Dataset", "access": "open"},
    ]
    _copy(rc, cli, tmp_path, rows)

    graph = _graph(tmp_path / "result.ttl")
    dcat = Namespace("http://www.w3.org/ns/dcat#")
    # Items keep the URI given for them, no generated URI is used for the array.
    assert (None, SH.path, dcat.dataset) in graph
    assert (None, SH.path, dcat.keyword) in graph
    assert (None, SH.path, _uri(f"id/{DATASET}/Catalog/dataset")) not in graph
    assert (None, SH.path, _uri(f"id/{DATASET}/Catalog/tags")) not in graph
    dataset_shapes = [shape for shape in graph.objects(None, SH.property) if (shape, SH.path, dcat.dataset) in graph]
    assert (dataset_shapes[0], SH["class"], dcat.Dataset) in graph


def test_copy_to_ttl_undeclared_prefix_is_an_error(rc, cli: SpintaCliRunner, tmp_path):
    rows = [
        {"dataset": DATASET},
        {"model": "Catalog", "uri": "dcat:Catalog", "access": "open"},
    ]
    _write_manifest(tmp_path / "manifest.csv", rows)

    result = cli.invoke(
        rc.fork({"uri_prefix": PREFIX}),
        ["copy", "--access", "open", "-o", tmp_path / "result.ttl", tmp_path / "manifest.csv"],
        fail=False,
    )

    assert result.exit_code != 0
    assert "'dcat' is not declared" in str(result.exception)
    assert not (tmp_path / "result.ttl").exists()


def test_copy_to_ttl_expands_uris_with_their_own_dataset_prefixes(rc, cli: SpintaCliRunner, tmp_path):
    # Both datasets declare `ex`, but for different namespaces. Each `ex:` URI
    # must be expanded with the prefix of the dataset that declares it.
    rows = [
        {"dataset": "datasets/a"},
        {"type": "prefix", "ref": "ex", "uri": "http://a.example/ns#"},
        {"model": "Country", "uri": "ex:Country", "access": "open"},
        {"property": "name", "type": "string", "access": "open", "uri": "ex:name"},
        {"dataset": "datasets/b"},
        {"type": "prefix", "ref": "ex", "uri": "http://b.example/ns#"},
        {"model": "Country", "uri": "ex:Country", "access": "open"},
        {"property": "name", "type": "string", "access": "open", "uri": "ex:name"},
    ]
    _copy(rc, cli, tmp_path, rows)

    graph = _graph(tmp_path / "result.ttl")
    # Each namespace is declared once, under its own name.
    assert {"http://a.example/ns#", "http://b.example/ns#"} <= {str(ns) for _, ns in graph.namespaces()}
    assert set(graph.objects(None, SH.targetClass)) == {
        URIRef("http://a.example/ns#Country"),
        URIRef("http://b.example/ns#Country"),
    }
    assert {URIRef("http://a.example/ns#name"), URIRef("http://b.example/ns#name")} <= set(graph.objects(None, SH.path))


def test_copy_to_ttl_expands_concept_uris_with_their_own_dataset_prefixes(rc, cli: SpintaCliRunner, tmp_path):
    rows = [
        {"dataset": "datasets/a"},
        {"type": "prefix", "ref": "ex", "uri": "http://a.example/ns#"},
        {"model": "Country", "uri": "ex:Country", "access": "open"},
        {"property": "status", "type": "string", "access": "open"},
        {"type": "enum", "prepare": "'ACTIVE'", "uri": "ex:active"},
        {"dataset": "datasets/b"},
        {"type": "prefix", "ref": "ex", "uri": "http://b.example/ns#"},
        {"model": "Country", "uri": "ex:Country", "access": "open"},
        {"property": "status", "type": "string", "access": "open"},
        {"type": "enum", "prepare": "'ACTIVE'", "uri": "ex:active"},
    ]
    _copy(rc, cli, tmp_path, rows)

    graph = _graph(tmp_path / "result.ttl")
    schemes = set(graph.objects(None, SKOS.inScheme))
    concepts = {s for s, _, _ in graph.triples((None, SKOS.inScheme, None))}
    assert URIRef("http://a.example/ns#active") in concepts
    assert URIRef("http://b.example/ns#active") in concepts
    assert len(schemes) == 2


def test_copy_to_ttl_same_uri_in_two_datasets_is_one_prefix(rc, cli: SpintaCliRunner, tmp_path):
    rows = [
        {"dataset": "datasets/a"},
        {"type": "prefix", "ref": "cv", "uri": "http://data.example/m8g/"},
        {"model": "Unit", "uri": "cv:AdminUnit", "access": "open"},
        {"dataset": "datasets/b"},
        {"type": "prefix", "ref": "cv", "uri": "http://data.example/m8g/"},
        {"model": "Unit", "uri": "cv:AdminUnit", "access": "open"},
    ]
    _copy(rc, cli, tmp_path, rows)

    text = (tmp_path / "result.ttl").read_text(encoding="utf-8")
    assert text.count("@prefix cv:") == 1
    assert "cv_2" not in text


def test_copy_to_ttl_uris_differing_by_trailing_slash_get_different_prefixes(rc, cli: SpintaCliRunner, tmp_path):
    rows = [
        {"dataset": "datasets/a"},
        {"type": "prefix", "ref": "cv", "uri": "http://data.example/m8g"},
        {"model": "Unit", "uri": "cv:AdminUnit", "access": "open"},
        {"dataset": "datasets/b"},
        {"type": "prefix", "ref": "cv", "uri": "http://data.example/m8g/"},
        {"model": "Unit", "uri": "cv:AdminUnit", "access": "open"},
    ]
    _copy(rc, cli, tmp_path, rows)

    text = (tmp_path / "result.ttl").read_text(encoding="utf-8")
    assert "@prefix cv: <http://data.example/m8g> ." in text
    assert "@prefix cv_2: <http://data.example/m8g/> ." in text


# SHACL vocabulary described in SHACL, used to check that generated shapes are valid SHACL.
SHACL_SHACL = pathlib.Path(pyshacl.__file__).parent / "assets" / "shacl-shacl.ttl"
EX = Namespace("https://example.com/data/")


def test_copy_to_ttl_shapes_are_valid_shacl(rc, cli: SpintaCliRunner, tmp_path):
    _copy(rc, cli, tmp_path, _manifest_rows())

    shapes = _graph(tmp_path / "result.ttl")
    conforms, _, report = pyshacl.validate(shapes, shacl_graph=_graph(SHACL_SHACL), data_graph_format="turtle")
    assert conforms, report


def test_copy_to_ttl_shapes_validate_data(rc, cli: SpintaCliRunner, tmp_path):
    _copy(rc, cli, tmp_path, _manifest_rows())
    shapes = _graph(tmp_path / "result.ttl")
    country = _uri(f"id/{DATASET}/Country")
    code = _uri(f"id/{DATASET}/Country/code")

    valid = Graph()
    valid.add((EX.country1, RDF.type, country))
    valid.add((EX.country1, code, Literal("LT")))
    conforms, _, report = pyshacl.validate(valid, shacl_graph=shapes)
    assert conforms, report

    # `code` is a string, a number is not valid.
    wrong_datatype = Graph()
    wrong_datatype.add((EX.country2, RDF.type, country))
    wrong_datatype.add((EX.country2, code, Literal(5)))
    conforms, _, _ = pyshacl.validate(wrong_datatype, shacl_graph=shapes)
    assert not conforms

    # `code` can have only one value.
    repeated = Graph()
    repeated.add((EX.country3, RDF.type, country))
    repeated.add((EX.country3, code, Literal("LT")))
    repeated.add((EX.country3, code, Literal("EE")))
    conforms, _, _ = pyshacl.validate(repeated, shacl_graph=shapes)
    assert not conforms


def test_copy_to_ttl_skips_enum_items_below_access(rc, cli: SpintaCliRunner, tmp_path):
    rows = [
        {"dataset": DATASET},
        {"type": "enum", "ref": "Status", "prepare": "'ACTIVE'", "access": "open", "title": "Aktyvus"},
        {"type": "enum", "prepare": "'SECRET'", "access": "private", "title": "Slaptas", "description": "Neskelbtinas"},
        {"model": "Country", "access": "open"},
        {"property": "status", "type": "string", "ref": "Status", "access": "open"},
    ]
    _copy(rc, cli, tmp_path, rows)

    graph = _graph(tmp_path / "result.ttl")
    assert (_uri(f"def/{DATASET}/Status/ACTIVE"), SKOS.prefLabel, Literal("Aktyvus")) in graph
    assert (_uri(f"def/{DATASET}/Status/SECRET"), None, None) not in graph
    assert (None, None, Literal("Slaptas")) not in graph
    assert (None, None, Literal("Neskelbtinas")) not in graph


def test_copy_to_ttl_does_not_export_enums_of_namespace_without_exported_models(rc, cli: SpintaCliRunner, tmp_path):
    rows = [
        {"dataset": DATASET},
        {"type": "enum", "ref": "Status", "prepare": "'ACTIVE'", "access": "open"},
        {"model": "Country", "access": "open"},
        {"property": "code", "type": "string", "access": "open"},
        {"dataset": "datasets/gov/hidden"},
        {"type": "enum", "ref": "Secret", "prepare": "'X'", "access": "open", "title": "Paslaptis"},
        {"model": "Hidden", "access": "private"},
        {"property": "code", "type": "string", "access": "private"},
    ]
    _copy(rc, cli, tmp_path, rows)

    graph = _graph(tmp_path / "result.ttl")
    assert (_uri("id/datasets/gov/hidden/Secret"), None, None) not in graph
    assert (_uri("id/datasets/gov/hidden/X"), None, None) not in graph
    assert (None, None, Literal("Paslaptis")) not in graph
    # The exported namespace keeps its enums.
    assert (_uri(f"def/{DATASET}/Status"), RDF.type, SKOS.ConceptScheme) in graph


def test_copy_to_ttl_exports_enums_of_namespace_with_exported_child_namespace(rc, cli: SpintaCliRunner, tmp_path):
    rows = [
        {"dataset": "datasets/gov"},
        {"type": "enum", "ref": "Parent", "prepare": "'P'", "access": "open", "title": "Tevinis"},
        {"dataset": DATASET},
        {"model": "Country", "access": "open"},
        {"property": "code", "type": "string", "access": "open"},
        {"property": "parent", "type": "string", "ref": "Parent", "access": "open"},
    ]
    _copy(rc, cli, tmp_path, rows)

    graph = _graph(tmp_path / "result.ttl")
    assert (_uri("def/datasets/gov/Parent"), RDF.type, SKOS.ConceptScheme) in graph
    assert (_uri("def/datasets/gov/Parent/P"), SKOS.prefLabel, Literal("Tevinis")) in graph


def test_code_of_boolean_enum_is_lowercase():
    assert _code("True", SimpleNamespace(prepare=True)) == "true"
    assert _code("False", SimpleNamespace(prepare=False)) == "false"
    assert _code("CREATED", SimpleNamespace(prepare="CREATED")) == "CREATED"
