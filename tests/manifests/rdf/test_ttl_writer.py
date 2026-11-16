import csv
import pathlib
from types import SimpleNamespace

import pyshacl
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.compare import isomorphic
from rdflib.namespace import DCTERMS, RDF, RDFS, SH, SKOS, XSD

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
    country = _uri(f"{DATASET}/Country")
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
    kind = _uri(f"{DATASET}/kind")
    assert (kind, RDF.type, RDF.Property) in graph
    assert (kind, RDFS.domain, _uri(f"{DATASET}/Country")) in graph
    # Values of `kind` are concepts of its inline enum.
    assert (kind, RDFS.range, SKOS.Concept) in graph


def test_copy_to_ttl_references_property_with_uri(rc, cli: SpintaCliRunner, tmp_path):
    _copy(rc, cli, tmp_path, _manifest_rows())

    graph = _graph(tmp_path / "result.ttl")
    # `name` has `dct:title`, so its definition is not exported, only referenced.
    assert (DCTERMS.title, RDF.type, RDF.Property) not in graph
    assert (DCTERMS.title, RDF.type, None) not in graph
    assert (None, SH.path, DCTERMS.title) in graph
    assert (_uri(f"{DATASET}/name"), RDF.type, None) not in graph


def test_copy_to_ttl_creates_scheme_for_global_enum(rc, cli: SpintaCliRunner, tmp_path):
    _copy(rc, cli, tmp_path, _manifest_rows())

    graph = _graph(tmp_path / "result.ttl")
    scheme = _uri(f"{DATASET}/Status")
    active = _uri(f"{DATASET}/ACTIVE")
    assert (scheme, RDF.type, SKOS.ConceptScheme) in graph
    assert (active, RDF.type, SKOS.Concept) in graph
    assert (active, SKOS.inScheme, scheme) in graph
    assert (active, SKOS.prefLabel, Literal("Aktyvus")) in graph


def test_copy_to_ttl_names_inline_enum_after_model_and_property(rc, cli: SpintaCliRunner, tmp_path):
    _copy(rc, cli, tmp_path, _manifest_rows())

    graph = _graph(tmp_path / "result.ttl")
    scheme = _uri(f"{DATASET}/Country_kind")
    assert (scheme, RDF.type, SKOS.ConceptScheme) in graph
    # Concepts are `{uri_prefix}{dataset}/{code}`, without the name of their enum.
    assert (_uri(f"{DATASET}/CITY"), RDF.type, SKOS.Concept) in graph
    assert (_uri(f"{DATASET}/TOWN"), SKOS.inScheme, scheme) in graph


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
    assert (active, SKOS.inScheme, _uri(f"{DATASET}/Status")) in graph
    # Concept with an external URI is only referenced.
    done = URIRef("https://other.example.com/done")
    assert (done, RDF.type, SKOS.Concept) not in graph
    assert (done, SKOS.prefLabel, None) not in graph
    assert (done, SKOS.inScheme, _uri(f"{DATASET}/Status")) in graph


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
    assert (_uri(f"{DATASET}/CITY%2FTOWN"), SKOS.prefLabel, Literal("Miestas")) in graph
    assert (_uri(f"{DATASET}/%C3%84%C5%BD"), SKOS.prefLabel, Literal("Raidė")) in graph


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

    code = _uri(f"{DATASET}/code")
    code_shapes = [shape for shape in graph.objects(None, SH.property) if (shape, SH.path, code) in graph]
    assert len(code_shapes) == 1
    assert (code_shapes[0], SH.datatype, XSD.string) in graph
    assert (code_shapes[0], SH.maxCount, Literal(1)) in graph

    tags = _uri(f"{DATASET}/tags")
    tag_shapes = [shape for shape in graph.objects(None, SH.property) if (shape, SH.path, tags) in graph]
    assert len(tag_shapes) == 1
    # Array properties have no maximum count.
    assert (tag_shapes[0], SH.maxCount, None) not in graph


def test_copy_to_ttl_ref_to_model_with_uri_uses_its_uri(rc, cli: SpintaCliRunner, tmp_path):
    _copy(rc, cli, tmp_path, _manifest_rows())

    graph = _graph(tmp_path / "result.ttl")
    org = URIRef("http://xmlns.com/foaf/0.1/Organization")
    shapes = [shape for shape in graph.objects(None, SH.property) if (shape, SH.path, _uri(f"{DATASET}/org")) in graph]
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
    assert (None, SH.targetClass, URIRef(f"{default}{DATASET}/Country")) in graph
    assert (None, SH.path, URIRef(f"{default}{DATASET}/code")) in graph


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
    assert (None, SH.path, _uri(f"{DATASET}/dataset")) not in graph
    assert (None, SH.path, _uri(f"{DATASET}/tags")) not in graph
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
    country = _uri(f"{DATASET}/Country")
    code = _uri(f"{DATASET}/code")

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


def test_code_of_boolean_enum_is_lowercase():
    assert _code("True", SimpleNamespace(prepare=True)) == "true"
    assert _code("False", SimpleNamespace(prepare=False)) == "false"
    assert _code("CREATED", SimpleNamespace(prepare="CREATED")) == "CREATED"
