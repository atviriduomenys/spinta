import pytest
from rdflib import BNode, Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS, SH, XSD

from spinta.manifests.rdf.turtle import TurtleDocument


def _parse(text: str) -> Graph:
    graph = Graph()
    graph.parse(data=text, format="turtle")
    return graph


def test_sections_are_written_in_creation_order_with_comments():
    doc = TurtleDocument()
    doc.bind("ex", "https://example.com/")
    classes = doc.section("Classes")
    shapes = doc.section("SHACL shapes")
    shape = BNode()
    shapes.add(shape, RDF.type, SH.NodeShape)
    classes.add(URIRef("https://example.com/Country"), RDF.type, RDFS.Class)

    text = doc.render()

    assert text.index("# Prefixes") < text.index("# Classes") < text.index("# SHACL shapes")
    assert text.count("# ") == 3


def test_iris_are_abbreviated_with_declared_prefixes():
    doc = TurtleDocument()
    doc.bind("ex", "https://example.com/")
    doc.bind("rdfs", str(RDFS))
    section = doc.section("Classes")
    section.add(URIRef("https://example.com/Country"), RDFS.subClassOf, URIRef("https://example.com/Place"))

    text = doc.render()

    assert "@prefix ex: <https://example.com/> ." in text
    assert "ex:Country" in text
    assert "rdfs:subClassOf ex:Place" in text
    assert "<https://example.com/Country>" not in text


def test_iri_local_names_that_are_not_valid_prefixed_names_are_not_abbreviated():
    doc = TurtleDocument()
    doc.bind("ex", "https://example.com/")
    section = doc.section("Classes")
    section.add(URIRef("https://example.com/a/b"), RDF.type, RDFS.Class)

    assert "<https://example.com/a/b>" in doc.render()


def test_literals_are_escaped_and_typed():
    doc = TurtleDocument()
    doc.bind("ex", "https://example.com/")
    section = doc.section("Shapes")
    subject = URIRef("https://example.com/x")
    section.add(subject, RDFS.comment, Literal('say "hi"\nand\\go'))
    section.add(subject, SH.maxCount, Literal(1))
    section.add(subject, SH.datatype, XSD.string)

    graph = _parse(doc.render())

    assert (subject, RDFS.comment, Literal('say "hi"\nand\\go')) in graph
    assert (subject, SH.maxCount, Literal(1)) in graph
    assert (subject, SH.datatype, XSD.string) in graph


def test_booleans_are_written_without_quotes():
    doc = TurtleDocument()
    section = doc.section("Concepts")
    subject = URIRef("https://example.com/x")
    section.add(subject, URIRef("https://example.com/active"), Literal(True))

    assert "true" in doc.render()
    assert '"true"' not in doc.render()


def test_blank_nodes_used_as_objects_are_written_inline():
    doc = TurtleDocument()
    doc.bind("sh", str(SH))
    doc.bind("ex", "https://example.com/")
    section = doc.section("SHACL shapes")
    shape = BNode()
    prop = BNode()
    section.add(shape, RDF.type, SH.NodeShape)
    section.add(shape, SH.property, prop)
    section.add(prop, SH.path, URIRef("https://example.com/name"))

    text = doc.render()
    graph = _parse(text)

    # Inline blank node is not written as a separate subject.
    assert text.count("sh:path") == 1
    assert len(list(graph.objects(None, SH.property))) == 1
    assert (None, SH.path, URIRef("https://example.com/name")) in graph


def test_invalid_iri_characters_are_rejected():
    doc = TurtleDocument()
    section = doc.section("Classes")
    section.add(URIRef("https://example.com/has space"), RDF.type, RDFS.Class)

    with pytest.raises(ValueError):
        doc.render()


def test_first_prefix_declaration_wins():
    doc = TurtleDocument()
    doc.bind("dct", "http://purl.org/dc/terms/")
    doc.bind("dct", "https://other.example/")

    assert doc.prefixes["dct"] == "http://purl.org/dc/terms/"
