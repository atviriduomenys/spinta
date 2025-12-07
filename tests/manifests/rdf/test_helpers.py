from rdflib import Graph
from rdflib.term import Literal

from spinta.manifests.rdf.helpers import _prepare_data, _get_schemas


def test_rdf_with_one_schema():
    graph = Graph()
    dataset = {"name": "datasets/rdf"}
    query_result = [
        {
            "schema": "shacl",
            "base": None,
            "model": "dcat:Dataset",
            "property": "dct:identifier",
            "type": "xsd:nonNegativeInteger",
            "title": Literal("Identifier", lang="en"),
            "description": Literal("Dataset identifier", lang="en"),
        },
        {
            "schema": "shacl",
            "base": None,
            "model": "dcat:Dataset",
            "property": None,
            "type": None,
            "title": Literal("Dataset", lang="en"),
            "description": Literal("Dataset", lang="en"),
        },
    ]
    schemas = _prepare_data(graph, query_result, dataset, lang="en")
    res = list(_get_schemas(graph, schemas.values(), dataset, schemas.keys()))
    assert res == [
        (
            1,
            {
                "type": "model",
                "name": "datasets/rdf/Dataset",
                "base": None,
                "properties": {
                    "identifier": {
                        "type": "integer",
                        "title": "Identifier",
                        "description": "Dataset identifier",
                        "uri": "dct:identifier",
                        "access": "open",
                    }
                },
                "uri": "dcat:Dataset",
                "access": "open",
                "description": "Dataset",
                "title": "Dataset",
                "external": {"dataset": "datasets/rdf"},
            },
        )
    ]


def test_rdf_with_duplicate_schema_for_model():
    graph = Graph()
    dataset = {"name": "datasets/rdf"}
    query_result = [
        {
            "schema": "shacl",
            "base": None,
            "model": "dcat:Dataset",
            "property": None,
            "type": None,
            "title": None,
            "description": Literal("Dataset shacl", lang="en"),
        },
        {
            "schema": "rdfs",
            "base": None,
            "model": "dcat:Dataset",
            "property": None,
            "type": None,
            "title": Literal("Dataset", lang="en"),
            "description": Literal("Dataset rdfs", lang="en"),
        },
    ]
    schemas = _prepare_data(graph, query_result, dataset, lang="en")
    res = list(_get_schemas(graph, schemas.values(), dataset, schemas.keys()))
    assert res == [
        (
            1,
            {
                "type": "model",
                "name": "datasets/rdf/Dataset",
                "base": None,
                "properties": {},
                "uri": "dcat:Dataset",
                "access": "open",
                "description": "Dataset rdfs\nDataset shacl",
                "title": "Dataset",
                "external": {"dataset": "datasets/rdf"},
            },
        )
    ]


def test_rdf_with_duplicate_schema_for_property():
    graph = Graph()
    dataset = {"name": "datasets/rdf"}
    query_result = [
        {
            "schema": "rdfs",
            "base": None,
            "model": "dcat:Dataset",
            "property": "dct:identifier",
            "type": "xsd:nonNegativeInteger",
            "title": Literal("Identifier", lang="en"),
            "description": Literal("Dataset identifier rdfs", lang="en"),
        },
        {
            "schema": "shacl",
            "base": None,
            "model": "dcat:Dataset",
            "property": "dct:identifier",
            "type": "xsd:string",
            "title": Literal("Identifier", lang="en"),
            "description": Literal("Dataset identifier shacl", lang="en"),
        },
        {
            "schema": "shacl",
            "base": None,
            "model": "dcat:Dataset",
            "property": None,
            "type": None,
            "title": Literal("Dataset", lang="en"),
            "description": Literal("Dataset", lang="en"),
        },
    ]
    schemas = _prepare_data(graph, query_result, dataset, lang="en")
    res = list(_get_schemas(graph, schemas.values(), dataset, schemas.keys()))
    assert res == [
        (
            1,
            {
                "type": "model",
                "name": "datasets/rdf/Dataset",
                "base": None,
                "properties": {
                    "identifier": {
                        "type": "string",
                        "title": "Identifier",
                        "description": "Dataset identifier rdfs\nDataset identifier shacl",
                        "uri": "dct:identifier",
                        "access": "open",
                    }
                },
                "uri": "dcat:Dataset",
                "access": "open",
                "description": "Dataset",
                "title": "Dataset",
                "external": {"dataset": "datasets/rdf"},
            },
        )
    ]


def test_rdf_with_missing_model():
    graph = Graph()
    dataset = {"name": "datasets/rdf"}
    query_result = [
        {
            "schema": "shacl",
            "base": None,
            "model": None,
            "property": "dct:identifier",
            "type": "xsd:nonNegativeInteger",
            "title": Literal("Identifier", lang="en"),
            "description": Literal("Dataset identifier", lang="en"),
        },
    ]
    schemas = _prepare_data(graph, query_result, dataset, lang="en")
    res = list(_get_schemas(graph, schemas.values(), dataset, schemas.keys()))
    assert res == [
        (
            1,
            {
                "type": "model",
                "name": "datasets/rdf/Resource",
                "base": None,
                "properties": {
                    "identifier": {
                        "type": "integer",
                        "title": "Identifier",
                        "description": "Dataset identifier",
                        "uri": "dct:identifier",
                        "access": "open",
                    }
                },
                "uri": "rdfs:Resource",
                "access": "open",
                "description": "",
                "title": "",
                "external": {"dataset": "datasets/rdf"},
            },
        )
    ]


def test_rdf_with_base():
    graph = Graph()
    dataset = {"name": "datasets/rdf"}
    query_result = [
        {
            "schema": "shacl",
            "base": "dcat:Base",
            "model": "dcat:Dataset",
            "property": None,
            "type": None,
            "title": Literal("Dataset", lang="en"),
            "description": Literal("Dataset", lang="en"),
        },
        {
            "schema": "shacl",
            "base": None,
            "model": "dcat:Base",
            "property": None,
            "type": None,
            "title": Literal("Base", lang="en"),
            "description": Literal("Base model", lang="en"),
        },
    ]
    schemas = _prepare_data(graph, query_result, dataset, lang="en")
    res = list(_get_schemas(graph, schemas.values(), dataset, schemas.keys()))
    assert res == [
        (
            1,
            {
                "type": "model",
                "name": "datasets/rdf/Base",
                "base": None,
                "properties": {},
                "uri": "dcat:Base",
                "access": "open",
                "description": "Base model",
                "title": "Base",
                "external": {"dataset": "datasets/rdf"},
            },
        ),
        (
            2,
            {
                "type": "model",
                "name": "datasets/rdf/Dataset",
                "base": {"name": "Base", "parent": "datasets/rdf/Base"},
                "properties": {},
                "uri": "dcat:Dataset",
                "access": "open",
                "description": "Dataset",
                "title": "Dataset",
                "external": {"dataset": "datasets/rdf"},
            },
        ),
    ]


def test_rdf_with_ref():
    graph = Graph()
    dataset = {"name": "datasets/rdf"}
    query_result = [
        {
            "schema": "shacl",
            "base": None,
            "model": "dcat:Dataset",
            "property": None,
            "type": None,
            "title": Literal("Dataset", lang="en"),
            "description": Literal("Dataset", lang="en"),
        },
        {
            "schema": "shacl",
            "base": None,
            "model": "dcat:Catalog",
            "property": None,
            "type": None,
            "title": Literal("Catalog", lang="en"),
            "description": Literal("Catalog", lang="en"),
        },
        {
            "schema": "shacl",
            "base": None,
            "model": "dcat:Dataset",
            "property": "dct:catalog",
            "type": "dcat:Catalog",
            "title": Literal("Catalog", lang="en"),
            "description": Literal("Dataset catalog", lang="en"),
        },
    ]
    schemas = _prepare_data(graph, query_result, dataset, lang="en")
    res = list(_get_schemas(graph, schemas.values(), dataset, schemas.keys()))
    assert res == [
        (
            1,
            {
                "type": "model",
                "name": "datasets/rdf/Dataset",
                "base": None,
                "properties": {
                    "catalog": {
                        "type": "ref",
                        "title": "Catalog",
                        "description": "Dataset catalog",
                        "uri": "dct:catalog",
                        "access": "open",
                        "model": "datasets/rdf/Catalog",
                    }
                },
                "uri": "dcat:Dataset",
                "access": "open",
                "description": "Dataset",
                "title": "Dataset",
                "external": {"dataset": "datasets/rdf"},
            },
        ),
        (
            2,
            {
                "type": "model",
                "name": "datasets/rdf/Catalog",
                "base": None,
                "properties": {},
                "uri": "dcat:Catalog",
                "access": "open",
                "description": "Catalog",
                "title": "Catalog",
                "external": {"dataset": "datasets/rdf"},
            },
        ),
    ]


def test_rdf_with_different_language():
    graph = Graph()
    dataset = {"name": "datasets/rdf"}
    query_result = [
        {
            "schema": "shacl",
            "base": None,
            "model": "dcat:Dataset",
            "property": "dct:identifier",
            "type": "xsd:nonNegativeInteger",
            "title": Literal("Identifier", lang="en"),
            "description": Literal("Dataset identifier", lang="en"),
        },
        {
            "schema": "shacl",
            "base": None,
            "model": "dcat:Dataset",
            "property": None,
            "type": None,
            "title": Literal("Dataset", lang="en"),
            "description": Literal("Dataset", lang="en"),
        },
    ]
    schemas = _prepare_data(graph, query_result, dataset, lang="lt")
    res = list(_get_schemas(graph, schemas.values(), dataset, schemas.keys()))
    assert res == [
        (
            1,
            {
                "type": "model",
                "name": "datasets/rdf/Dataset",
                "base": None,
                "properties": {
                    "identifier": {
                        "type": "integer",
                        "title": "",
                        "description": "",
                        "uri": "dct:identifier",
                        "access": "open",
                    }
                },
                "uri": "dcat:Dataset",
                "access": "open",
                "description": "",
                "title": "",
                "external": {"dataset": "datasets/rdf"},
            },
        )
    ]
