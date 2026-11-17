from __future__ import annotations

import enum
import itertools
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.collection import Collection
from rdflib.namespace import DCTERMS, OWL, RDF, RDFS, XSD
from rdflib.query import Result
from rdflib.term import Node
from rdflib.util import guess_format

from spinta.manifests.rdf.components import RdfManifest
from spinta.manifests.tabular.helpers import get_relative_model_name
from spinta.utils.naming import to_dataset_name, to_model_name

log = logging.getLogger(__name__)

SH = Namespace("http://www.w3.org/ns/shacl#")
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")

# Shape properties that only describe how a violation is reported. A property
# shape that has nothing else is not a property definition, for example
# deprecation warnings, so it is ignored.
_SHAPE_REPORTING_PREDICATES = {SH.path, SH.severity, SH.message}

# Maximum depth when following `sh:node` / `sh:or` references between shapes.
_MAX_SHAPE_DEPTH = 5

# Maximum depth when following `owl:imports` from the given files.
_MAX_IMPORT_DEPTH = 5


class Schema(enum.IntEnum):
    undefined = 0
    rdfs = 1
    owl = 2
    shacl = 3


def read_rdf_manifest(
    manifests: RdfManifest | list[RdfManifest],
    dataset_name: str | None = None,
    lang: str = "en",
    *,
    fetch_remote: bool = False,
) -> Iterator[tuple[int, dict]]:
    """Read one or more RDF files as manifest datasets.

    All files are parsed into one graph, because SHACL shapes, RDFS
    definitions and class ranges are often spread over several files.

    Models are grouped into datasets by the prefix of their URI, for example
    `dcat:Dataset` goes to the `dcat` dataset. Models without a declared prefix
    go to the main dataset, named by `get_dataset_name`.

    Web IRIs are dereferenced only when `fetch_remote` is true: `owl:imports`,
    concept schemes and titles or descriptions missing in the files. Without it
    the result depends only on the given files.
    """
    if isinstance(manifests, RdfManifest):
        manifests = [manifests]

    graph, sources = read_rdf_graph(manifests, fetch_remote=fetch_remote)

    main = _dataset_row(graph, get_dataset_name(manifests, dataset_name, sources, lang), dataset_name)
    shacl = list(_shacl_rows(graph, lang))
    rows = itertools.chain(
        shacl,
        _ontology_rows(graph, sources, shacl),
    )
    models = _prepare_data(graph, rows, main, lang)

    datasets = {main["name"]: main}
    for model in models.values():
        name = _model_dataset(model["name"])
        if name not in datasets:
            datasets[name] = _dataset_row(graph, name)

    # The main dataset is left out when all models have a prefix, so that no
    # empty dataset is written. The main dataset comes first.
    model_datasets = {_model_dataset(model["name"]) for model in models.values()}
    if model_datasets and main["name"] not in model_datasets:
        del datasets[main["name"]]
    ordered = sorted(datasets.values(), key=lambda d: (d["name"] != main["name"], d["name"]))

    # Models of one dataset must be consecutive, the tabular writer starts a
    # new dataset section whenever the dataset of the next model changes.
    position = {dataset["name"]: i for i, dataset in enumerate(ordered)}
    model_rows = sorted(models.values(), key=lambda model: position[_model_dataset(model["name"])])
    result = list(
        _get_schemas(
            graph,
            [*ordered, *model_rows],
            main,
            list(models.keys()),
            lang,
            datasets=datasets,
            fetch_remote=fetch_remote,
        )
    )
    for dataset in ordered:
        dataset_models = [
            row for _, row in result if row.get("type") == "model" and _model_dataset(row["name"]) == dataset["name"]
        ]
        _keep_used_prefixes(dataset, dataset_models)
    yield from result


def _dataset_row(graph: Graph, name: str, given_name: str | None = None) -> dict:
    """Return a dataset with all prefixes of the graph, unused ones are removed later."""
    prefixes = {}
    for i, (prefix, uri) in enumerate(graph.namespaces()):
        if not prefix:
            continue
        prefixes[prefix] = {
            "type": "prefix",
            "eid": i,
            "name": prefix,
            "uri": str(uri),
        }
    return {
        "type": "dataset",
        "name": name,
        "prefixes": prefixes,
        "given_name": given_name,
    }


def _keep_used_prefixes(dataset: dict, models: list[dict]) -> None:
    """Keep only prefixes that the dataset uses in its models, properties and enums.

    Imported ontologies declare many prefixes, which are not needed by the
    manifest, and every dataset would repeat them all.
    """
    uris = []
    for row in models:
        uris.append(row.get("uri"))
        uris.extend(prop.get("uri") for prop in row["properties"].values())
    for items in dataset.get("enums", {}).values():
        uris.extend(item.get("uri") for item in items.values())
    used: set[str] = set()
    for uri in uris:
        if uri and ":" in uri and "://" not in uri:
            used.add(uri.split(":", 1)[0])

    prefixes = dataset["prefixes"]
    for name in list(prefixes):
        if name not in used:
            del prefixes[name]
    for eid, prefix in enumerate(prefixes.values()):
        prefix["eid"] = eid


def read_rdf_graph(manifests: list[RdfManifest], *, fetch_remote: bool = False) -> tuple[Graph, Graph]:
    """Parse all files and the ontologies they import into one graph.

    Returns the merged graph and the graph of the given files only. Imports
    are followed transitively, every document is loaded once. Web imports are
    followed only when `fetch_remote` is true.

    Each file is parsed separately first, because files may declare the same
    empty prefix for different namespaces, which would otherwise be renamed
    to generated prefixes.
    """
    merged = Graph(bind_namespaces="none")
    sources = Graph(bind_namespaces="none")
    loaded: set[str] = set()
    pending: list[tuple[Graph, int]] = []

    for manifest in manifests:
        source = Graph(bind_namespaces="none")
        source.parse(manifest.path, format=manifest.format)
        loaded.add(_document_key(manifest.path))
        _add_graph(merged, source)
        _add_graph(sources, source)
        pending.append((source, 0))

    while pending:
        source, depth = pending.pop(0)
        for iri in sorted({str(o) for o in source.objects(None, OWL.imports)}):
            if depth >= _MAX_IMPORT_DEPTH:
                log.warning("Import %s is nested too deep, it is not loaded.", iri)
                continue
            key = _document_key(iri)
            if key in loaded:
                continue
            loaded.add(key)
            imported = _load_import(iri, fetch_remote=fetch_remote)
            if imported is None:
                continue
            _add_graph(merged, imported)
            pending.append((imported, depth + 1))
    return merged, sources


def _add_graph(target: Graph, source: Graph) -> None:
    for prefix, namespace in source.namespaces():
        if prefix:
            target.bind(prefix, namespace, override=False)
    for triple in source:
        target.add(triple)


def _document_key(location: str) -> str:
    """Return a key identifying a document, a local path and its `file:` IRI are the same document."""
    location = str(location)
    if location.startswith("file:"):
        location = urllib.request.url2pathname(urllib.parse.urlsplit(location).path)
    if _is_web_iri(location):
        return location
    return Path(location).resolve().as_uri()


def _load_import(iri: str, *, fetch_remote: bool = False) -> Graph | None:
    """Load an imported ontology, `None` if it is not available as RDF.

    Imports are looked up at the IRI they are given with, `file:` IRIs are read
    from disk, web IRIs are fetched only when `fetch_remote` is true.
    """
    if iri.startswith("file:"):
        path = urllib.request.url2pathname(urllib.parse.urlsplit(iri).path)
        graph = Graph(bind_namespaces="none")
        try:
            graph.parse(path, format=guess_format(path) or "turtle")
        except Exception as e:
            # Any parse error means the import is not usable.
            log.warning("Could not load import %s: %s", iri, e)
            return None
        return graph

    if not _is_web_iri(iri):
        log.warning("Unsupported import %s, it is not loaded.", iri)
        return None

    if not fetch_remote:
        log.warning("Import %s is not fetched, remote fetching is disabled (rdf_fetch_remote).", iri)
        return None

    try:
        imported = _fetch_graph(iri)
    except _Unavailable as e:
        log.warning("Could not fetch import %s: %s", iri, e)
        return None
    if imported is None:
        log.warning("Import %s is not available as RDF, it is not loaded.", iri)
    return imported


def get_dataset_name(
    manifests: list[RdfManifest],
    dataset_name: str | None = None,
    graph: Graph | None = None,
    lang: str = "en",
) -> str:
    """Return dataset name for RDF files.

    The name is taken, in order of preference, from:

    - the name given explicitly, `spinta copy --dataset`,
    - the title of an `owl:Ontology` in the sources,
    - the name of the directory containing several files,
    - the file name of a single file.
    """
    if dataset_name:
        return dataset_name

    if graph is not None and (title := _ontology_title(graph, lang)):
        if name := to_dataset_name(title):
            return name

    paths = [m.path for m in manifests if m.path]
    if len(paths) > 1:
        directory = os.path.basename(os.path.commonpath([os.path.abspath(p) for p in paths]).rstrip(os.sep))
        if name := to_dataset_name(directory):
            return name

    if paths:
        name = os.path.basename(paths[0]).rsplit(".", 1)[0]
        return to_dataset_name(name) or "rdf"
    return "rdf"


def _ontology_title(graph: Graph, lang: str) -> str:
    """Return title of the first `owl:Ontology` that has one."""
    for ontology in sorted(graph.subjects(RDF.type, OWL.Ontology), key=str):
        if title := _pick_text(graph, ontology, [DCTERMS.title, RDFS.label], lang):
            return title
    return ""


def _prepare_data(graph: Graph, query: Iterable[dict] | Result, dataset: dict, lang: str = "en") -> dict:
    data: dict = {}
    for row in query:
        dim = _get_dimension(row["model"], row["property"])
        if dim == "model":
            _read_model(graph, dataset, data, row, lang)
        elif dim == "property":
            _read_property(graph, dataset, data, row, lang)
    return data


def _get_schemas(
    graph: Graph,
    schemas: list[dict],
    dataset: dict,
    models: list,
    lang: str = "en",
    datasets: dict[str, dict] | None = None,
    *,
    fetch_remote: bool = False,
):
    """Finalize models and their properties.

    Enums of a property are added to the dataset of its model, `datasets` maps
    dataset names to dataset rows. Without `datasets` all models belong to
    `dataset`. Definitions are looked up remotely only when `fetch_remote` is true.
    """
    definitions = _Definitions(graph, lang, fetch_remote=fetch_remote)
    definitions.prefetch(_definition_iris(graph, schemas))
    enums: _EnumCache = {}

    for row in schemas:
        if "schema" in row:
            row.pop("schema")

        if row.get("type") == "model":
            _fill_definitions(graph, definitions, row)
            for prop in row["properties"].values():
                _fill_definitions(graph, definitions, prop)

        if "description" in row:
            row["description"] = "\n".join(sorted(set(row["description"])))

        if "properties" in row:
            target = datasets[_model_dataset(row["name"])] if datasets else dataset
            for name, prop in list(row["properties"].items()):
                row["properties"][name] = _finalize_property(graph, target, models, name, prop, definitions, enums)

    # Enums are added to the dataset while its properties are finalized, so
    # rows are yielded only after all of them are processed.
    for i, row in enumerate(schemas, 1):
        yield i, row


def _definition_iris(graph: Graph, schemas: Iterable[dict]) -> Iterator[str]:
    """Yield IRIs of all models and properties, whose definitions may be looked up."""
    for row in schemas:
        if row.get("type") == "model":
            yield _iri_of(graph, row.get("uri"))
            for prop in row["properties"].values():
                yield _iri_of(graph, prop.get("uri"))


def _fill_definitions(graph: Graph, definitions: _Definitions, row: dict) -> None:
    """Fill empty title and description of a model or a property from its definition."""
    iri = _iri_of(graph, row.get("uri"))
    if not iri:
        return
    title, description = definitions.get(iri)
    if not row.get("title"):
        row["title"] = title
    if not row.get("description") and description:
        row["description"] = [description]


def _finalize_property(
    graph: Graph,
    dataset: dict,
    models: list,
    name: str,
    prop: dict,
    definitions: _Definitions,
    enums: _EnumCache,
) -> dict:
    prop.pop("schema", None)
    required = prop.pop("required", None)
    array = prop.pop("array", None)
    schemes = prop.pop("schemes", None) or []

    prop["description"] = "\n".join(sorted(set(prop["description"])))
    prop["type"], ref_model = _to_type_name(graph, dataset, models, prop["type"])
    if ref_model:
        prop["model"] = ref_model
    if schemes and (enum := _load_enum(graph, dataset, name, schemes, definitions, enums)):
        prop["enum"] = enum
    if required:
        prop["required"] = True

    if array:
        # Multi-valued property, represented as an array with a single item
        # type. The referenced model and the enum belong to the item only: an
        # array with `model` is treated as a many-to-many intermediate table.
        item = {k: v for k, v in prop.items() if k != "required"}
        item["given_name"] = f"{name}[]"
        outer = {k: v for k, v in prop.items() if k not in ("model", "type", "enum")}
        return {
            **outer,
            "type": "array",
            "explicitly_given": False,
            "items": item,
        }
    return prop


def _read_model(graph: Graph, dataset: dict, data: dict, row: dict, lang: str = "en") -> None:
    schema = _load_schema(str(row["schema"]))
    model, uri = _to_model_name(graph, dataset, row["model"])
    base, _ = _to_model_name(graph, dataset, row["base"])
    title = _get_value_for_language(row["title"], lang)
    properties = {}
    descriptions = []

    if description := _get_value_for_language(row["description"], lang):
        descriptions.append(description)

    if model in data:
        existing_model = data[model]
        properties = existing_model["properties"]
        title = title or existing_model["title"]
        descriptions.extend(existing_model["description"])
        if not base and existing_model["base"]:
            base = existing_model["base"]["parent"]

        if data[model]["schema"] > schema:
            schema = existing_model["schema"]
            base = existing_model["base"]["parent"] if existing_model["base"] else base
            title = existing_model["title"] or title

    if base and base not in data:
        # Add temporary model for base
        data.update({base: _stub_model(base, "")})

    data.update(
        {
            model: {
                "type": "model",
                "schema": schema,
                "name": model,
                "base": {
                    "name": base.split("/")[-1],
                    "parent": base,
                }
                if base
                else None,
                "title": title,
                "description": descriptions,
                "properties": properties,
                "uri": uri,
                "access": "open",
                "external": {
                    "dataset": _model_dataset(model),
                },
            }
        }
    )


def _stub_model(name: str, uri: str) -> dict:
    return {
        "type": "model",
        "schema": Schema.undefined,
        "name": name,
        "base": None,
        "properties": {},
        "uri": uri,
        "access": "open",
        "description": [],
        "title": "",
        "external": {
            "dataset": _model_dataset(name),
        },
    }


def _ensure_model(graph: Graph, dataset: dict, data: dict, name: Any) -> str:
    """Make sure a model for a class exists, referenced by a property."""
    model, model_uri = _to_model_name(graph, dataset, name)
    if model not in data:
        data[model] = _stub_model(model, model_uri)
    return model


def _read_property(graph: Graph, dataset: dict, data: dict, row: dict, lang: str = "en") -> None:
    schema = _load_schema(str(row["schema"]))
    model, model_uri = _to_model_name(graph, dataset, row["model"] or "rdfs:Resource")
    prop, prop_uri = _to_property_name(graph, row["property"])
    type = _type_iri(row["type"])
    title = _get_value_for_language(row["title"], lang)
    descriptions = []
    required = bool(row.get("required"))
    array = row.get("array")
    schemes = list(row.get("schemes") or [])

    if description := _get_value_for_language(row["description"], lang):
        descriptions.append(description)

    if type and _is_model_type(type):
        # Referenced class is not necessarily defined in the sources, but a
        # `ref` needs a model to point to, so add a placeholder for it.
        _ensure_model(graph, dataset, data, type)

    if model not in data:
        # Add temporary model for property
        data.update({model: _stub_model(model, model_uri)})

    model = data[model]

    if prop in model["properties"]:
        existing_prop = model["properties"][prop]
        if existing_prop["uri"] and prop_uri and existing_prop["uri"] != prop_uri:
            log.warning(
                "Properties %r and %r are both named %r in model %r, they are merged into one property.",
                existing_prop["uri"],
                prop_uri,
                prop,
                model["name"],
            )
        title = title or existing_prop["title"]
        type = type or existing_prop["type"]
        descriptions.extend(existing_prop["description"])
        required = required or existing_prop.get("required", False)
        array = _merge_flag_and(array, existing_prop.get("array"))
        schemes = sorted({*schemes, *existing_prop.get("schemes", [])}, key=str)

        if model["properties"][prop]["schema"] > schema:
            schema = existing_prop["schema"]
            type = existing_prop["type"] or type
            title = existing_prop["title"] or title

    model["properties"].update(
        {
            prop: {
                "schema": schema,
                "type": type,
                "title": title,
                "description": descriptions,
                "uri": prop_uri,
                "access": "open",
                "required": required,
                "array": array,
                "schemes": schemes,
            }
        }
    )


def _merge_flag_and(a: bool | None, b: bool | None) -> bool | None:
    """Combine two cardinality flags, `None` means that a flag is not known."""
    if a is None:
        return b
    if b is None:
        return a
    return a and b


def _get_dimension(model: str, prop: str) -> str:
    if model and not prop:
        return "model"
    elif prop:
        return "property"
    return ""


def _load_schema(schema: str) -> Schema:
    if schema == "rdfs":
        return Schema.rdfs
    elif schema == "owl":
        return Schema.owl
    elif schema == "shacl":
        return Schema.shacl
    else:
        return Schema.undefined


def _to_model_name(graph: Graph, dataset: dict, name: Any) -> tuple[str, str]:
    """Return full model name, `dataset/Model`, and its URI.

    A class with a prefix belongs to the dataset named by the prefix, other
    classes belong to `dataset`.
    """
    uri = ""
    name = _parse_uri(graph, name)
    if name:
        if ":" in name:
            uri = name
            name = to_model_name(_local_name(name))
            name = f"{_dataset_of_uri(graph, uri, dataset['name'])}/{name}"
        else:
            name = get_relative_model_name(dataset, name)
    return name, uri


def _model_dataset(model: str) -> str:
    """Return dataset name of a full model name, `dcat/Dataset` -> `dcat`."""
    return model.rsplit("/", 1)[0]


def _dataset_of_uri(graph: Graph, uri: str, default: str) -> str:
    """Return dataset name for a class URI, `dcat:Dataset` -> `dcat`.

    Classes whose prefix is not declared, for example full IRIs, go to `default`.
    """
    prefix, sep, _ = uri.partition(":")
    if not sep or "://" in uri or not _PREFIX_NAME_RE.match(prefix):
        return default
    if prefix not in {name for name, _ in graph.namespaces()}:
        return default
    return to_dataset_name(prefix) or default


def _to_property_name(graph: Graph, name: Any) -> tuple[str, str]:
    name = _parse_uri(graph, name)
    uri = ""
    if name and ":" in name:
        uri = name
        name = to_snake_case(_local_name(name))
    return name, uri


def to_snake_case(name: str) -> str:
    """Convert a URI local name to a manifest name, `accessURL` -> `access_url`.

    Unlike `spinta.utils.naming.to_property_name`, this keeps acronyms
    together.
    """
    name = _ACRONYM_RE.sub(r"\1_\2", name)
    name = _CAMEL_RE.sub(r"\1_\2", name)
    return "_".join(p for p in _NON_ALNUM_RE.split(name) if p).lower()


_ACRONYM_RE = re.compile(r"([A-Z]+)([A-Z][a-z])")
_CAMEL_RE = re.compile(r"([a-z0-9])([A-Z])")
_NON_ALNUM_RE = re.compile(r"[^A-Za-z0-9]+")
_NCNAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.\-]*$")


def _local_name(name: str) -> str:
    """Return local part of a qname (`dct:Location`) or of an IRI."""
    if "#" in name:
        return name.rsplit("#", 1)[1]
    if "/" in name:
        return name.rsplit("/", 1)[1]
    if ":" in name:
        return name.split(":", 1)[1]
    return name


TYPES = {
    "xsd:hexBinary": "binary",
    "xsd:base64Binary": "binary",
    "xsd:duration": "string",
    "xsd:decimal": "number",
    "xsd:double": "number",
    "xsd:float": "number",
    "xsd:boolean": "boolean",
    "xsd:anyURI": "uri",
    "xsd:dateTime": "datetime",
    "xsd:integer": "integer",
    "xsd:int": "integer",
    "xsd:long": "integer",
    "xsd:short": "integer",
    "xsd:byte": "integer",
    "xsd:nonNegativeInteger": "integer",
    "xsd:positiveInteger": "integer",
    "xsd:nonPositiveInteger": "integer",
    "xsd:negativeInteger": "integer",
    "xsd:unsignedInt": "integer",
    "xsd:unsignedLong": "integer",
    "xsd:date": "date",
    "xsd:dateTimeStamp": "datetime",
    "xsd:gYear": "string",
    "xsd:gYearMonth": "string",
    "xsd:string": "string",
    "dct:MediaType": "file",
    "rdfs:Literal": "string",
    "rdf:langString": "string",
    "vcard:Kind": "string",
    "foaf:Document": "file",
    "dcterms:Location": "string",
    "owl:Thing": "string",
}

# Namespaces of well known types, used to compare types given as full IRIs
# with the keys of `TYPES`.
_TYPE_NAMESPACES = {
    "xsd": str(XSD),
    "rdf": str(RDF),
    "rdfs": str(RDFS),
    "owl": str(OWL),
    "dct": "http://purl.org/dc/terms/",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "vcard": "http://www.w3.org/2006/vcard/ns#",
}


def _canonical_type(name: str) -> str:
    name = str(name)
    for prefix, namespace in _TYPE_NAMESPACES.items():
        if name.startswith(namespace):
            return f"{prefix}:{name[len(namespace) :]}"
    return name


def _is_datatype(name: str) -> bool:
    return _canonical_type(name).startswith("xsd:")


def _is_model_type(name: str) -> bool:
    """Return True if type name refers to a class, which is not a known simple type."""
    key = _canonical_type(name)
    return key not in TYPES and not _is_datatype(key)


def _type_iri(name: Any) -> str | None:
    if not name:
        return None
    return str(name)


def _to_type_name(
    graph: Graph,
    dataset: dict,
    models: list,
    name: str,
) -> tuple[str, str]:
    ref_model = None
    type = "string"
    if name:
        key = _canonical_type(name)
        if key in TYPES:
            type = TYPES[key]
        else:
            model_name, _ = _to_model_name(graph, dataset, name)
            if model_name in models:
                type = "ref"
                ref_model = model_name
    return type, ref_model


def _parse_uri(graph: Graph, name: Any) -> str:
    if isinstance(name, URIRef):
        return _qname(graph, str(name))
    elif name and "://" in str(name):
        return _qname(graph, str(name))
    elif name:
        return str(name)
    else:
        return name


def _qname(graph: Graph, iri: str) -> str:
    """Return `prefix:local` for an IRI, if its namespace is bound, otherwise the IRI itself."""
    best: tuple[str, str] | None = None
    for prefix, namespace in graph.namespaces():
        namespace = str(namespace)
        if not prefix or not iri.startswith(namespace):
            continue
        local = iri[len(namespace) :]
        if not _NCNAME_RE.match(local):
            continue
        if best is None or len(namespace) > len(best[1]):
            best = (prefix, namespace)
    if best is None:
        return iri
    return f"{best[0]}:{iri[len(best[1]) :]}"


def _get_value_for_language(value: Any, lang: str) -> str:
    if not value:
        return ""
    if isinstance(value, Literal):
        if value.language == lang:
            return str(value)
        return ""
    # Already selected value, for example by `_shacl_rows`.
    return str(value)


# SHACL extraction.


@dataclass
class _PropertyConstraints:
    classes: set[URIRef] = field(default_factory=set)
    datatypes: set[URIRef] = field(default_factory=set)
    schemes: set[URIRef] = field(default_factory=set)
    required: bool = False
    array: bool = True
    titles: list[str] = field(default_factory=list)
    descriptions: list[str] = field(default_factory=list)


@dataclass
class _ClassConstraints:
    # Titles and descriptions are tagged with whether the shape they come from
    # defines properties. Shapes that only report violations, for example
    # deprecation warnings, usually have their own labels, which should not
    # name the class.
    titles: list[tuple[bool, str]] = field(default_factory=list)
    descriptions: list[tuple[bool, str]] = field(default_factory=list)
    properties: dict[URIRef, _PropertyConstraints] = field(default_factory=dict)


def _shacl_rows(graph: Graph, lang: str = "en") -> Iterator[dict]:
    """Yield model and property rows for SHACL node shapes that target classes.

    All shapes targeting the same class are merged, because DCAT-AP spreads
    constraints of a single class over several files.
    """
    classes: dict[URIRef, _ClassConstraints] = {}

    shapes = sorted(set(graph.subjects(SH.targetClass, None)), key=str)
    for shape in shapes:
        if _is_deactivated(graph, shape):
            continue
        targets = sorted((t for t in graph.objects(shape, SH.targetClass) if isinstance(t, URIRef)), key=str)
        defines = _has_definitions(graph, shape)
        for target in targets:
            cls = classes.setdefault(target, _ClassConstraints())
            if title := _pick_text(graph, shape, [RDFS.label, SH.name], lang):
                cls.titles.append((defines, title))
            if description := _pick_text(graph, shape, [RDFS.comment, SH.description], lang):
                cls.descriptions.append((defines, description))
            for prop_shape in graph.objects(shape, SH.property):
                _collect_property(graph, cls, prop_shape, lang)

    for cls_iri in sorted(classes, key=str):
        cls = classes[cls_iri]
        yield {
            "schema": "shacl",
            "base": None,
            "model": cls_iri,
            "property": None,
            "type": None,
            "title": _pick_title(cls.titles),
            "description": _join(_defining_texts(cls.descriptions)),
            "required": None,
            "array": None,
        }

        for path, constraints in sorted(cls.properties.items(), key=lambda item: str(item[0])):
            schemes = _enum_schemes(constraints)
            yield {
                "schema": "shacl",
                "base": None,
                "model": cls_iri,
                "property": path,
                "type": None if schemes else _resolve_type(graph, constraints.classes, constraints.datatypes),
                "title": _first_text(constraints.titles),
                "description": _join(constraints.descriptions),
                "required": constraints.required,
                "array": constraints.array,
                "schemes": schemes,
            }


def _collect_property(graph: Graph, cls: _ClassConstraints, prop_shape: Node, lang: str) -> None:
    if _is_deactivated(graph, prop_shape):
        # SHACL disables deactivated shapes, so their paths and constraints
        # must not become manifest properties.
        return

    path = graph.value(prop_shape, SH.path)
    if not isinstance(path, URIRef):
        # Inverse, sequence and alternative paths can't be expressed as a
        # manifest property.
        log.debug("Skipping property shape %s with complex path %s.", prop_shape, path)
        return

    if _is_reporting_only(graph, prop_shape):
        return

    constraints = cls.properties.setdefault(path, _PropertyConstraints())
    classes, datatypes, schemes = _value_types(graph, prop_shape)
    constraints.classes |= classes
    constraints.datatypes |= datatypes
    constraints.schemes |= schemes

    min_count = graph.value(prop_shape, SH.minCount)
    severity = graph.value(prop_shape, SH.severity)
    # SHACL violations are errors by default, `sh:Warning` and `sh:Info`
    # only recommend a value.
    if min_count is not None and int(min_count) >= 1 and severity not in (SH.Warning, SH.Info):
        constraints.required = True

    max_count = graph.value(prop_shape, SH.maxCount)
    constraints.array = constraints.array and (max_count is None or int(max_count) > 1)

    if title := _pick_text(graph, prop_shape, [SH.name, RDFS.label], lang):
        constraints.titles.append(title)
    if description := _pick_text(graph, prop_shape, [SH.description, RDFS.comment], lang):
        constraints.descriptions.append(description)


def _is_deactivated(graph: Graph, shape: Node) -> bool:
    return any(str(value).lower() == "true" for value in graph.objects(shape, SH.deactivated))


def _is_reporting_only(graph: Graph, shape: Node) -> bool:
    predicates = {p for p, _ in graph.predicate_objects(shape)}
    return predicates <= _SHAPE_REPORTING_PREDICATES | {RDF.type}


def _has_definitions(graph: Graph, shape: Node) -> bool:
    """Return True if a node shape defines at least one property."""
    return any(
        not _is_deactivated(graph, ps) and not _is_reporting_only(graph, ps) for ps in graph.objects(shape, SH.property)
    )


def _defining_texts(texts: list[tuple[bool, str]]) -> list[str]:
    """Return texts from shapes that define properties, or all texts if there are none."""
    defining = [text for defines, text in texts if defines]
    return defining or [text for _, text in texts]


def _pick_title(titles: list[tuple[bool, str]]) -> str:
    """Return the most common title, ties are resolved by the first occurrence.

    DCAT-AP shapes are split over several files and some labels are copied
    between shapes, so the most common title is the most reliable one.
    """
    candidates = _defining_texts(titles)
    candidates = [c for c in candidates if c]
    if not candidates:
        return ""
    return Counter(candidates).most_common(1)[0][0]


def _value_types(
    graph: Graph,
    shape: Node,
    depth: int = 0,
    seen: set | None = None,
) -> tuple[set[URIRef], set[URIRef], set[URIRef]]:
    """Collect classes, datatypes and concept schemes that constrain values of a shape.

    Follows `sh:node`, `sh:shape` (used in DCAT-AP, although it is not a
    SHACL predicate) and `sh:or`/`sh:and`/`sh:xone` lists.

    A concept scheme is given by a `skos:inScheme` constraint with `sh:hasValue`,
    which is how DCAT-AP restricts values to a controlled vocabulary.
    """
    classes: set[URIRef] = set()
    datatypes: set[URIRef] = set()
    schemes: set[URIRef] = set()
    seen = set() if seen is None else seen
    if shape in seen or depth > _MAX_SHAPE_DEPTH:
        return classes, datatypes, schemes
    seen.add(shape)

    for dt in graph.objects(shape, SH.datatype):
        if isinstance(dt, URIRef):
            datatypes.add(dt)
    for cls in graph.objects(shape, SH["class"]):
        if isinstance(cls, URIRef):
            classes.add(cls)
    for prop in graph.objects(shape, SH.property):
        if graph.value(prop, SH.path) == SKOS.inScheme:
            schemes.update(v for v in graph.objects(prop, SH.hasValue) if isinstance(v, URIRef))

    nested: list[Node] = []
    for predicate in (SH.node, SH.shape):
        nested.extend(graph.objects(shape, predicate))
    for predicate in (SH["or"], SH["and"], SH.xone):
        for head in graph.objects(shape, predicate):
            try:
                nested.extend(Collection(graph, head))
            except Exception:
                # Malformed list in the source.
                log.debug("Malformed SHACL list %s.", head)

    for node in nested:
        if not isinstance(node, URIRef | BNode):
            continue
        targets = [t for t in graph.objects(node, SH.targetClass) if isinstance(t, URIRef)]
        if targets:
            # Values must conform to a shape that targets a class, so this is
            # a reference to that class.
            classes.update(targets)
        else:
            nested_classes, nested_datatypes, nested_schemes = _value_types(graph, node, depth + 1, seen)
            classes |= nested_classes
            datatypes |= nested_datatypes
            schemes |= nested_schemes

    return classes, datatypes, schemes


def _resolve_type(graph: Graph, classes: set[URIRef], datatypes: set[URIRef]) -> URIRef | None:
    """Pick a single type for a property from its collected constraints.

    Returns `None` (string) when constraints can't be expressed as a single type.
    """
    if len(classes) == 1:
        return next(iter(classes))
    if classes:
        # A union of classes is a `ref` only to a class that all of them are
        # subclasses of, for example `dcat:Resource`.
        return _common_superclass(graph, classes)
    if not datatypes:
        return None

    mapped = {TYPES.get(_canonical_type(dt), "string") for dt in datatypes}
    if len(mapped) == 1:
        return sorted(datatypes, key=str)[0]
    if mapped <= {"date", "datetime"}:
        return XSD.dateTime
    return XSD.string


def _enum_schemes(constraints: _PropertyConstraints) -> list[URIRef]:
    """Return concept schemes of a property whose values are controlled vocabulary concepts.

    A property is an enum only when its values are `skos:Concept`s from a
    concept scheme. Concepts without a scheme keep a `ref` to `Concept`.
    """
    if SKOS.Concept not in constraints.classes:
        return []
    return sorted(constraints.schemes, key=str)


# Enums already added to datasets, keyed by dataset name and the set of concept
# scheme URIs they were loaded from.
_EnumCache = dict[tuple[str, frozenset[str]], str]


def _load_enum(
    graph: Graph,
    dataset: dict,
    prop_name: str,
    schemes: list[URIRef],
    definitions: _Definitions,
    enums: _EnumCache,
) -> str | None:
    """Add enum with all concepts of the schemes to the dataset, return its name.

    Enums are dataset level, so properties that use the same set of schemes
    share one enum. Schemes with the same local name are different vocabularies,
    they get separate enums, see `_enum_name`.
    """
    key = (dataset["name"], frozenset(str(scheme) for scheme in schemes))
    if key in enums:
        return enums[key]

    concepts = [concept for scheme in schemes for concept in _scheme_concepts(graph, scheme, definitions)]
    definitions.prefetch(str(concept) for concept in concepts)

    items: dict[str, dict] = {}
    iris: dict[str, str] = {}
    for concept in concepts:
        iri = str(concept)
        source = _local_name(iri)
        if source in items:
            if iris[source] != iri:
                log.warning(
                    "Concepts %r and %r have the same code %r, only the first one is used.",
                    iris[source],
                    iri,
                    source,
                )
            continue
        title, description = definitions.get(iri)
        iris[source] = iri
        items[source] = {
            "source": source,
            "prepare": source,
            "title": title,
            "description": description,
            "uri": _concept_uri(dataset, concept),
        }
    if not items:
        log.warning(
            "No concepts found for %s, property %r is not an enum.",
            ", ".join(str(s) for s in schemes),
            prop_name,
        )
        return None

    enum_name = _enum_name(dataset, schemes)
    dataset.setdefault("enums", {})[enum_name] = items
    enums[key] = enum_name
    return enum_name


def _enum_name(dataset: dict, schemes: list[URIRef]) -> str:
    """Return a name for a new enum of the schemes, which is not used by another enum of the dataset.

    The name is made of local names of the schemes, `https://a.example/status`
    -> `status`. When that name is already used, for example by
    `https://b.example/status`, the authority is added, `a_example_status`. If
    that is used too, a number is added, `status_2`.
    """
    used = set(dataset.get("enums", {}))
    name = "_".join(sorted({_scheme_name(scheme) for scheme in schemes}))
    if name in used:
        authority = "_".join(sorted({to_snake_case(urllib.parse.urlsplit(str(s)).netloc) for s in schemes} - {""}))
        if authority:
            name = f"{authority}_{name}"
    candidate, n = name, 1
    while candidate in used:
        n += 1
        candidate = f"{name}_{n}"
    return candidate


def _scheme_name(scheme: URIRef) -> str:
    """Return enum name for a concept scheme, `.../authority/dataset-type` -> `dataset_type`."""
    parts = [p for p in re.split(r"[/#]", str(scheme)) if p]
    if len(parts) > 1 and re.fullmatch(r"[\d.]+", parts[-1]):
        # Version, for example `http://purl.org/adms/status/1.0`.
        parts = parts[:-1]
    return to_snake_case(parts[-1]) if parts else ""


_PREFIX_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")

# Predicates used for titles and descriptions, in order of preference. SKOS
# and RDFS definitions are used for concepts, classes and properties, SHACL
# names and descriptions for the shapes that define them.
_TITLE_PREDICATES = [SKOS.prefLabel, RDFS.label, SH.name]
_DESCRIPTION_PREDICATES = [SKOS.definition, RDFS.comment, SH.description]

# Timeouts and retries used when definitions are looked up from their IRIs.
_FETCH_TIMEOUT = 1
_FETCH_ATTEMPTS = 3
# Few parallel requests, the vocabulary servers answer with 503 when overloaded.
_LOOKUP_WORKERS = 2
# After this many unavailable IRIs of a host, its IRIs are not looked up anymore.
_HOST_FAILURE_LIMIT = 5
_RDF_ACCEPT = "text/turtle, application/rdf+xml;q=0.9, text/n3;q=0.8"
# Some vocabulary servers (Cloudflare) reject the default `Python-urllib` agent.
_USER_AGENT = "spinta (+https://github.com/atviriduomenys/spinta)"
_XML_MEDIA_TYPES = {"application/rdf+xml", "text/xml", "application/xml"}
_TURTLE_MEDIA_TYPES = {"text/turtle", "application/x-turtle", "text/n3"}


def _scheme_concepts(graph: Graph, scheme: URIRef, definitions: _Definitions) -> list[URIRef]:
    """Return concepts of a concept scheme.

    Concepts are taken from the sources when they define the scheme, otherwise
    the scheme is fetched from its IRI, because controlled vocabularies are
    published separately from the shapes that refer to them. The fetched
    scheme is also a source of definitions of its concepts.
    """
    concepts = _concepts_in_scheme(graph, scheme)
    if not concepts and definitions.fetch_remote:
        remote = _fetch_vocabulary(scheme)
        if remote is not None:
            definitions.add_source(remote)
            concepts = _concepts_in_scheme(remote, scheme)
    return sorted(concepts, key=str)


def _concepts_in_scheme(graph: Graph, scheme: URIRef) -> set[URIRef]:
    concepts = set(graph.subjects(SKOS.inScheme, scheme))
    concepts.update(graph.objects(scheme, SKOS.hasTopConcept))
    return {concept for concept in concepts if isinstance(concept, URIRef)}


def _concept_uri(dataset: dict, concept: URIRef) -> str:
    """Return `prefix:code` for a concept, or the full IRI if it can't be shortened.

    The namespace of a concept is declared as a prefix, unless it is already
    declared, for example `dataset-type:APROF` for
    `http://publications.europa.eu/resource/authority/dataset-type/APROF`.
    """
    iri = str(concept)
    cut = max(iri.rfind("/"), iri.rfind("#")) + 1
    namespace, code = iri[:cut], iri[cut:]
    if not _NCNAME_RE.match(code):
        return iri

    prefixes = dataset.setdefault("prefixes", {})
    for name, prefix in prefixes.items():
        if prefix["uri"] == namespace:
            return f"{name}:{code}"

    name = namespace.rstrip("/#").rsplit("/", 1)[-1]
    if not _PREFIX_NAME_RE.match(name) or name in prefixes:
        return iri
    prefixes[name] = {
        "type": "prefix",
        "eid": max((p.get("eid", 0) for p in prefixes.values()), default=-1) + 1,
        "name": name,
        "uri": namespace,
    }
    return f"{name}:{code}"


def _iri_of(graph: Graph, name: Any) -> str:
    """Return full IRI for a `prefix:local` name or an IRI, empty if it can't be expanded."""
    if not name:
        return ""
    name = str(name)
    if "://" in name:
        return name
    prefix, sep, local = name.partition(":")
    if not sep:
        return ""
    for bound, namespace in graph.namespaces():
        if bound == prefix:
            return f"{namespace}{local}"
    return ""


def _is_web_iri(iri: str) -> bool:
    return str(iri).startswith(("http://", "https://"))


def _definitions(graph: Graph, subject: Node, lang: str) -> tuple[str, str]:
    """Return title and description of a subject from its RDFS, SKOS or SHACL definitions."""
    return (
        _pick_text(graph, subject, _TITLE_PREDICATES, lang),
        _pick_text(graph, subject, _DESCRIPTION_PREDICATES, lang),
    )


class _Definitions:
    """Titles and descriptions of IRIs.

    Definitions are taken from the sources first. IRIs that still have no title
    or no description are looked up by dereferencing them, because vocabularies
    (DCAT, DCTERMS, controlled vocabularies, ...) are published separately from
    the shapes that use them. Each IRI is looked up separately, concurrently.
    """

    def __init__(self, graph: Graph, lang: str, *, fetch_remote: bool = False):
        self.graph = graph
        self.lang = lang
        self.fetch_remote = fetch_remote
        self._sources: list[Graph] = []
        self._remote: dict[str, tuple[str, str]] = {}
        self._lock = threading.Lock()
        self._unavailable: Counter[str] = Counter()

    def add_source(self, graph: Graph) -> None:
        """Use definitions from a graph that was fetched, for example a concept scheme."""
        self._sources.append(graph)

    def get(self, iri: str) -> tuple[str, str]:
        title, description = self._local(iri)
        remote_title, remote_description = self._remote.get(iri, ("", ""))
        return title or remote_title, description or remote_description

    def _local(self, iri: str) -> tuple[str, str]:
        title = description = ""
        for graph in (self.graph, *self._sources):
            found_title, found_description = _definitions(graph, URIRef(iri), self.lang)
            title = title or found_title
            description = description or found_description
        return title, description

    def prefetch(self, iris: Iterable[str]) -> None:
        """Look up IRIs that have no title or no description in the sources, when remote fetching is enabled."""
        if not self.fetch_remote:
            return
        missing = sorted(
            {iri for iri in iris if _is_web_iri(iri) and iri not in self._remote and not all(self._local(iri))}
        )
        if not missing:
            return
        with ThreadPoolExecutor(max_workers=_LOOKUP_WORKERS) as pool:
            for iri, found in zip(missing, pool.map(self._lookup, missing)):
                self._remote[iri] = found

    def _lookup(self, iri: str) -> tuple[str, str]:
        host = urllib.parse.urlsplit(iri).netloc
        with self._lock:
            if self._unavailable[host] >= _HOST_FAILURE_LIMIT:
                return "", ""
        try:
            graph = _fetch_graph(iri)
        except _Unavailable as e:
            log.warning("Could not fetch %s: %s", iri, e)
            with self._lock:
                self._unavailable[host] += 1
                if self._unavailable[host] == _HOST_FAILURE_LIMIT:
                    log.warning("%s is not available, its definitions are not looked up.", host)
            return "", ""
        if graph is None:
            return "", ""
        return _definitions(graph, URIRef(iri), self.lang)


class _Unavailable(Exception):
    """An IRI can't be dereferenced, because its server is not available."""


def _fetch_vocabulary(scheme: URIRef) -> Graph | None:
    """Fetch RDF that describes a concept scheme from its IRI, `None` if it is not available."""
    try:
        return _fetch_graph(str(scheme))
    except _Unavailable as e:
        log.warning("Could not fetch concept scheme %s: %s", scheme, e)
        return None


def _is_timeout(error: BaseException) -> bool:
    """Whether a request failed because the server did not answer in time."""
    if isinstance(error, urllib.error.URLError):
        return isinstance(error.reason, TimeoutError)
    return isinstance(error, TimeoutError)


def _fetch_graph(iri: str) -> Graph | None:
    """Fetch RDF that describes an IRI, `None` if the IRI can't be dereferenced.

    Servers are sometimes temporarily unavailable, so server errors are retried.
    Pages that are not RDF, for example HTML, are skipped.
    """
    if not _is_web_iri(iri):
        return None

    error: Exception | None = None
    for attempt in range(1, _FETCH_ATTEMPTS + 1):
        request = urllib.request.Request(iri, headers={"Accept": _RDF_ACCEPT, "User-Agent": _USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=_FETCH_TIMEOUT) as response:
                content = response.read()
                media_type = response.headers.get_content_type()
            break
        except urllib.error.HTTPError as e:
            if e.code < 500:
                log.debug("Could not fetch %s: %s", iri, e)
                return None
            error = e
        except (OSError, ValueError) as e:
            if _is_timeout(e):
                log.warning("Timed out fetching %s, skipped.", iri)
                return None
            error = e
        if attempt < _FETCH_ATTEMPTS:
            time.sleep(attempt)
    else:
        raise _Unavailable(str(error))

    if media_type == "text/html":
        log.debug("%s is HTML, RDF is not available.", iri)
        return None
    if media_type in _XML_MEDIA_TYPES:
        fmt = "xml"
    elif media_type in _TURTLE_MEDIA_TYPES:
        fmt = "turtle"
    else:
        # Byte order mark and whitespace may precede the XML declaration.
        fmt = "xml" if content.lstrip(b"\xef\xbb\xbf \t\r\n").startswith(b"<") else "turtle"

    graph = Graph(bind_namespaces="none")
    try:
        graph.parse(data=content, format=fmt)
    except Exception as e:
        # Any parse error means the IRI is not usable.
        log.warning("Could not parse RDF from %s: %s", iri, e)
        return None
    return graph


def _pick_text(graph: Graph, subject: Node, predicates: list[URIRef], lang: str) -> str:
    """Return text in the requested language, falling back to text without a language tag."""
    untagged = ""
    for predicate in predicates:
        for value in graph.objects(subject, predicate):
            if not isinstance(value, Literal):
                continue
            if value.language == lang:
                return str(value)
            if value.language is None and not untagged:
                untagged = str(value)
    return untagged


def _first_text(values: list[str]) -> str:
    return next((v for v in values if v), "")


def _join(values: list[str]) -> str:
    return "\n".join(sorted({v for v in values if v}))


# RDFS and OWL ontologies.


def _ontology_rows(graph: Graph, sources: Graph, shacl_rows: list[dict]) -> Iterator[dict]:
    """Yield model and property rows for RDFS and OWL ontologies.

    Imported ontologies describe far more classes and properties than a profile
    uses, and the properties a profile uses are defined by its SHACL shapes. So:

    - classes are used if they are declared in the given files, or targeted by
      SHACL shapes, plus their superclasses, so that the class hierarchy is kept,
      and the ranges of used properties,
    - properties are taken from the given files only, if their domain is used,
      properties that SHACL shapes already define are skipped.

    Imported ontologies still give superclasses, labels and descriptions.
    """
    declared = _declared_classes(graph)
    shacl_models = {row["model"] for row in shacl_rows if row["property"] is None}
    defined = {(row["model"], row["property"]) for row in shacl_rows if row["property"] is not None}

    used: set[URIRef] = shacl_models | _declared_classes(sources)
    included: dict[tuple[URIRef | None, URIRef], URIRef | None] = {}

    changed = True
    while changed:
        changed = False
        for cls in sorted(used, key=str):
            for sup in _direct_superclasses(graph, cls, declared):
                if sup not in used:
                    used.add(sup)
                    changed = True

        for prop in _property_candidates(sources):
            for model in _domains(graph, prop) or [None]:
                key = (model, prop)
                if key in included:
                    continue
                if model is not None and model not in used:
                    continue
                rng = _ontology_range(graph, prop, declared)
                included[key] = rng
                if rng is not None and _is_model_type(rng):
                    used.add(rng)
                changed = True

    for cls in sorted(used, key=str):
        supers = _direct_superclasses(graph, cls, declared)
        yield {
            "schema": "owl" if (cls, RDF.type, OWL.Class) in graph else "rdfs",
            "base": supers[0] if supers else None,
            "model": cls,
            "property": None,
            "type": None,
            "title": "",
            "description": "",
            "required": None,
            "array": None,
        }

    for (model, prop), rng in sorted(included.items(), key=lambda item: (str(item[0][0] or ""), str(item[0][1]))):
        if (model, prop) in defined:
            continue
        yield {
            "schema": "rdfs",
            "base": None,
            "model": model,
            "property": prop,
            "type": rng,
            "title": "",
            "description": "",
            "required": None,
            "array": None,
        }


def _declared_classes(graph: Graph) -> set[URIRef]:
    """Return classes declared with `rdfs:Class` or `owl:Class`."""
    classes = set(graph.subjects(RDF.type, RDFS.Class)) | set(graph.subjects(RDF.type, OWL.Class))
    return {c for c in classes if isinstance(c, URIRef)}


def _direct_superclasses(graph: Graph, cls: URIRef, declared: set[URIRef]) -> list[URIRef]:
    """Return declared superclasses of a class, sorted, the first one is used as the base model.

    Built-in types, for example `owl:Thing`, are not models.
    """
    return sorted(
        {
            sup
            for sup in graph.objects(cls, RDFS.subClassOf)
            if isinstance(sup, URIRef) and sup in declared and sup != cls and _is_model_type(sup)
        },
        key=str,
    )


def _superclasses(graph: Graph, cls: URIRef) -> set[URIRef]:
    """Return a class and all its superclasses, following `rdfs:subClassOf` transitively."""
    found = {cls}
    stack = [cls]
    while stack:
        for sup in graph.objects(stack.pop(), RDFS.subClassOf):
            if isinstance(sup, URIRef) and sup not in found:
                found.add(sup)
                stack.append(sup)
    return found


def _common_superclass(graph: Graph, classes: set[URIRef]) -> URIRef | None:
    """Return the most specific declared class that all given classes are subclasses of.

    Used for a union of classes, for example `Catalog`, `Dataset` and
    `DataService` are all `dcat:Resource`. Returns `None` if there is no single
    such class, or if the classes are not declared in the graph.
    """
    common = set.intersection(*(_superclasses(graph, c) for c in classes))
    declared = {c for c in common if _is_declared(graph, c)}
    specific = [c for c in declared if not (_superclasses(graph, c) - {c}) & declared]
    return specific[0] if len(specific) == 1 else None


def _is_declared(graph: Graph, cls: URIRef) -> bool:
    return (cls, RDF.type, RDFS.Class) in graph or (cls, RDF.type, OWL.Class) in graph


def _property_candidates(graph: Graph) -> list[URIRef]:
    """Return properties that are declared or have a domain or a range."""
    candidates = set(graph.subjects(RDF.type, RDF.Property))
    candidates.update(graph.subjects(RDF.type, OWL.ObjectProperty))
    candidates.update(graph.subjects(RDF.type, OWL.DatatypeProperty))
    candidates.update(graph.subjects(RDFS.domain, None))
    candidates.update(graph.subjects(RDFS.range, None))
    return sorted((p for p in candidates if isinstance(p, URIRef)), key=str)


def _domains(graph: Graph, prop: URIRef) -> list[URIRef]:
    return sorted({d for d in graph.objects(prop, RDFS.domain) if isinstance(d, URIRef)}, key=str)


def _ontology_range(graph: Graph, prop: URIRef, declared: set[URIRef]) -> URIRef | None:
    """Return the range of a property if it is a known datatype or a declared class.

    Properties with several ranges, for example unions, are not typed.
    """
    ranges = sorted(set(graph.objects(prop, RDFS.range)), key=str)
    if len(ranges) != 1 or not isinstance(ranges[0], URIRef):
        return None
    rng = ranges[0]
    if _canonical_type(rng) in TYPES or rng in declared:
        return rng
    return None
