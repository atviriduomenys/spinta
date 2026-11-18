"""Write a manifest as RDF Turtle.

Every model and property is described by a SHACL shape, which is how values are
validated. Classes, properties and concepts are also described by RDFS classes,
RDFS properties and SKOS concepts, if they are described here, see below. Enums
are described by SKOS concept schemes.

A class, property or concept is described here if it has no `uri`, if its `uri`
is under `uri_prefix`, or if its `uri` is a relative path, which is relative to
`uri_prefix`. Other items are only referenced by their `uri`.

Generated URIs are built from `uri_prefix` configuration option, for example
`https://data.gov.lt/id/`. It defaults to `https://example.com/id/`:

- classes: `{uri_prefix}{dataset}/{model}`
- properties: `{uri_prefix}{dataset}/{property}`
- concept schemes (enums): `{uri_prefix}{dataset}/{enum}`. Inline enums are
  named `{model}_{property}`.
- concepts: `{uri_prefix}{dataset}/{enum}/{code}`, so concepts of different
  enums with the same code are different concepts. The prepared value of a
  concept is its `skos:notation`, written only when it differs from the local
  name of the concept, which is the source of an enum item.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import quote

import click
from rdflib import BNode, Literal, URIRef
from rdflib.namespace import OWL, RDF, RDFS, SH, SKOS, XSD

from spinta import commands
from spinta.components import Context, Model, Property
from spinta.core.enums import Access
from spinta.dimensions.enum.components import EnumItem, EnumValue
from spinta.dimensions.prefix.components import UriPrefix
from spinta.manifests.components import Manifest
from spinta.manifests.rdf.helpers import _local_name
from spinta.manifests.rdf.turtle import Section, TurtleDocument, TurtleList
from spinta.types.datatype import Array, ArrayBackRef, BackRef, DataType, Ref
from spinta.utils.schema import NA

if TYPE_CHECKING:
    from spinta.components import Namespace
    from spinta.datasets.components import Dataset

log = logging.getLogger(__name__)

# Prefixes that are always declared, unless the manifest declares the same name.
_STANDARD_PREFIXES = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "sh": "http://www.w3.org/ns/shacl#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
}

# Spinta types and their XSD datatypes. Other types have no datatype constraint.
_XSD_TYPES: dict[str, URIRef] = {
    "string": XSD.string,
    "url": XSD.anyURI,
    "uri": XSD.anyURI,
    "integer": XSD.integer,
    "number": XSD.decimal,
    "boolean": XSD.boolean,
    "date": XSD.date,
    "datetime": XSD.dateTime,
    "time": XSD.time,
    "binary": XSD.base64Binary,
}


def write_rdf_manifest(
    context: Context,
    manifest: Manifest,
    output: str | Path,
    *,
    access: Access = Access.private,
) -> None:
    """Write manifest as RDF Turtle file, with SHACL shapes, RDFS and SKOS definitions."""
    config = context.get("config")
    exporter = _RdfExporter(context, manifest, uri_prefix=config.uri_prefix, access=access)
    exporter.export().write(output)


@dataclass(frozen=True)
class _Value:
    """What values of a property are: a concept of a scheme, a class, a datatype, or nothing that can be expressed."""

    cls: URIRef | None = None
    datatype: URIRef | None = None
    scheme: URIRef | None = None

    @property
    def range(self) -> URIRef | None:
        if self.scheme is not None:
            return SKOS.Concept
        return self.cls or self.datatype


@dataclass
class _Enum:
    name: str
    dataset: str
    scheme: URIRef
    values: EnumValue
    # Prefixes of the dataset that declares the enum, or of the manifest.
    prefixes: dict[str, str]


@dataclass
class _PropertyDefinition:
    title: str = ""
    description: str = ""
    # Ordered sets, values are dict keys.
    domains: dict[URIRef, None] = field(default_factory=dict)
    # `None` means that the range can't be expressed.
    ranges: dict[URIRef | None, None] = field(default_factory=dict)


class _RdfExporter:
    def __init__(self, context: Context, manifest: Manifest, *, uri_prefix: str | None, access: Access):
        self.context = context
        self.manifest = manifest
        self.uri_prefix = uri_prefix
        self.access = access
        self.doc = TurtleDocument()
        # Enums are keyed by identity, values of the same enum are shared by all
        # properties that use it.
        self.enums: dict[int, _Enum] = {}
        self.definitions: dict[URIRef, _PropertyDefinition] = {}
        # Prefixes are not shared between datasets, a URI is expanded with the prefixes of its owner.
        self._manifest_prefixes: dict[str, str] = {}
        self._dataset_prefixes: dict[str, dict[str, str]] = {}
        self._namespace_owner: dict[str, str] = {}

    def export(self) -> TurtleDocument:
        models = [m for m in commands.get_models(self.context, self.manifest).values() if self._is_exported(m)]
        exported = _namespaces_with_exported_models(models)
        namespaces = [ns for ns in commands.get_namespaces(self.context, self.manifest).values() if ns.name in exported]
        self._load_prefixes(models)
        self._bind_prefixes(models, namespaces)

        # Sections are written in the order they are created.
        classes = self.doc.section("Classes")
        properties = self.doc.section("Properties")
        concepts = self.doc.section("Concepts")
        shapes = self.doc.section("SHACL shapes")

        for namespace in namespaces:
            for name, values in (namespace.enums or {}).items():
                self._register_enum(values, namespace.name, name, self._namespace_owner.get(namespace.name))
        for name, values in (self.manifest.enums or {}).items():
            self._register_enum(values, "", name, None)

        for model in models:
            if self._is_described(self._class_iri(model)):
                self._class_definition(classes, model)
            self._shape(shapes, model)

        for iri, definition in self.definitions.items():
            self._property_definition(properties, iri, definition)

        for enum in self.enums.values():
            self._concepts(concepts, enum)

        return self.doc

    def _is_exported(self, model: Model) -> bool:
        if model.basename.startswith("_"):
            return False
        return model.access is None or model.access >= self.access

    def _load_prefixes(self, models: list[Model]) -> None:
        """Collect prefixes of the manifest and of every dataset, and the dataset that owns each namespace."""
        self._manifest_prefixes = _declared(self.manifest.prefixes)
        for dataset in commands.get_datasets(self.context, self.manifest).values():
            self._add_dataset(dataset)
        for model in models:
            if model.external and model.external.dataset:
                self._add_dataset(model.external.dataset)

    def _add_dataset(self, dataset: Dataset) -> None:
        self._dataset_prefixes[dataset.name] = _declared(dataset.prefixes)
        if dataset.ns is not None:
            self._namespace_owner[dataset.ns.name] = dataset.name

    def _prefixes(self, owner: str | None) -> dict[str, str]:
        """Return prefixes that URIs of an owner are expanded with.

        The owner's own prefixes win over the manifest's, so the same name can
        mean different namespaces in different datasets.
        """
        own = self._dataset_prefixes.get(owner, {}) if owner else {}
        return {**self._manifest_prefixes, **own}

    def _bind_prefixes(self, models: list[Model], namespaces: list[Namespace]) -> None:
        """Declare each namespace once, under one name.

        Datasets declare prefixes independently, so one name can stand for
        different namespaces. The first declaration keeps the name, a later one
        for another namespace gets a number suffix, like `ex_2`. Full IRIs are
        abbreviated with these names, so every abbreviation is unambiguous.
        """
        owners = {_owner(m) for m in models}
        owners.update(
            self._namespace_owner[ns.name] for ns in namespaces if ns.enums and ns.name in self._namespace_owner
        )
        sources = [self._dataset_prefixes.get(owner, {}) for owner in sorted(o for o in owners if o)]
        sources.append(self._manifest_prefixes)

        bound: dict[str, str] = {}  # Namespace -> name it is declared under.
        for prefixes in sources:
            for name, namespace in prefixes.items():
                if namespace in bound:
                    continue
                output, number = name, 1
                while output in self.doc.prefixes:
                    number += 1
                    output = f"{name}_{number}"
                self.doc.bind(output, namespace)
                bound[namespace] = output
        for name, namespace in _STANDARD_PREFIXES.items():
            self.doc.bind(name, namespace)

    def _class_definition(self, section: Section, model: Model) -> None:
        cls = self._class_iri(model)
        section.add(cls, RDF.type, RDFS.Class)
        _add_texts(section, cls, model.title, model.description, RDFS.label, RDFS.comment)
        if model.base and model.base.parent:
            section.add(cls, RDFS.subClassOf, self._class_iri(model.base.parent))

    def _property_definition(self, section: Section, iri: URIRef, definition: _PropertyDefinition) -> None:
        section.add(iri, RDF.type, RDF.Property)
        _add_texts(section, iri, definition.title, definition.description, RDFS.label, RDFS.comment)
        _domain(section, iri, list(definition.domains))
        # Ranges of a property shared by several models are used only if all of them agree.
        ranges = [r for r in definition.ranges if r is not None]
        if len(definition.ranges) == 1 and ranges:
            section.add(iri, RDFS.range, ranges[0])

    def _concepts(self, section: Section, enum: _Enum) -> None:
        section.add(enum.scheme, RDF.type, SKOS.ConceptScheme)
        section.add(enum.scheme, SKOS.prefLabel, Literal(enum.name))
        for code, item in enum.values.items():
            # Items below the access threshold are not exported, as in the tabular manifest.
            if item.access is not None and item.access < self.access:
                continue
            if item.uri:
                iri = self._uri_term(item.uri, enum.name, enum.prefixes)
            else:
                iri = self._generated(enum.dataset, enum.name, _code(code, item))
            # The key of an item is its source, which is the local name of the concept, so the prepared
            # value is written only when it differs from it. Source-only items have no notation.
            if item.prepare is not NA and item.prepare is not None and str(item.prepare) != _local_name(str(iri)):
                section.add(iri, SKOS.notation, Literal(item.prepare))
            if not self._is_described(iri):
                # Concepts that are not described here are only referenced by their URI.
                section.add(iri, SKOS.inScheme, enum.scheme)
                continue
            section.add(iri, RDF.type, SKOS.Concept)
            section.add(iri, SKOS.inScheme, enum.scheme)
            _add_texts(section, iri, item.title, item.description, SKOS.prefLabel, SKOS.definition)

    def _shape(self, section: Section, model: Model) -> None:
        shape = BNode()
        section.add(shape, RDF.type, SH.NodeShape)
        section.add(shape, SH.targetClass, self._class_iri(model))
        _add_texts(section, shape, model.title, model.description, SH.name, SH.description)
        for prop in _exported_properties(model, self.access):
            section.add(shape, SH.property, self._property_shape(section, model, prop))

    def _property_shape(self, section: Section, model: Model, prop: Property) -> BNode:
        term, inverse = self._property_term(model, prop)
        path = self._path(section, term, inverse)
        value, array = self._value(model, prop)
        # An inverse path describes the other side of a property, so it is not defined here.
        if not inverse and self._is_described(term):
            self._add_definition(model, prop, term, value)

        shape = BNode()
        section.add(shape, SH.path, path)
        _add_texts(section, shape, prop.title, prop.description, SH.name, SH.description)
        if value.scheme is not None:
            # Values are concepts of the scheme, DCAT-AP style: `skos:inScheme` of a value has the scheme.
            section.add(shape, SH["class"], SKOS.Concept)
            in_scheme = BNode()
            section.add(in_scheme, SH.path, SKOS.inScheme)
            section.add(in_scheme, SH.hasValue, value.scheme)
            section.add(shape, SH.property, in_scheme)
        elif value.cls is not None:
            section.add(shape, SH["class"], value.cls)
        elif value.datatype is not None:
            section.add(shape, SH.datatype, value.datatype)
        if prop.dtype.required:
            section.add(shape, SH.minCount, Literal(1))
        if not array:
            section.add(shape, SH.maxCount, Literal(1))
        return shape

    def _property_term(self, model: Model, prop: Property) -> tuple[URIRef, bool]:
        """Return IRI of a property, and whether it is an inverse path, `<` in a URI, like `< dcat:distribution`."""
        uri = _uri_of(prop)
        if not uri:
            return self._generated(_dataset_name(model), prop.name), False

        raw = str(uri).strip()
        inverse = raw.startswith("<")
        if inverse:
            raw = raw[1:].strip()
        term = self._uri_term(raw, prop.name, self._prefixes(_owner(model)))
        if term is None:
            term = self._generated(_dataset_name(model), prop.name)
        return term, inverse

    def _path(self, section: Section, term: URIRef, inverse: bool) -> URIRef | BNode:
        """Return SHACL path, an inverse path is a blank node with `sh:inversePath`."""
        if not inverse:
            return term
        path = BNode()
        section.add(path, SH.inversePath, term)
        return path

    def _value(self, model: Model, prop: Property) -> tuple[_Value, bool]:
        """Return what values of a property are, and whether the property is an array."""
        dtype: DataType = prop.dtype
        array = isinstance(dtype, Array)
        enum_owner: Property = prop
        if array:
            if dtype.items is None:
                return _Value(), True
            enum_owner = dtype.items
            dtype = dtype.items.dtype

        if enum_owner.enum is not None:
            return _Value(scheme=self._enum_for(model, prop, enum_owner).scheme), array

        if isinstance(dtype, Ref):
            if isinstance(dtype.model, Model):
                return _Value(cls=self._class_iri(dtype.model)), array
            return _Value(), array

        return _Value(datatype=_XSD_TYPES.get(dtype.name)), array

    def _enum_for(self, model: Model, prop: Property, owner: Property) -> _Enum:
        """Return enum of a property, enums are named after the enum they reference or after the model and property."""
        if (enum := self.enums.get(id(owner.enum))) is not None:
            return enum
        name = owner.given.enum or f"{model.basename}_{prop.name}"
        return self._register_enum(owner.enum, _dataset_name(model), name, _owner(model))

    def _register_enum(self, values: EnumValue, dataset: str, name: str, owner: str | None) -> _Enum:
        if (enum := self.enums.get(id(values))) is None:
            enum = _Enum(
                name=name,
                dataset=dataset,
                scheme=self._generated(dataset, name),
                values=values,
                prefixes=self._prefixes(owner),
            )
            self.enums[id(values)] = enum
        return enum

    def _add_definition(self, model: Model, prop: Property, iri: URIRef, value: _Value) -> None:
        definition = self.definitions.setdefault(iri, _PropertyDefinition())
        definition.title = definition.title or prop.title or ""
        definition.description = definition.description or prop.description or ""
        definition.domains.setdefault(self._class_iri(model))
        definition.ranges.setdefault(value.range)

    def _class_iri(self, model: Model) -> URIRef:
        if model.uri:
            return self._uri_term(model.uri, model.name, self._prefixes(_owner(model)))
        return self._generated(_dataset_name(model), model.basename)

    def _uri_term(self, raw: str | None, owner: str, prefixes: Mapping[str, str]) -> URIRef | None:
        """Expand a URI given in a manifest to an IRI, `None` if no URI is given.

        A URI is a full IRI, a prefixed name, like `dct:title`, or a relative path,
        which is relative to `uri_prefix`. A URI that is given but can't be expanded
        is an error, not a generated URI, because a typo in a prefix would silently
        produce a wrong IRI.
        """
        if not raw:
            return None
        raw = str(raw).strip()
        if "://" in raw:
            return URIRef(raw)
        prefix, sep, local = raw.partition(":")
        if sep and local and prefix in prefixes:
            return URIRef(prefixes[prefix] + local)
        if not sep:
            return URIRef(self._base() + raw.lstrip("/"))
        if local:
            reason = f"prefix {prefix!r} is not declared"
        else:
            reason = "expected `prefix:name`, a full IRI or a relative path"
        raise click.UsageError(f"Invalid URI {raw!r} of {owner!r}: {reason}.")

    def _base(self) -> str:
        """Return `uri_prefix` with a trailing slash, all generated and relative URIs start with it."""
        if not self.uri_prefix:
            raise click.UsageError(
                "Generated RDF URIs need non-empty `uri_prefix` configuration option, for example "
                "`-o uri_prefix=https://data.gov.lt/id/` or `SPINTA_URI_PREFIX=https://data.gov.lt/id/`."
            )
        return self.uri_prefix.rstrip("/") + "/"

    def _is_described(self, iri: URIRef) -> bool:
        """Whether a class, property or concept with this IRI is described here, or only referenced.

        Described are items without a `uri` (their IRI is generated) and items whose
        IRI is under `uri_prefix`. Other items are only referenced by their IRI.
        """
        return bool(self.uri_prefix) and str(iri).startswith(self._base())

    def _generated(self, dataset: str, *names: str) -> URIRef:
        """Return URI generated as `{uri_prefix}{dataset}/{name}/...`, each name is percent-encoded completely.

        Empty segments, for example the dataset of a manifest without a dataset, are left out.
        """
        segments = [quote(dataset, safe="/"), *(quote(name, safe="") for name in names)]
        return URIRef(self._base() + "/".join(s for s in segments if s))


def _namespaces_with_exported_models(models: list[Model]) -> set[str]:
    """Return names of namespaces of the models, and of their parent namespaces.

    Enums of a namespace are used by models of its child namespaces too, so a
    namespace is exported if one of its child namespaces has an exported model.
    """
    names: set[str] = set()
    for model in models:
        names.add(model.ns.name)
        names.update(ns.name for ns in model.ns.parents())
    return names


def _exported_properties(model: Model, access: Access) -> Iterator[Property]:
    for prop in model.properties.values():
        if prop.name.startswith("_") and not prop.explicitly_given:
            continue
        if prop.access is not None and prop.access < access:
            continue
        if isinstance(prop.dtype, BackRef | ArrayBackRef):
            # Back references are the other side of references, they are described by them.
            continue
        yield prop


def _uri_of(prop: Property) -> str | None:
    """Return the URI given for a property.

    An array keeps its URI on its items, the array itself has none.
    """
    if isinstance(prop.dtype, Array) and prop.dtype.items is not None and prop.dtype.items.uri:
        return prop.dtype.items.uri
    return prop.uri


def _declared(prefixes: dict[str, UriPrefix] | None) -> dict[str, str]:
    """Return prefixes that have a URI, by their name."""
    return {name: prefix.uri for name, prefix in (prefixes or {}).items() if prefix.uri}


def _owner(model: Model) -> str | None:
    """Return name of the dataset that declares the model, `None` if it has no dataset."""
    if model.external and model.external.dataset:
        return model.external.dataset.name
    return None


def _dataset_name(model: Model) -> str:
    if model.external and model.external.dataset:
        return model.external.dataset.name
    return model.ns.name


def _code(key: str, item: EnumItem) -> str:
    """Return code of an enum item as it is written in a manifest, `true` and not `True`."""
    if isinstance(item.prepare, bool):
        return "true" if item.prepare else "false"
    return str(key)


def _add_texts(
    section: Section,
    subject: URIRef | BNode,
    title: str | None,
    description: str | None,
    title_predicate: URIRef,
    description_predicate: URIRef,
) -> None:
    if title:
        section.add(subject, title_predicate, Literal(title))
    if description:
        section.add(subject, description_predicate, Literal(description))


def _domain(section: Section, iri: URIRef, domains: list[URIRef]) -> None:
    """Write `rdfs:domain` of a property.

    Several `rdfs:domain` triples mean that a subject is an instance of all of
    them, not one of them. A property shared by several models is therefore
    written with the union of its domains, `owl:unionOf` of a Turtle list.
    """
    if len(domains) == 1:
        section.add(iri, RDFS.domain, domains[0])
    elif domains:
        section.doc.bind("owl", OWL)
        union = BNode()
        section.add(iri, RDFS.domain, union)
        section.doc.add(union, RDF.type, OWL.Class)
        section.doc.add(union, OWL.unionOf, TurtleList(tuple(domains)))
