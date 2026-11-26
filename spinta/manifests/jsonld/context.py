"""The JSON-LD context of one model, `.tmp/specs/json-ld.md` section 3.3.

A context has a term for every property of the model that is visible to every caller (`package` or `public`, or no
visibility given), at every nesting level, whatever a request selects (rule 5). Each term is the IRI that
`spinta.manifests.simple.uri` gives the property (rule 2, `UriContext.property_iri`). Properties that are `private` or
`protected` have no term (10.2). So do the protocol keys, whose names start with `_` (`_id`, `_type`, ...), and the
back references, which are the other side of a reference (rule 5).

The IRIs are written as compact IRIs with the prefixes of the model's dataset (`schema:name`), so the context is short,
and a full IRI where no prefix covers it. `xsd` and `gsp` are added when a datatype needs them.
"""

from __future__ import annotations

from typing import Any

from spinta.components import Model
from spinta.core.enums import Visibility
from spinta.manifests.simple.uri import UriContext
from spinta.types.datatype import Array, ArrayBackRef, BackRef, Object, Ref

XSD = "http://www.w3.org/2001/XMLSchema#"
GSP = "http://www.opengis.net/ont/geosparql#"

# Spinta types and their XSD datatypes. `number` is `xsd:decimal`, as the RDF writer maps it.
_XSD_TYPES = {
    "integer": ("xsd", XSD, "integer"),
    "number": ("xsd", XSD, "decimal"),
    "boolean": ("xsd", XSD, "boolean"),
    "date": ("xsd", XSD, "date"),
    "datetime": ("xsd", XSD, "dateTime"),
    "time": ("xsd", XSD, "time"),
    "geometry": ("gsp", GSP, "wktLiteral"),
}


def hidden(visibility: Visibility | None) -> bool:
    """Whether a model or property with this visibility has no context: `protected` and `private` (10.2, 10.7)."""
    return visibility in (Visibility.private, Visibility.protected)


def in_scope(model: Model) -> bool:
    """Whether a model has a context at all. A `protected` or `private` model has none (10.2)."""
    return not hidden(model.visibility)


class ModelContext:
    """The context document of one model. `uris` gives the IRIs, and the prefixes come from the model's dataset."""

    def __init__(self, uris: UriContext, manifest: Any, model: Model) -> None:
        self.uris = uris
        self.model = model
        dataset = uris.dataset(model)
        source = dataset.prefixes if dataset is not None else manifest.prefixes
        self.prefixes: dict[str, str] = {name: p.uri for name, p in (source or {}).items() if p.uri}
        # `xsd` and `gsp`, when a term needs them. Their names are taken only when the dataset declares them too.
        self.added: dict[str, str] = {}

    def document(self) -> dict[str, Any]:
        """`{"@context": {...}}`: the prefixes the model uses, and its terms."""
        terms = self._terms(self.model.properties, "")
        return {"@context": {**self.prefixes, **self.added, **terms}}

    # Terms.

    def _terms(self, properties: dict[str, Any] | None, parent: str) -> dict[str, Any]:
        """The terms of `properties`, keyed by their name. `parent` is the document path of their owner (`address`)."""
        terms: dict[str, Any] = {}
        for key, prop in (properties or {}).items():
            if key.startswith("_") or hidden(prop.visibility):
                continue
            name = f"{parent}.{key}" if parent else key
            term = self._term(prop, name)
            if term is not None:
                terms[key] = term
        return terms

    def _term(self, prop: Any, name: str) -> str | dict[str, Any] | None:
        dtype = prop.dtype
        if isinstance(dtype, BackRef | ArrayBackRef):
            return None
        if isinstance(dtype, Array):
            # An array keeps its IRI on its items (`tags[]`), so the term is the items' IRI with `@set`.
            items = dtype.items
            if items is None or isinstance(items.dtype, BackRef | ArrayBackRef):
                return None
            item_name = f"{name}[]"
            term = self._value(items, item_name, self._iri(items, item_name))
            if term is None:
                return None
            return self._finish(term, container="@set")
        return self._finish(self._value(prop, name, self._iri(prop, name)))

    def _iri(self, prop: Any, name: str) -> str | None:
        iri = self.uris.property_iri(self.model, prop, name)
        return iri or None

    def _value(self, prop: Any, name: str, iri: str | None) -> dict[str, Any] | None:
        """The term of a value: what its datatype adds to the IRI. `None` if there is no IRI, or no term."""
        if iri is None:
            return None
        dtype = prop.dtype
        term: dict[str, Any] = {"@id": self._compact(iri)}
        if isinstance(dtype, Ref):
            # A reference is a model object with its own `@id` (rule 7), so it needs no `@type` and no scoped context.
            if isinstance(dtype.model, Model) and hidden(dtype.model.visibility):
                return None
            return term
        if isinstance(dtype, Object):
            term["@context"] = self._terms(dtype.properties, name)
            return term
        if dtype.name == "text":
            term["@container"] = "@language"
            return term
        if dtype.name in _XSD_TYPES:
            prefix, namespace, local = _XSD_TYPES[dtype.name]
            term["@type"] = self._typed(prefix, namespace, local)
        return term

    def _finish(self, term: dict[str, Any] | None, container: str | None = None) -> str | dict[str, Any] | None:
        """A term with only an `@id` is written as the IRI itself, as in `"continent": "schema:containedInPlace"`."""
        if term is None:
            return None
        if container is not None:
            # A text array keeps its language container too, so the term is a list of both (JSON-LD 1.1).
            term["@container"] = [container, term["@container"]] if "@container" in term else container
        if set(term) == {"@id"}:
            return term["@id"]
        return term

    # IRIs.

    def _compact(self, iri: str) -> str:
        """The compact IRI of `iri` with the longest matching prefix of the dataset, or `iri` itself."""
        best: tuple[str, str] | None = None
        for name, namespace in self.prefixes.items():
            if iri.startswith(namespace) and len(iri) > len(namespace):
                if best is None or len(namespace) > len(best[1]) or (len(namespace) == len(best[1]) and name < best[0]):
                    best = (name, namespace)
        if best is None:
            return iri
        return f"{best[0]}:{iri[len(best[1]) :]}"

    def _typed(self, prefix: str, namespace: str, local: str) -> str:
        """A datatype as a compact IRI, such as `xsd:integer`. The prefix is added to the context, unless the dataset
        declares that name for another namespace; then the full IRI is written."""
        declared = self.prefixes.get(prefix)
        if declared is not None and declared != namespace:
            return f"{namespace}{local}"
        self.added[prefix] = namespace
        return f"{prefix}:{local}"
