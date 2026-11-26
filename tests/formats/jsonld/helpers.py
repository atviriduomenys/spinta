"""Test harness for JSON-LD responses (`.tmp/specs/json-ld.md`, sections 9.4 and 9.5, decision 10.16).

- `to_triples(body)`: the RDF triples of a JSON-LD body, with blank nodes canonicalized, so two bodies that mean the same
  graph give the same set. It fails for a body with colliding keywords (finding 2).
- `served_contexts()`: serves remote `@context` URLs from memory. `rdflib` fetches them over HTTP. This patches its
  remote context reader and refuses any URL that is not served, so the tests never reach `data.gov.lt`.
- `served_documents()`: the context documents of the example manifest, under the URLs of URL mode. They come from
  `spinta copy` (task 4), and the protocol context `_uapi.json` (task 5).
- `in_mode(body, mode)`: the same response in URL mode (the goldens) or inline mode (10.16).
- `golden(name, mode)`, `plain(name)`: the goldens of `goldens/`, and the plain responses of `goldens/plain/`.
- `strip_jsonld(body)`: the plain response of an enriched one, for the backward compatibility check (9.5).
"""

from __future__ import annotations

import copy
import json
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from typing import Any
from unittest import mock

import rdflib
import rdflib.plugins.shared.jsonld.context as jsonld_context
from rdflib.compare import to_canonical_graph
from rdflib.namespace import XSD
from rdflib.plugins.shared.jsonld.context import Context

from spinta.lib import Spinta
from tests.manifests.jsonld.helpers import SPEC_CSV, UR

URL = "url"
INLINE = "inline"
MODES = (INLINE, URL)

CONTEXT_URL = f"{UR}/context"
PROTOCOL_URL = f"{CONTEXT_URL}/_uapi.json"
PROTOCOL_CONTEXT = {"_data": "@graph"}  # the inline value of the protocol context (10.4, 10.16)

GOLDENS = Path(__file__).parent / "goldens"
BASE = "https://base.invalid/"  # rdflib resolves relative IRIs against it, so one would show up as an invalid IRI

KEYWORDS = {"@id", "@type"}
JSONLD_KEYS = {"@context", "@id", "@type"}


class UnservedContext(RuntimeError):
    """A `@context` URL that the harness does not serve. The tests stay offline, so this is a failure."""


class CollidingKeywords(ValueError):
    """Two keys of one object expand to the same keyword, for example `_id` (mapped to `@id`) and `@id` (finding 2).

    JSON-LD processors refuse this (`colliding keywords`). rdflib does not: it silently takes one of the two, so the
    harness checks it itself.
    """


@lru_cache(maxsize=1)
def bundle() -> dict[str, Any]:
    """The context documents of the example manifest, by model name, as `spinta copy` writes them (task 4)."""
    doc = Spinta(uri=UR).copy(SPEC_CSV.encode("utf-8"), "csv")
    return json.loads(Spinta(uri=UR).copy(doc, "jsonld"))


def served_documents() -> dict[str, dict[str, Any]]:
    """The documents served under the context URLs, by URL (URL mode, 10.9, 10.16). A copy, so a test can change it."""
    documents = {f"{CONTEXT_URL}/{name}.json": document for name, document in bundle().items()}
    documents[PROTOCOL_URL] = {"@context": PROTOCOL_CONTEXT}
    return copy.deepcopy(documents)


def _serve(url: str, *args: Any, **kwargs: Any) -> tuple[dict[str, Any], None]:
    documents = served_documents()
    if url not in documents:
        raise UnservedContext(f"{url} is not served by the test harness")
    return copy.deepcopy(documents[url]), None


@contextmanager
def served_contexts() -> Iterator[None]:
    """Serve remote `@context` URLs from `served_documents()` while the block runs."""
    with mock.patch.object(jsonld_context, "source_to_json", side_effect=_serve):
        yield


def check_colliding_keywords(body: Any, context: Context | None = None) -> None:
    """Raise `CollidingKeywords` if two keys of one object expand to the same keyword (finding 2).

    The active context follows the same scope rules as JSON-LD: a `@context` key of an object, and a term with a
    scoped `@context`, change it for the object and its values. Remote contexts are served (`served_contexts`).
    """
    if isinstance(body, list):
        for item in body:
            check_colliding_keywords(item, context)
        return
    if not isinstance(body, dict):
        return
    if context is None:
        context = Context(base=BASE)
    if "@context" in body:
        context = context.subcontext(copy.deepcopy(body["@context"]))

    keywords = Counter(keyword for key in body if (keyword := _keyword(context, key)) is not None)
    for keyword in KEYWORDS:
        if keywords[keyword] > 1:
            raise CollidingKeywords(f"colliding keywords: {sorted(k for k in body if _keyword(context, k) == keyword)}")

    for key, value in body.items():
        if key == "@context":
            continue
        term = context.terms.get(key)
        child = context.subcontext(term.context) if term is not None and term.context else context
        check_colliding_keywords(value, child)


def _keyword(context: Context, key: str) -> str | None:
    if key.startswith("@"):
        return key
    term = context.terms.get(key)
    if term is not None and term.id and term.id.startswith("@"):
        return term.id
    return None


def _normalize(term: Any) -> Any:
    # A plain literal and an `xsd:string` literal are the same in RDF 1.1, and language tags are case-insensitive.
    if isinstance(term, rdflib.Literal):
        if term.datatype == XSD.string:
            return rdflib.Literal(str(term))
        if term.language:
            return rdflib.Literal(str(term), lang=term.language.lower())
    return term


def to_triples(body: Any) -> set[tuple[Any, Any, Any]]:
    """The RDF triples of a JSON-LD body.

    Blank nodes get canonical labels (`rdflib.compare.to_canonical_graph`), so the result is the same for every parse.
    Literals are compared by value (`_normalize`). Raises `CollidingKeywords` for a body with colliding keywords, and
    `UnservedContext` for a context URL the harness does not serve.
    """
    graph = rdflib.Graph()
    with served_contexts():
        check_colliding_keywords(body)
        graph.parse(data=json.dumps(body), format="json-ld", base=BASE)
    canonical = to_canonical_graph(graph)
    return {(s, p, _normalize(o)) for s, p, o in canonical}


def in_mode(body: Any, mode: str) -> Any:
    """The body in `mode`. The goldens are in URL mode. Inline mode writes the document in place of each URL (10.16)."""
    if mode == URL:
        return copy.deepcopy(body)
    if mode != INLINE:
        raise ValueError(f"unknown mode {mode!r}")
    return _inline(copy.deepcopy(body), served_documents())


def _inline(node: Any, documents: dict[str, dict[str, Any]]) -> Any:
    if isinstance(node, list):
        return [_inline(item, documents) for item in node]
    if not isinstance(node, dict):
        return node
    for key, value in node.items():
        if key == "@context" and isinstance(value, str):
            node[key] = copy.deepcopy(documents[value]["@context"])
        else:
            node[key] = _inline(value, documents)
    return node


def golden(name: str, mode: str = URL) -> Any:
    """The enriched response of a response type, from `goldens/<name>.json`, in `mode`."""
    return in_mode(_read(GOLDENS / f"{name}.json"), mode)


def plain(name: str) -> Any:
    """The response of a response type without JSON-LD, from `goldens/plain/<name>.json`."""
    return _read(GOLDENS / "plain" / f"{name}.json")


def _read(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def strip_jsonld(node: Any, *, ref: bool = False, extra: tuple[str, ...] = ()) -> Any:
    """Remove what JSON-LD adds to a response (9.5), for the backward compatibility check.

    Removed everywhere: `@context`, `@id` and `@type`. Removed from a ref, a value of a property that is a model object
    (rule 7): its `_type`. Removed from a model object (the response, or an item of `_data`): the keys in `extra`, for
    keys that the rules add to a response that did not have them (`_type` of a `select`, rule 7, 4.3; `_id` only when the
    client selects it, 10.17).
    """
    if isinstance(node, list):
        return [strip_jsonld(item, ref=ref, extra=extra) for item in node]
    if not isinstance(node, dict):
        return node
    is_object = not ref
    result = {}
    for key, value in node.items():
        if key in JSONLD_KEYS:
            continue
        if ref and key == "_type":
            continue
        if is_object and key in extra:
            continue
        if key == "_data":
            result[key] = strip_jsonld(value, ref=False, extra=extra)
        elif key.startswith("_"):
            result[key] = strip_jsonld(value, ref=False)
        else:
            result[key] = strip_jsonld(value, ref=True)
    return result
