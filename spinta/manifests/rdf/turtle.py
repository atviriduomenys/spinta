"""Minimal Turtle writer.

Triples are written in the order they were added, which keeps the output stable
and in the same order as the manifest it was generated from. The document is
split into sections, each section is introduced by a comment. Blank nodes that
are objects of other triples are written inline, nested in their subject.
Lists are written as Turtle collections, `( a b c )`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

from rdflib import BNode, Literal, URIRef
from rdflib.namespace import RDF, RDFS, SH, SKOS, XSD

Subject = URIRef | BNode
Predicate = URIRef


@dataclass(frozen=True)
class TurtleList:
    """Ordered values written as a Turtle collection, `( a b c )`."""

    items: tuple[URIRef | BNode, ...]


Object = URIRef | BNode | Literal | TurtleList

# Predicates written first, in this order. Other predicates are written after
# them, in the order they were added.
_PREDICATE_ORDER = [
    RDF.type,
    RDFS.label,
    SKOS.prefLabel,
    SH.name,
    RDFS.comment,
    SKOS.definition,
    SH.description,
    RDFS.subClassOf,
    SH.targetClass,
    SH.path,
    SKOS.inScheme,
    RDFS.domain,
    RDFS.range,
    SH["class"],
    SH.datatype,
    SH.hasValue,
    SH.minCount,
    SH.maxCount,
]

# Local names that can be written as a prefixed name, `prefix:local`.
_LOCAL_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-]*$")
_INTEGER_RE = re.compile(r"^[+-]?[0-9]+$")
_INVALID_IRI_CHARS_RE = re.compile(r'[\x00-\x20<>"{}|^`\\]')
_ESCAPES = {"\\": "\\\\", '"': '\\"', "\n": "\\n", "\r": "\\r", "\t": "\\t"}
_INDENT = "    "


@dataclass
class Section:
    """A group of subjects, written under one comment.

    Sections are created with `TurtleDocument.section`, which also fixes their
    position in the document.
    """

    doc: TurtleDocument = field(repr=False)
    title: str
    subjects: dict[Subject, None] = field(default_factory=dict)

    def add(self, subject: Subject, predicate: Predicate, obj: Object) -> None:
        self.subjects.setdefault(subject, None)
        self.doc.add(subject, predicate, obj)


class TurtleDocument:
    """Turtle document, built from prefixes, sections and triples."""

    def __init__(self) -> None:
        self.prefixes: dict[str, str] = {}
        self._sections: list[Section] = []
        self._triples: dict[Subject, dict[tuple[Predicate, Object], None]] = {}

    def bind(self, name: str, namespace: str) -> None:
        """Declare a prefix, the first declaration of a name wins."""
        self.prefixes.setdefault(name, str(namespace))

    def section(self, title: str) -> Section:
        section = Section(doc=self, title=title)
        self._sections.append(section)
        return section

    def add(self, subject: Subject, predicate: Predicate, obj: Object) -> None:
        """Add a triple, use `Section.add` to also place its subject in a section."""
        self._triples.setdefault(subject, {})[(predicate, obj)] = None

    def render(self) -> str:
        """Return the document as Turtle text."""
        # Blank nodes that are objects are written inline, not as subjects.
        referenced = {node for pairs in self._triples.values() for _, obj in pairs for node in _nodes(obj)}
        blocks = [self._render_prefixes()]
        for section in self._sections:
            subjects = [s for s in section.subjects if s not in referenced]
            blocks.append("\n\n".join([f"# {section.title}", *(self._render_subject(s) for s in subjects)]))
        return "\n\n".join(blocks) + "\n"

    def write(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.render(), encoding="utf-8")

    def _render_prefixes(self) -> str:
        lines = [f"@prefix {name}: <{namespace}> ." for name, namespace in sorted(self.prefixes.items())]
        return "\n".join(["# Prefixes", "", *lines])

    def _render_subject(self, subject: Subject) -> str:
        head = "[]" if isinstance(subject, BNode) else self._iri(subject)
        predicates = self._render_predicates(subject, depth=0)
        if not predicates:
            return f"{head} ."
        return f"{head}\n{predicates} ."

    def _render_predicates(self, subject: Subject, depth: int) -> str:
        indent = _INDENT * (depth + 1)
        pairs = sorted(self._triples.get(subject, {}), key=lambda pair: _predicate_rank(pair[0]))
        return " ;\n".join(f"{indent}{self._predicate(p)} {self._object(o, depth + 1)}" for p, o in pairs)

    def _object(self, obj: Object, depth: int) -> str:
        if isinstance(obj, BNode):
            return f"[\n{self._render_predicates(obj, depth)}\n{_INDENT * depth}]"
        if isinstance(obj, TurtleList):
            items = " ".join(self._object(item, depth) for item in obj.items)
            return f"({items})" if items else "()"
        if isinstance(obj, Literal):
            return self._literal(obj)
        return self._iri(obj)

    def _predicate(self, predicate: Predicate) -> str:
        if predicate == RDF.type:
            return "a"
        return self._iri(predicate)

    def _literal(self, literal: Literal) -> str:
        text = str(literal)
        if literal.datatype == XSD.integer and _INTEGER_RE.match(text):
            return text
        if literal.datatype == XSD.boolean and text in ("true", "false"):
            return text
        quoted = '"' + "".join(_ESCAPES.get(char, char) for char in text) + '"'
        if literal.language:
            return f"{quoted}@{literal.language}"
        if literal.datatype:
            return f"{quoted}^^{self._iri(literal.datatype)}"
        return quoted

    def _iri(self, iri: URIRef) -> str:
        """Return `prefix:local` if a declared prefix matches, otherwise `<iri>`."""
        value = str(iri)
        if _INVALID_IRI_CHARS_RE.search(value):
            raise ValueError(f"Can't write IRI with invalid characters as Turtle: {value!r}")

        best: tuple[str, str] | None = None
        for name, namespace in self.prefixes.items():
            if not value.startswith(namespace) or not _LOCAL_NAME_RE.match(value[len(namespace) :]):
                continue
            if best is None or len(namespace) > len(best[1]):
                best = (name, namespace)
        if best is None:
            return f"<{value}>"
        return f"{best[0]}:{value[len(best[1]) :]}"


def _nodes(obj: Object) -> list[BNode]:
    """Return blank nodes of an object, the items of a list are written inline too."""
    if isinstance(obj, BNode):
        return [obj]
    if isinstance(obj, TurtleList):
        return [item for item in obj.items if isinstance(item, BNode)]
    return []


def _predicate_rank(predicate: Predicate) -> int:
    try:
        return _PREDICATE_ORDER.index(predicate)
    except ValueError:
        return len(_PREDICATE_ORDER)
