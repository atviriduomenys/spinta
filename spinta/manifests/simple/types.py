"""Plain types of the `simple` manifest, see `spinta.lib`.

This module imports nothing from Spinta, so `spinta.lib` can re-export the
names without import cycles.
"""

from __future__ import annotations

from typing import Any, Literal, TypedDict

FORMAT_VERSION = 1

SimpleManifest = dict[str, Any]


class ElementRef(TypedDict):
    type: str
    path: str


class ManifestError(TypedDict):
    code: str
    severity: Literal["error", "warning"]
    message: str
    template: str
    context: dict[str, str]
    index: int | None
    key: str | None


class SimpleManifestError(ValueError):
    """A `simple` document is malformed, or merged documents conflict."""


# The element type of an input file (4.3). Its element has the path of the input (`source`), no parent, and
# holds the document-level errors (4.10), so every element without a parent points to it.
MANIFEST_TYPE = "manifest"

# Element types of the document, in the order they are written (4.1, 4.3).
ELEMENT_TYPES: tuple[str, ...] = (
    MANIFEST_TYPE,
    "backend",
    "ns",
    "prefix",
    "enum",
    "dataset",
    "resource",
    "base",
    "model",
    "property",
    "param",
    "append",
    "unique",
    "scope",
    "lang",
    "comment",
)

# Top level key of a document that is not an element type (4.1). It has the format version and the Spinta version.
SPINTA_KEY = "spinta"

# Keys of a `manifest` element, besides the keys of every element (4.3).
MANIFEST_KEYS: tuple[str, ...] = ("type", "source", "columns")

# The `source` of a manifest read from memory, which has no file path.
MEMORY_SOURCE = "<memory>"

# The column of the name of each element type. Its cell is not stored in
# `given`, because the path has the name (4.5). `None` means the name cell is
# kept, because the path cannot give it back: a unique row joins its columns
# with `_` (4.4), so `ref` is stored as written.
NAME_COLUMN: dict[str, str | None] = {
    MANIFEST_TYPE: None,
    "dataset": "dataset",
    "ns": "dataset",
    "backend": "resource",
    "resource": "resource",
    "base": "base",
    "model": "model",
    "property": "property",
    "prefix": "ref",
    "enum": "ref",
    "param": "ref",
    "lang": "ref",
    "scope": "ref",
    "unique": None,
    "comment": None,
    "append": None,
}

# Extra dimensions: their `type` cell is the keyword, which is not stored (4.5).
KEYWORD_TYPES = frozenset({"prefix", "enum", "param", "lang", "scope", "unique", "comment", "ns"})
