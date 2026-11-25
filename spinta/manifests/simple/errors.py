"""Errors of a `simple` document (3.3, 4.10).

A `BaseError` raised while linking or checking becomes a `ManifestError` and
is stored on the element it is about. The element is found from the error's
`context` (3.3). An error whose element is not in the document goes to the
`manifest` of its input (4.3). Read errors (rows that cannot be read) are built here too,
so that every stored error has the same shape.
"""

from __future__ import annotations

from typing import Any

from spinta.exceptions import BaseError
from spinta.manifests.simple.types import ManifestError

# The column an error is about (3.3, `key`). Errors not listed have `null`.
ERROR_KEYS: dict[str, str] = {
    "UndefinedEnum": "ref",
    "ModelReferenceKeyNotFound": "ref",
    "UnresolvedReference": "ref",
    "MissingRefModel": "ref",
    "InvalidLevel": "level",
    "UndeclaredUriPrefix": "uri",
    "UndeclaredEliPrefix": "eli",
}

# Keys of an error's `context` that describe Spinta's objects, not the element
# (`get_error_context`). They are not stored (3.3: `context` names the element).
_INTERNAL_CONTEXT = frozenset({"component", "manifest", "schema", "entity", "attribute", "this"})

# The code of a row that cannot be read (4.10). Read errors of rows are
# always this code, or the class name of a `BaseError` raised by the reader.
READ_ERROR_CODE = "TabularManifestError"


def read_error(code: str, message: str, index: int | None, context: dict[str, str] | None = None) -> ManifestError:
    """A `ManifestError` for a row that cannot be read (4.10). `context` names what the error is about (3.3)."""
    return {
        "code": code,
        "severity": "error",
        "message": message,
        "template": message,
        "context": dict(context or {}),
        "index": index,
        "key": None,
    }


def index_of_line(line: str | None) -> int | None:
    """The data row of a record label, for a CSV-like table (4.2): the header is record 1."""
    if line is not None and line.isdigit():
        return int(line) - 1
    return None


def manifest_error(error: BaseError, index: int | None = None) -> ManifestError:
    """The `ManifestError` of a `BaseError` (3.3). `index` is the index of its element."""
    code = type(error).__name__
    return {
        "code": code,
        "severity": error.severity,
        "message": error.message,
        "template": error.template or "",
        "context": {k: str(v) for k, v in error.context.items() if v is not None and k not in _INTERNAL_CONTEXT},
        "index": index,
        "key": ERROR_KEYS.get(code),
    }


def element_of(error: BaseError) -> tuple[str, str] | None:
    """The `(type, path)` of the element an error is about, from its `context` (3.3)."""
    return element_in_context(error.context)


def element_in_context(context: dict[str, Any]) -> tuple[str, str] | None:
    """The `(type, path)` of the element a `context` names (3.3).

    A property names its model and property, a model its path, and a
    resource or dataset its dataset. `None` when the context names none.
    """
    if context.get("model") and context.get("property"):
        return "property", f"{context['model']}/{context['property']}"
    if context.get("model"):
        return "model", str(context["model"])
    if context.get("dataset") and context.get("resource"):
        return "resource", f"{context['dataset']}/{context['resource']}"
    if context.get("dataset"):
        return "dataset", str(context["dataset"])
    return None
