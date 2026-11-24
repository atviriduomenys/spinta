"""Shape rules of a `simple` document (section 4.9).

Pure Python, no Spinta imports. `validate_document` raises
`SimpleManifestError` for a malformed document and never for DSA content.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from spinta.manifests.simple.types import (
    ELEMENT_TYPES,
    FORMAT_VERSION,
    KEYWORD_TYPES,
    MANIFEST_KEYS,
    MANIFEST_TYPE,
    NAME_COLUMN,
    SPINTA_KEY,
    SimpleManifest,
    SimpleManifestError,
)

ELEMENT_KEYS = ("index", "parent", "given", "implied", "errors")
ERROR_KEYS = ("code", "severity", "message", "template", "context", "index", "key")
SEVERITIES = ("error", "warning")


def _fail(message: str) -> None:
    raise SimpleManifestError(message)


def _check_error(where: str, error: Any) -> None:
    if not isinstance(error, dict):
        _fail(f"{where}: error must be an object.")
    missing = [k for k in ERROR_KEYS if k not in error]
    if missing:
        _fail(f"{where}: error is missing keys {missing}.")
    if error["severity"] not in SEVERITIES:
        _fail(f"{where}: error severity must be one of {SEVERITIES}.")
    if not isinstance(error["code"], str) or not error["code"]:
        _fail(f"{where}: error code must be a non-empty string.")
    if error["index"] is not None and not _is_index(error["index"]):
        _fail(f"{where}: error index must be an integer >= 1 or null.")
    if error["key"] is not None and not isinstance(error["key"], str):
        _fail(f"{where}: error key must be a string or null.")


def _is_index(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def document_columns(doc: SimpleManifest) -> list[str]:
    """The columns of a document: the union of the `columns` of its manifests, in order (4.3).

    A document has one table header. Each input file (`manifest` element) has its own `columns`, and a
    merged document has the columns of all of them, in the order they first appear.
    """
    columns: list[str] = []
    for manifest in doc.get(MANIFEST_TYPE, {}).values():
        for column in manifest["columns"]:
            if column not in columns:
                columns.append(column)
    return columns


def _check_manifest(where: str, path: str, elem: dict[str, Any], dsa_columns: set[str] | None) -> None:
    """Rules of a `manifest` element (4.3): an input file, with no row and no parent. Its path is its source."""
    if elem["parent"] is not None:
        _fail(f"{where}: a manifest has no parent.")
    if elem["index"] is not None or elem["given"] is not None or elem["implied"] is not None:
        _fail(f"{where}: a manifest has no index, given or implied.")
    if not isinstance(elem["type"], str) or not elem["type"]:
        _fail(f"{where}: type must be a non-empty string, the format of the input.")
    if not isinstance(elem["source"], str) or not elem["source"]:
        _fail(f"{where}: source must be a non-empty string, the path of the input.")
    if elem["source"] != path:
        _fail(f"{where}: the path of a manifest must be its source {elem['source']!r}.")
    columns = elem["columns"]
    if not isinstance(columns, list) or not all(isinstance(c, str) for c in columns):
        _fail(f"{where}: columns must be a list of strings.")
    if dsa_columns is not None:
        unknown = [c for c in columns if c not in dsa_columns]
        if unknown:
            _fail(f"{where}: columns has unknown columns {unknown}.")


def validate_document(doc: Any, dsa_columns: Iterable[str] | None = None) -> None:
    """Validate the shape of a `simple` document (rules 1 to 8 of 4.9).

    `dsa_columns` is the set of known DSA columns. When it is given, rule 1
    checks the columns of every manifest against it. The caller passes it, so
    this module imports nothing from Spinta.

    Raises `SimpleManifestError` on the first violation.
    """
    if not isinstance(doc, dict):
        _fail("Document must be a JSON object.")

    # Rule 1.
    spinta = doc.get(SPINTA_KEY)
    if not isinstance(spinta, dict):
        _fail("Document must have a `spinta` object.")
    for key in ("version", "format"):
        if key not in spinta:
            _fail(f"`spinta.{key}` is missing.")
    if not isinstance(spinta["format"], int) or isinstance(spinta["format"], bool):
        _fail("`spinta.format` must be an integer.")
    if spinta["format"] > FORMAT_VERSION:
        _fail(f"`spinta.format` {spinta['format']} is newer than this Spinta reads ({FORMAT_VERSION}).")
    if not isinstance(doc.get(MANIFEST_TYPE), dict) or not doc[MANIFEST_TYPE]:
        _fail(f"Document must have at least one `{MANIFEST_TYPE}` element (4.3).")
    known = set(dsa_columns) if dsa_columns is not None else None

    indexes: set[int] = set()
    elements: dict[tuple[str, str], dict] = {}

    # Rule 2, the first pass, to know every element before checking parents.
    for type_, elems in doc.items():
        if type_ == SPINTA_KEY:
            continue
        if type_ not in ELEMENT_TYPES:
            _fail(f"Unknown element type {type_!r}.")
        if not isinstance(elems, dict):
            _fail(f"Type {type_!r} must be an object of elements.")
        keys = ELEMENT_KEYS + MANIFEST_KEYS if type_ == MANIFEST_TYPE else ELEMENT_KEYS
        for path, elem in elems.items():
            if not isinstance(elem, dict):
                _fail(f"{type_}/{path}: element must be an object.")
            missing = [k for k in keys if k not in elem]
            if missing:
                _fail(f"{type_}/{path}: element is missing keys {missing}.")
            elements[(type_, path)] = elem

    # The manifests come first: their columns are the columns of the document (rule 5).
    for path, elem in doc[MANIFEST_TYPE].items():
        _check_manifest(f"{MANIFEST_TYPE}/{path}", path, elem, known)
    columns = document_columns(doc)

    for (type_, path), elem in elements.items():
        where = f"{type_}/{path}"

        # Rule 3.
        index = elem["index"]
        given = elem["given"]
        if index is not None:
            if not _is_index(index):
                _fail(f"{where}: index must be an integer >= 1 or null.")
            if index in indexes:
                _fail(f"{where}: index {index} is used twice.")
            indexes.add(index)
        if (index is None) != (given is None):
            _fail(f"{where}: index is null exactly when given is null.")

        # Rule 4. Every element but a manifest has a parent: a top level element points to its manifest.
        parent = elem["parent"]
        if parent is None and type_ != MANIFEST_TYPE:
            _fail(f"{where}: parent is null, but only a manifest has no parent.")
        if parent is not None:
            if not isinstance(parent, dict) or set(parent) != {"type", "path"}:
                _fail(f"{where}: parent must be null or {{type, path}}.")
            if (parent["type"], parent["path"]) not in elements:
                _fail(f"{where}: parent {parent['type']}/{parent['path']} does not exist.")

        # Rule 5.
        if given is not None:
            if not isinstance(given, dict):
                _fail(f"{where}: given must be an object or null.")
            for key, value in given.items():
                if key not in columns:
                    _fail(f"{where}: given key {key!r} is not in the columns of the manifests.")
                if not isinstance(value, str) or value == "":
                    _fail(f"{where}: given {key!r} must be a non-empty string.")
            # 4.5: the name cell is the path, and the keyword is not stored.
            name_column = NAME_COLUMN.get(type_)
            # A model's features (`/:part`) are not in its path, so its name cell is kept (4.4).
            kept = type_ == "model" and "/:" in str(given.get("model", ""))
            if name_column is not None and name_column in given and not kept:
                _fail(f"{where}: given has the name cell {name_column!r}, which is the path.")
            # A `param` keeps the `type` cell it was written with, which may be the keyword or empty (4.5).
            if type_ in KEYWORD_TYPES and type_ != "param" and "type" in given:
                _fail(f"{where}: given has the dimension keyword 'type'.")

        # Rule 7: implied must be an object or null. Keys are not checked.
        if elem["implied"] is not None and not isinstance(elem["implied"], dict):
            _fail(f"{where}: implied must be an object or null.")

        # Rule 8.
        if not isinstance(elem["errors"], list):
            _fail(f"{where}: errors must be a list.")
        for i, error in enumerate(elem["errors"]):
            _check_error(f"{where}.errors[{i}]", error)

    # Rule 4, acyclic parents.
    for key in elements:
        seen = {key}
        current = key
        while True:
            parent = elements[current]["parent"]
            if parent is None:
                break
            current = (parent["type"], parent["path"])
            if current in seen:
                _fail(f"{key[0]}/{key[1]}: parents form a cycle.")
            seen.add(current)

    # Rule 6, every append names an existing target, and its `#n` is unique.
    for (type_, path), elem in elements.items():
        if type_ != "append":
            continue
        if "#" not in path:
            _fail(f"append/{path}: path must be `{{target}}#{{n}}`.")
        target = path.rsplit("#", 1)[0]
        if elem["parent"] is None or elem["parent"]["path"] != target:
            _fail(f"append/{path}: parent must be its target {target!r}.")
