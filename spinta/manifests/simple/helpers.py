"""Reading and writing the rows of a `simple` document (sections 4.7 and 4.8).

The reader rebuilds every row from its `given` cells (4.7 step 5), plus the
cells that 4.5 does not store: the name cell, which comes from the path, and
the dimension keyword `type`, which is written on the first row of a group.
`implied` is not read here: it is what Spinta derived, and link recomputes it.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

from spinta.manifests.simple.schema import document_columns, validate_document
from spinta.manifests.simple.types import (
    ELEMENT_TYPES,
    FORMAT_VERSION,
    KEYWORD_TYPES,
    MANIFEST_TYPE,
    SPINTA_KEY,
    SimpleManifest,
    SimpleManifestError,
)
from spinta.manifests.tabular.elements import escape


def canonical_json(doc: SimpleManifest) -> str:
    """Canonical JSON of a document (4.1): sorted keys, UTF-8, no extra space."""
    return json.dumps(doc, ensure_ascii=False, sort_keys=True, separators=(",", ": "), indent=2)


def check_format(doc: SimpleManifest) -> None:
    """Refuse a document written by a newer format than this reader (4.7 step 2)."""
    fmt = doc[SPINTA_KEY]["format"]
    if fmt > FORMAT_VERSION:
        raise SimpleManifestError(f"`spinta.format` {fmt} is newer than this Spinta reads ({FORMAT_VERSION}).")


def _relative(path: str, base: str | None) -> str:
    """`path` without the `base` path and its `/` (the whole path if no base)."""
    if base and path.startswith(base + "/"):
        return path[len(base) + 1 :]
    return path


def _ancestor_path(type_: str, elem: dict[str, Any], elements: dict[tuple[str, str], dict]) -> str | None:
    """Path of the nearest ancestor of type `type_`, following `parent`."""
    current = elem
    while current is not None and current["parent"] is not None:
        parent = current["parent"]
        if parent["type"] == type_:
            return parent["path"]
        current = elements.get((parent["type"], parent["path"]))
    return None


def _owner(elem: dict[str, Any]) -> dict[str, str] | None:
    """The element an element belongs to, or `None` at the top level.

    A top level element points to its `manifest` (4.2). That is not an owner, so it is `None` here.
    """
    parent = elem["parent"]
    if parent is None or parent["type"] == MANIFEST_TYPE:
        return None
    return parent


def _parent_path(elem: dict[str, Any]) -> str | None:
    owner = _owner(elem)
    return owner["path"] if owner is not None else None


def _enum_name(path: str, owner: str | None, given: dict[str, str]) -> str:
    """The enum an item belongs to (4.3): the path without its owner and its item."""
    item = escape(given.get("source") or given.get("prepare") or "")
    rel = _relative(path, owner)
    if rel == item:
        return ""  # an unnamed enum: the item is the only segment
    if rel.endswith("/" + item):
        return rel[: -len(item) - 1]
    return rel


def _dataset_of(type_: str, path: str, elem: dict[str, Any], elements: dict[tuple[str, str], dict]) -> str | None:
    """The dataset an element is in: its own path for a dataset, else its nearest dataset ancestor."""
    if type_ in ("dataset", "ns"):
        return path
    return _ancestor_path("dataset", elem, elements)


def _name_cell(
    type_: str,
    path: str,
    elem: dict[str, Any],
    elements: dict[tuple[str, str], dict],
    first: bool,
    enum: str,
) -> tuple[str, str] | None:
    """The name cell of a row, as `(column, value)`, or None (4.8 step 2).

    `first` is True for the first row of an enum group: only that row has the
    enum name. A `unique` row has no name cell, because its `ref` is in `given`.
    """
    if type_ in ("dataset", "ns"):
        return ("dataset", path)
    if type_ == "backend":
        return ("resource", path)
    if type_ == "resource":
        return ("resource", _relative(path, _ancestor_path("dataset", elem, elements)))
    if type_ == "base":
        # A repeated base name has a `#n` suffix in its path (4.3).
        return ("base", _relative(path, _ancestor_path("dataset", elem, elements)).split("#", 1)[0])
    if type_ == "model":
        return ("model", _relative(path, _ancestor_path("dataset", elem, elements)))
    if type_ == "property":
        return ("property", _relative(path, _ancestor_path("model", elem, elements)))
    if type_ == "enum":
        return ("ref", enum) if first and enum else None
    if type_ in ("prefix", "param", "lang", "scope"):
        return ("ref", _relative(path, _parent_path(elem)))
    return None


# Extra dimensions whose rows may continue the row above with an empty `type` (4.3). The reader registers a
# continuation of these. The others keep their keyword on every row: a `ns` row with a `dataset` cell is a
# dataset, the reader does not register a continuation of a `scope`, `unique` or `comment` (it would read
# an update, or drop the row), and a `param` keeps the `type` cell it was written with.
GROUPED_TYPES = frozenset({"prefix", "enum", "lang"})


def _group_starts(rows: list[tuple[int, str, str | None, str, dict]]) -> dict[int, bool]:
    """Whether each row starts a group of an extra dimension (4.8 step 3).

    A group is consecutive rows of the same type under the same owner, and for
    an enum the same enum. Its first row has the keyword in `type`. A row
    that repeats the keyword in the source is written as a continuation: the
    reader reads both the same way, but the cells differ (4.5).
    """
    starts: dict[int, bool] = {}
    previous: tuple | None = None
    for index, type_, owner, enum, _ in rows:
        if type_ not in GROUPED_TYPES:
            starts[index] = type_ in KEYWORD_TYPES
            previous = None
            continue
        group = (type_, owner, enum if type_ == "enum" else None)
        starts[index] = group != previous
        previous = group
    return starts


def read_simple_rows(doc: SimpleManifest, dsa_columns=None) -> Iterator[tuple[str, list[str]]]:
    """Yield `(line, cells)` rows of a `simple` document, in `index` order.

    The first pair is the header (the columns of the manifests, 4.3), at line `"1"`. Then each
    element with an `index` is one row, at line `index + 1`, the record number
    of a CSV file (4.2: `index + 1` is the `eid` of the reader). Elements
    without `index` are implied and have no row.

    The document is validated first (4.9), so a malformed document raises
    `SimpleManifestError` before anything is read.
    """
    validate_document(doc, dsa_columns)
    columns = document_columns(doc)
    elements = {(type_, path): elem for type_ in ELEMENT_TYPES for path, elem in doc.get(type_, {}).items()}

    # (index, type, owner path, enum name, element), in row order.
    rows: list[tuple[int, str, str | None, str, dict]] = []
    for (type_, path), elem in elements.items():
        if elem["index"] is None:
            continue
        owner = _parent_path(elem)
        enum = _enum_name(path, owner, elem["given"]) if type_ == "enum" else ""
        rows.append((elem["index"], type_, owner, enum, (path, elem)))
    rows.sort(key=lambda r: r[0])

    starts = _group_starts([(i, t, o, e, None) for i, t, o, e, _ in rows])

    yield "1", columns
    last = 1  # the label of the last row written: the header
    open_dataset = None  # the dataset the reader has open (4.8 step 4)
    open_resource = None  # the resource the reader has open: a model inherits it (4.3, 4.8 step 4)
    open_base = None  # the base the reader has open: a model inherits it (4.3, 4.8 step 4)
    for index, type_, _owner, enum, (path, elem) in rows:
        cells = dict(elem["given"])
        first = starts.get(index, False)
        name = _name_cell(type_, path, elem, elements, first, enum)
        if name is not None and name[0] in columns and name[0] not in cells:
            cells[name[0]] = name[1]
        if first and type_ in KEYWORD_TYPES and type_ != "param" and "type" in columns:
            # A `param` keeps the `type` cell it was written with (4.5), so it is not set here.
            cells["type"] = type_

        dataset = _dataset_of(type_, path, elem, elements)
        if dataset is not None:
            open_dataset = dataset
        elif open_dataset is not None and "dataset" in columns and _needs_closer(type_, elem, cells):
            # Scope closer (4.3, 4.8 step 4): the reader would attach this element
            # to the open dataset, unless a `/` row closes it first.
            yield f"{last + 1}/", ["/" if column == "dataset" else "" for column in columns]
            open_dataset = None
            open_base = None
        if type_ == "dataset":
            open_resource = None
        if type_ in ("dataset", "resource"):
            # A dataset or a resource row closes the base the reader has open.
            open_base = None
        if type_ == "resource":
            open_resource = path
        if type_ == "model" and open_resource is not None and elem.get("resource") != open_resource:
            # A `/` in `resource` closes the open resource and its base, so this model is not read into them.
            yield f"{last + 1}/", ["/" if column == "resource" else "" for column in columns]
            open_resource = None
            open_base = None
        if type_ == "model" and open_base is not None and elem.get("base") != open_base:
            # A `/` in `base` closes the open base, so this model is not read into it.
            yield f"{last + 1}/", ["/" if column == "base" else "" for column in columns]
            open_base = None

        last = index + 1
        yield str(last), [cells.get(column, "") for column in columns]
        if type_ == "base":
            open_base = path


def _needs_closer(type_: str, elem: dict[str, Any], cells: dict[str, str]) -> bool:
    """Whether the reader would attach this row to the dataset that is open (4.8 step 4).

    A backend or a base with no dataset, a model with a simple name (no `/`),
    and a prefix, enum or param with no parent are read into the open dataset.
    A property, a scope, or a model with a qualified name is not: the reader
    attaches it to its model, or reads its name as it is.
    """
    if type_ in ("backend", "base"):
        return True
    if type_ == "model":
        return "/" not in cells.get("model", "")
    if type_ in ("prefix", "enum", "param"):
        return _owner(elem) is None
    return False


def simple_to_rows(doc: SimpleManifest) -> list[dict[str, str]]:
    """Rows of a `simple` document as `{column: cell}` dicts, in `index` order.

    Used by tests and by the round trip (4.8). Blank and `/` rows are not
    regenerated here: the writer does that from the loaded nodes.
    """
    columns = document_columns(doc)
    return [dict(zip(columns, cells)) for _line, cells in read_simple_rows(doc)][1:]
