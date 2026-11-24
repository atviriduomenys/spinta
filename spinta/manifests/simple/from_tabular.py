"""Build a `simple` document from the rows of a table (3.1, 4.2 to 4.5, 4.10).

The reader runs as it does for a manifest, with a log. Each row becomes an
element (4.3), with its `index` and its `given` cells, and each read error
is stored on the element of its row. The input file is a `manifest` element
(4.3): its `columns` are the header, and the errors that belong to no row
are stored on it. A table with no readable header gives a document with
only that element, with the error on it.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

from spinta.manifests.simple.errors import index_of_line, read_error
from spinta.manifests.simple.types import (
    FORMAT_VERSION,
    MANIFEST_TYPE,
    MEMORY_SOURCE,
    SPINTA_KEY,
    SimpleManifest,
)
from spinta.manifests.tabular.elements import Element, Problem
from spinta.manifests.tabular.helpers import TabularManifestError, TabularReadLog, _read_tabular_manifest_rows


def _element_dict(element: Element, source: str) -> dict[str, Any]:
    parent = element.parent
    # A top level element has no parent element, so it points to its input file (4.2).
    ref = {"type": parent.type, "path": parent.path} if parent is not None else {"type": MANIFEST_TYPE, "path": source}
    out: dict[str, Any] = {
        "index": element.index,
        "parent": ref,
        "given": element.given,
        "implied": None,  # the reader does not compute it: `copy` and `check` do, from the loaded nodes (4.6)
        "errors": [read_error(code, message, element.index, context) for code, message, context in element.errors],
    }
    if element.type == "model":
        # The resource and base scope the row was read in (4.2). Not trusted on read: they only place the
        # `/` closers of 4.8.
        out["resource"] = element.resource
        out["base"] = element.base
    return out


def _document(
    *,
    version: str,
    type_: str,
    source: str,
    columns: list[str],
    elements: Iterable[Element],
    problems: Iterable[Problem],
) -> SimpleManifest:
    doc: SimpleManifest = {
        SPINTA_KEY: {
            "version": version,
            "format": FORMAT_VERSION,
        },
        MANIFEST_TYPE: {
            source: {
                "index": None,
                "parent": None,
                "given": None,
                "implied": None,
                "errors": [read_error(p.code, p.message, index_of_line(p.line)) for p in problems],
                "type": type_,
                "source": source,
                "columns": list(columns),
            },
        },
    }
    for element in elements:
        doc.setdefault(element.type, {})[element.path] = _element_dict(element, source)
    return doc


def tabular_to_simple(
    rows: Iterator[tuple[str, list[str]]],
    *,
    type: str,
    version: str,
    source: str | None = None,
    rename_duplicates: bool = False,
) -> SimpleManifest:
    """A `simple` document of the rows of a table.

    `rows` yields `(line, cells)` pairs, the header first, as the tabular
    readers do. `type` is the format the rows were read from (`csv`, `ascii`,
    ...), and `source` is the path of the input file (`manifest.source`, 4.3),
    `<memory>` when there is none. A path is also given to the reader, for the
    location of its errors.
    """
    path = source
    source = source if source is not None else MEMORY_SOURCE
    log = TabularReadLog()
    try:
        for _ in _read_tabular_manifest_rows(path, rows, rename_duplicates=rename_duplicates, log=log):
            pass
    except TabularManifestError as e:
        # No readable header: nothing else can be read (4.10).
        return _document(
            version=version,
            type_=type,
            source=source,
            columns=[],
            elements=[],
            problems=[Problem(line=str(e.line or ""), message=str(e))],
        )
    table = log.table
    return _document(
        version=version,
        type_=type,
        source=source,
        columns=table.columns,
        elements=table.elements,
        problems=table.problems,
    )
