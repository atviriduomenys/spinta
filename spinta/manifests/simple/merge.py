"""Merging of `simple` documents (3.1): a list of documents is one document.

Elements of all documents are concatenated, in order, and nothing is removed
(D6). The `index` values of a later document are shifted, so that they stay
unique. The same `type` and `path` in two documents is kept once when `given`
and `implied` are equal, and raises `SimpleManifestError` otherwise. A
`manifest` element is an input file, so the same input in two documents is
kept once with the columns of both, and the errors of both.
"""

from __future__ import annotations

import copy as _copy
from typing import Any

from spinta.manifests.simple.types import (
    FORMAT_VERSION,
    MANIFEST_TYPE,
    SPINTA_KEY,
    SimpleManifest,
    SimpleManifestError,
)


def merge_documents(docs: list[SimpleManifest], *, version: str) -> SimpleManifest:
    """One document from `docs`. `version` is the running Spinta version (3.1)."""
    if not docs:
        raise ValueError("Nothing to merge: the list of documents is empty.")

    out: SimpleManifest = {
        SPINTA_KEY: {
            "version": version,
            "format": FORMAT_VERSION,
        },
    }

    for doc in docs:
        offset = _largest_index(out)
        for type_, elements in doc.items():
            if type_ == SPINTA_KEY:
                continue
            target = out.setdefault(type_, {})
            for path, element in elements.items():
                if path in target:
                    kept = target[path]
                    if type_ == MANIFEST_TYPE:
                        _join_manifest(kept, element, offset)
                        continue
                    if kept["given"] == element["given"] and kept["implied"] == element["implied"]:
                        continue
                    raise SimpleManifestError(f"{type_} {path!r} is defined in two documents with different content.")
                new = _copy.deepcopy(element)
                new["index"] = _shift(new["index"], offset)
                new["errors"] = _shifted(new["errors"], offset)
                target[path] = new

    return out


def _join_manifest(kept: dict[str, Any], other: dict[str, Any], offset: int) -> None:
    """Add the columns and the errors of `other`, the same input file, to `kept`."""
    for column in other["columns"]:
        if column not in kept["columns"]:
            kept["columns"].append(column)
    kept["errors"].extend(_shifted(other["errors"], offset))


def _shift(index: int | None, offset: int) -> int | None:
    return None if index is None else index + offset


def _shifted(errors: list[dict[str, Any]], offset: int) -> list[dict[str, Any]]:
    out = _copy.deepcopy(errors)
    for error in out:
        error["index"] = _shift(error["index"], offset)
    return out


def _largest_index(doc: SimpleManifest) -> int:
    """The largest `index` used in `doc`, so that a later document starts after it."""
    largest = 0
    for type_, elements in doc.items():
        if type_ == SPINTA_KEY:
            continue
        for element in elements.values():
            for index in [element["index"], *(e["index"] for e in element["errors"])]:
                if index is not None:
                    largest = max(largest, index)
    return largest
