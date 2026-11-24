"""`check` of a `simple` document (3.3, 4.10): link and check it, without a backend.

Each element is rebuilt from its `given` (4.7), loaded, linked and checked
under an `ErrorCollector`, so every error that can be found is collected and
none stops the others. The errors are then stored on the elements they are
about (`simple.errors`). The input document is not changed.
"""

from __future__ import annotations

import copy as _copy
from dataclasses import dataclass
from typing import Any

from spinta import commands
from spinta.cli.helpers.store import load_store
from spinta.components import Context
from spinta.core.config import RawConfig, configure_rc
from spinta.core.context import create_context
from spinta.exceptions import BaseError
from spinta.handlers import ErrorCollector
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.simple.bases import DefaultBases, link_default_bases
from spinta.manifests.simple.errors import (
    READ_ERROR_CODE,
    element_of,
    index_of_line,
    manifest_error,
    read_error,
)
from spinta.manifests.simple.helpers import read_simple_rows
from spinta.manifests.simple.implied import implied_of_document
from spinta.manifests.simple.schema import validate_document
from spinta.manifests.simple.types import MANIFEST_TYPE, SPINTA_KEY, ManifestError, SimpleManifest
from spinta.manifests.tabular.elements import Element
from spinta.manifests.tabular.helpers import TabularReadLog, _read_tabular_manifest_rows

Key = tuple[str, str]


@dataclass
class LoadedDocument:
    """A `simple` document loaded into nodes, with the errors of that run (3.4, 3.5)."""

    context: Context
    manifest: Any
    collector: ErrorCollector
    log: TabularReadLog

    @property
    def failed(self) -> bool:
        """Whether the run stopped on an error (3.5): an error that was raised, or a row that cannot be read."""
        return (
            bool(self.collector.raised)
            or bool(self.log.table.problems)
            or any(e.errors for e in self.log.table.elements)
        )


def load_document(
    base: RawConfig,
    doc: SimpleManifest,
    *,
    check_names: bool | None,
    strict_references: bool,
    rename_duplicates: bool = False,
    default_bases: DefaultBases | None = None,
) -> LoadedDocument:
    """Load, link and check `doc` into nodes, collecting every error (3.4).

    `rename_duplicates` is read as the tabular reader reads it (`copy`). The
    nodes are what `copy` renders and what `check` reports on. `default_bases`
    matches the models that have no base to the default bases, in the link
    phase, before the models are linked (`copy -b`, see `spinta.manifests.simple.bases`).
    """
    validate_document(doc)

    rc = configure_rc(base, manifest_type="inline", ensure_backends=False)
    mode = "error" if strict_references else "downgrade"
    # The store is the given document only. `configure_rc` leaves the manifests
    # of `base` in place when no manifests are given, so they are replaced here.
    # `check.names` is set here too, because `configure_rc` sets it only with manifests.
    options = {
        "manifest": "default",
        "manifests.default": {
            "type": "inline",
            "backend": "default",
            "keymap": "default",
            "mode": "internal",
            "sync": [],
            "manifest": [],
        },
        "unresolved_references": mode,
    }
    if check_names is not None:
        options["check.names"] = check_names
    rc = rc.fork(options)

    context: Context = create_context("spinta", rc)
    collector = ErrorCollector()
    manager = context.get("error_manager")
    manager.handler = collector
    manager.collect = True

    # Read the rows of `given` as the reader does, keeping read errors on their elements.
    log = TabularReadLog()
    schemas = list(
        _read_tabular_manifest_rows(None, read_simple_rows(doc), rename_duplicates=rename_duplicates, log=log)
    )

    store = load_store(context, verbose=False, check_config=False)
    manifest = store.manifest
    commands.load(context, manifest, rename_duplicates=rename_duplicates, load_internal=False, full_load=True)
    with manager.guard():
        load_manifest_nodes(context, manifest, schemas)
        if default_bases is not None:
            link_default_bases(context, manifest, default_bases)
        commands.link(context, manifest)
        commands.check(context, manifest)
    return LoadedDocument(context=context, manifest=manifest, collector=collector, log=log)


def check_document(
    base: RawConfig,
    doc: SimpleManifest,
    *,
    check_names: bool | None,
    strict_references: bool,
) -> tuple[SimpleManifest, bool]:
    """The document with its errors, and whether the run raised an error (see `LoadedDocument.failed`).

    `base` is the configuration of the call: the package defaults and the
    options of `Spinta`, or the configuration of the CLI (3.6). The manifests
    in `base` are not loaded. The flag is set when an error stopped a guarded
    block, or a row of the document could not be read. Those ended the
    command before the errors were collected, so they decide its exit status.
    """
    loaded = load_document(base, doc, check_names=check_names, strict_references=strict_references)
    checked = with_implied(_with_errors(doc, loaded.log, loaded.collector.errors), loaded)
    return checked, loaded.failed or _has_read_errors(doc, loaded)


def with_implied(doc: SimpleManifest, loaded: LoadedDocument) -> SimpleManifest:
    """`doc` with the `implied` of each element, derived from the nodes of this run (4.6).

    Changes `doc` in place and returns it. An element that has no node keeps `implied` `null`.
    """
    implied = implied_of_document(loaded.context, loaded.manifest, doc)
    for key, element in _elements_of(doc).items():
        element["implied"] = implied.get(key)
    return doc


def _with_errors(doc: SimpleManifest, log: TabularReadLog, errors: list[BaseError]) -> SimpleManifest:
    """A copy of `doc` with the errors of this run.

    Every element was rebuilt from `given`, so its read errors are the ones
    the reader found now (4.7, 4.10). The link and check errors are added to
    the element they name. Errors that name no element of the document, and
    read problems of rows that make no element, are document-level: they go
    to the `manifest` element (4.3), the first one of a merged document.
    """
    out = _copy.deepcopy(doc)
    elements = _elements_of(out)

    read: dict[Key, list[ManifestError]] = {}
    for element in log.table.elements:
        key = (element.type, element.path)
        read.setdefault(key, []).extend(_read_errors(element))

    for key, element in elements.items():
        element["errors"] = list(read.pop(key, []))

    # Read errors of elements that the document does not have are document-level too.
    doc_errors: list[ManifestError] = [read_error(p.code, p.message, index_of_line(p.line)) for p in log.table.problems]
    for errs in read.values():
        doc_errors.extend(errs)

    for error in errors:
        key = element_of(error)
        if key is not None and key in elements:
            element = elements[key]
            # An element that could not be read has no full row to link. Its read error is the one to report.
            if not element["errors"]:
                element["errors"].append(manifest_error(error, element["index"]))
        else:
            doc_errors.append(manifest_error(error))

    # A read problem of the input that this run finds again is not kept twice. Other read problems stay: their
    # source is not in the document any more. Link and check errors of the document are recomputed, so they go.
    manifests = list(out[MANIFEST_TYPE].values())
    for manifest in manifests:
        manifest["errors"] = [
            e for e in manifest["errors"] if e["code"] == READ_ERROR_CODE and not _is_duplicate(e, doc_errors)
        ]
    manifests[0]["errors"].extend(doc_errors)
    return out


def _has_read_errors(doc: SimpleManifest, loaded: LoadedDocument) -> bool:
    """Whether the document stores a read error that this run does not find again (4.10).

    A stored error can be a read error or a link and check error, and `copy` and `check` store both (4.10). The
    errors that this run finds again, from the rows of `given` or by linking, are not read errors that stopped
    anything. The others are read errors of a source that is not in the document any more: they stopped the
    command before.
    """
    found = {(e["code"], e["message"]) for e in _run_errors(loaded)}
    # A read error is always an error (`read_error`). A warning, such as `UnresolvedReference`, never is.
    return any(
        error["severity"] == "error" and (error["code"], error["message"]) not in found
        for element in _all_elements(doc)
        for error in element["errors"]
    )


def _run_errors(loaded: LoadedDocument) -> list[ManifestError]:
    """The errors of a run: the read errors that the reader found, and the link and check errors."""
    read = [
        read_error(code, message, None) for element in loaded.log.table.elements for code, message in element.errors
    ]
    read += [read_error(p.code, p.message, None) for p in loaded.log.table.problems]
    return read + [manifest_error(e) for e in loaded.collector.errors]


def _elements_of(doc: SimpleManifest) -> dict[Key, dict[str, Any]]:
    """The elements of `doc` that a row can be read to, by `(type, path)`: every element but the manifests."""
    return {
        (type_, path): element
        for type_, elements in doc.items()
        if type_ not in (SPINTA_KEY, MANIFEST_TYPE)
        for path, element in elements.items()
    }


def _all_elements(doc: SimpleManifest) -> list[dict[str, Any]]:
    return [element for type_, elements in doc.items() if type_ != SPINTA_KEY for element in elements.values()]


def _read_errors(element: Element) -> list[ManifestError]:
    return [read_error(code, message, element.index) for code, message in element.errors]


def _is_duplicate(error: ManifestError, others: list[ManifestError]) -> bool:
    return any(
        o["code"] == error["code"] and o["message"] == error["message"] and o["index"] == error["index"] for o in others
    )
