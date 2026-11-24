"""Public API of Spinta as a library (section 3 of `.tmp/specs/exchange.md`).

Other packages import only from this module. Every name they need is in
`__all__`. Everything else under `spinta` is internal.

Implemented so far: `Spinta.version`, `Spinta.copy` (CSV, ASCII and XLSX
sources to `simple`; `simple` to `simple`, `csv`, `xlsx`, `ascii`, `mermaid`
and `rdf`), `Spinta.check` (link and check, without a backend) and
`Spinta.errors`. Rendered outputs are built from the loaded nodes, with the
same writers as `spinta copy` (D14). An option that an output does not take
raises `ValueError`, and an option not implemented yet raises
`NotImplementedError`, so a caller never gets a silently ignored option.
"""

from __future__ import annotations

import csv
import io
import json
import os
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import IO, Any, Literal, get_args

from spinta.core.config import Path as ConfigPath
from spinta.core.config import RawConfig
from spinta.core.enums import Access
from spinta.exceptions import BaseError
from spinta.manifests.mermaid.helpers import write_mermaid_manifest
from spinta.manifests.rdf.writer import render_rdf_manifest
from spinta.manifests.simple.bases import DefaultBases, default_models, visibility_scope
from spinta.manifests.simple.check import LoadedDocument, _with_errors, check_document, load_document, with_implied
from spinta.manifests.simple.errors import index_of_line, manifest_error, read_error
from spinta.manifests.simple.from_tabular import tabular_to_simple
from spinta.manifests.simple.helpers import check_format
from spinta.manifests.simple.merge import merge_documents
from spinta.manifests.simple.schema import validate_document
from spinta.manifests.simple.types import (
    ELEMENT_TYPES,
    FORMAT_VERSION,
    SPINTA_KEY,
    ElementRef,
    ManifestError,
    SimpleManifest,
    SimpleManifestError,
)
from spinta.manifests.tabular.constants import DATASET
from spinta.manifests.tabular.helpers import (
    TabularManifestError,
    _read_ascii_tabular_manifest,
    _read_tabular_manifest_rows,
    _read_xlsx_manifest,
    datasets_to_tabular,
    normalizes_columns,
    render_tabular_manifest_rows,
    write_xlsx,
)
from spinta.naming.helpers import reformat_names
from spinta.utils.enums import get_enum_by_name

ReadFormat = Literal["csv", "ascii", "xlsx", "gsheets", "xml", "json", "xsd", "rdf", "sql", "openapi", "simple"]
WriteFormat = Literal["csv", "ascii", "xlsx", "mermaid", "rdf", "simple"]
RawSource = str | os.PathLike | bytes | IO
Manifests = SimpleManifest | Sequence[SimpleManifest]

__all__ = [
    "ELEMENT_TYPES",
    "FORMAT_VERSION",
    "ElementRef",
    "ManifestError",
    "Manifests",
    "RawSource",
    "ReadFormat",
    "SimpleManifest",
    "SimpleManifestError",
    "Spinta",
    "WriteFormat",
]

# Formats that are implemented. The rest are refused.
_IMPLEMENTED_READ = frozenset({"csv", "ascii", "xlsx", "simple"})

# The options of 3.1 that each output takes. Any other value of an option raises.
_OUTPUT_OPTIONS: dict[str, frozenset[str]] = {
    "ascii": frozenset({"source", "access", "columns", "order_by", "format_names", "rename_duplicates"}),
    "csv": frozenset({"source", "access", "order_by", "format_names", "rename_duplicates"}),
    "xlsx": frozenset({"source", "access", "order_by", "format_names", "rename_duplicates"}),
    "mermaid": frozenset({"dataset", "format_names", "rename_duplicates"}),
    "rdf": frozenset({"access", "dataset", "format_names", "rename_duplicates"}),
    "simple": frozenset(),
}
_RENAMES = frozenset({"format_names", "rename_duplicates"})
_DEFAULTS: dict[str, Any] = {
    "source": True,
    "access": "private",
    "dataset": None,
    "columns": None,
    "order_by": None,
    "format_names": False,
    "rename_duplicates": False,
}


@dataclass
class _CopyRun:
    """The result of a copy, and the errors it reports (3.5).

    `output` is what `copy` returns, `None` when a table could not be written.
    `reports` are the errors of the run, in order, each with whether it stopped
    the run. `stop` is the exception of that error: `copy` raises it when there
    is no `on_error` and nothing was written.
    """

    output: Any
    reports: list[tuple[ManifestError, bool]]
    stop: Exception | None


class Spinta:
    """Spinta with its configuration and one context.

    Options are config keys, given by the caller only. No file, `.env` or
    environment variable is read (D15). No option is supported yet, so any
    option raises `ValueError`; `config` names files and is always refused.

    One instance is safe for threads: `copy` and `errors` take `self._lock`,
    so the calls run one after another.
    """

    def __init__(self, **options: Any) -> None:
        if "config" in options:
            raise ValueError("`config` names files, which only the CLI reads (3.6). Pass config keys instead.")
        if options:
            names = ", ".join(sorted(options))
            raise ValueError(f"Unknown option(s): {names}. No option is supported yet.")
        self._lock = threading.RLock()
        self._rc = _package_config()

    @classmethod
    def _from_context(cls, context: Any) -> Spinta:
        """A `Spinta` with the configuration of a CLI context (3.6, point 6).

        Not in `__all__`: only the CLI uses it. It keeps the configuration of
        `context` (`.env`, `spinta.yml`, `SPINTA_*`), but not its logging. The
        CLI's own config load sets logging up, once per run.
        """
        spinta = cls.__new__(cls)
        spinta._lock = threading.RLock()
        spinta._rc = context.get("rc").fork({"setup_logging": False})
        return spinta

    @property
    def version(self) -> str:
        """Running Spinta version, as `spinta --version` prints it."""
        import spinta

        return spinta.__version__

    def copy(
        self,
        manifest: Manifests | RawSource,
        format: ReadFormat | WriteFormat | None = None,
        *,
        source: bool = True,
        access: str = "private",
        dataset: str | None = None,
        columns: Sequence[str] | None = None,
        order_by: str | None = None,
        include: Sequence[SimpleManifest] = (),
        base: Sequence[SimpleManifest | str | os.PathLike] = (),
        base_visibility: Sequence[str] | None = None,
        format_names: bool = False,
        rename_duplicates: bool = False,
        on_error: Callable[[ManifestError, bool], None] | None = None,
    ) -> SimpleManifest | str | bytes | None:
        """Read a raw source to `simple`, or write a `simple` document (3.1).

        A raw source (a path, bytes or an open file) is always read to a
        `simple` manifest. A `simple` manifest, or a list of them, is written
        in `format`, `simple` by default. A list is merged first.

        `base` is a list of default base manifests, paths or `simple` documents
        (`spinta copy -b`). A model that has no base is matched to the one model
        of the same name, without its dataset, in them: the match is written as
        the model's base. No match, or more than one, is a `DefaultBaseNotFound`
        error of the model, and the model is written without a base. `base_visibility`
        names the visibilities (`public`, `package`, `protected`, `private`) of the
        models that must match; the others get no error. `None` is all of them.
        The base manifests are only read to build the match, and are then released.

        A value that a model or a property does not set (`status`, `visibility`,
        `access`, `uri`, `eli`, `title`, `description`) is inherited from its
        nearest base that sets it, see `spinta.manifests.inherit`. Datasets and
        resources are not inherited. A `simple` result keeps
        `given` and fills in `implied`; a table, RDF or Mermaid result writes the
        inherited values as if they were given.

        Errors do not raise. Each error of the run is passed to `on_error(error,
        raised)`, in order: the errors stored in a `simple` result, or the errors
        found while writing a table. `raised` is true for the error that stopped
        a table (a row that cannot be read, a link error). Then nothing is
        written and the result is `None`. Without `on_error` that error is raised
        instead, as `spinta copy` did (3.5, rule 4).
        """
        run = self._copy_run(
            manifest,
            format,
            source=source,
            access=access,
            dataset=dataset,
            columns=columns,
            order_by=order_by,
            include=include,
            base=base,
            base_visibility=base_visibility,
            format_names=format_names,
            rename_duplicates=rename_duplicates,
        )
        return _report(run, on_error)

    def _copy_run(
        self,
        manifest: Manifests | RawSource,
        format: str | None,
        *,
        source: bool,
        access: str | Access,
        dataset: str | None,
        columns: Sequence[str] | None,
        order_by: str | None,
        include: Sequence[SimpleManifest],
        base: Sequence[SimpleManifest | str | os.PathLike],
        base_visibility: Sequence[str] | None,
        format_names: bool,
        rename_duplicates: bool,
    ) -> _CopyRun:
        """`copy`, with its errors and the error that stopped it (see `_CopyRun`)."""
        with self._lock:
            if include:
                raise NotImplementedError("`include` is not implemented yet.")
            options = _copy_options(source, access, dataset, columns, order_by, format_names, rename_duplicates)
            if not _is_document(manifest):
                # A raw source is read to `simple`. Renaming happens in the read, so it is allowed (3.1).
                _refuse_output_options("simple", {**options, "rename_duplicates": False})
                bases = self._default_bases(base, base_visibility)
                doc, stop = self._read_with_stop(manifest, format, rename_duplicates=rename_duplicates)
                return self._simple_run(doc, stop, bases)
            fmt = format or "simple"
            # The output is checked before any base is read, so a refused option raises before anything is loaded.
            _check_output_format(fmt)
            _refuse_output_options(fmt, options)
            docs = _checked_documents(manifest)
            bases = self._default_bases(base, base_visibility)
            return self._write_docs(docs, fmt, options, stop=None, bases=bases)

    def _copy_files(
        self,
        files: Sequence[tuple[str, str | None]],
        format: str,
        *,
        source: bool = True,
        access: str = "private",
        dataset: str | None = None,
        columns: Sequence[str] | None = None,
        order_by: str | None = None,
        format_names: bool = False,
        rename_duplicates: bool = False,
        base: Sequence[SimpleManifest | str | os.PathLike] = (),
        base_visibility: Sequence[str] | None = None,
        on_error: Callable[[ManifestError, bool], None] | None = None,
    ) -> Any:
        """`spinta copy FILE... -o OUTPUT`: each file is read to `simple` (3.5, rule 2), then the documents are written.

        `files` are `(path, input format)` pairs; a `None` format is detected from the path. A row that cannot
        be read stops a table output before anything is written, so its `output` is `None` (3.5, rule 4). A
        `simple` output keeps every element (4.10), so it is written with its errors. `base` is the `-b`
        default bases, see `copy`.
        """
        with self._lock:
            options = _copy_checked(
                format,
                source=source,
                access=access,
                dataset=dataset,
                columns=columns,
                order_by=order_by,
                format_names=format_names,
                rename_duplicates=rename_duplicates,
            )
            bases = self._default_bases(base, base_visibility)
            reads = [self._read_with_stop(path, fmt, rename_duplicates=rename_duplicates) for path, fmt in files]
            stop = next((error for _, error in reads if error is not None), None)
            if stop is not None and format != "simple":
                return _report(_CopyRun(None, [(_as_error(stop), True)], stop), on_error)
            docs = [doc for doc, _ in reads]
            return _report(self._write_docs(docs, format, options, stop, bases=bases), on_error)

    def _write_docs(
        self,
        docs: list[SimpleManifest],
        fmt: str,
        options: dict[str, Any],
        stop: Exception | None,
        bases: DefaultBases | None = None,
    ) -> _CopyRun:
        """Write `docs` as one document (3.1), in `fmt`. `options` are checked by `_refuse_output_options`."""
        _check_output_format(fmt)
        _refuse_output_options(fmt, options)
        doc = docs[0] if len(docs) == 1 else merge_documents(docs, version=self.version)
        if fmt == "simple":
            return self._simple_run(doc, stop, bases)
        return self._render(doc, fmt, bases, **{name: options[name] for name in _RENDER_OPTIONS})

    def _default_bases(
        self,
        base: Sequence[SimpleManifest | str | os.PathLike],
        base_visibility: Sequence[str] | None,
    ) -> DefaultBases | None:
        """The index of the default bases (`copy -b`), or `None` when there are none (3.1).

        Each base manifest is loaded only to list its models, in a context of its own. The index keeps
        names and keys, so the contexts are released once it is built.
        """
        if base is None:
            base = ()
        if isinstance(base, str | bytes | os.PathLike | dict) or not isinstance(base, Sequence):
            raise ValueError("`base` takes a list of manifests, not one manifest (3.1).")
        bases = list(base)
        if not bases:
            if base_visibility is not None:
                raise ValueError("`base_visibility` needs `base`: name the default base manifests too (3.1).")
            return None
        index = DefaultBases(scope=visibility_scope(base_visibility))
        for item in bases:
            loaded = load_document(self._rc, self._base_document(item), check_names=None, strict_references=False)
            index.add(default_models(loaded.context, loaded.manifest))
        return index

    def _base_document(self, item: SimpleManifest | str | os.PathLike) -> SimpleManifest:
        """A default base given as a path, read by its format, or as a `simple` document (3.1)."""
        if isinstance(item, dict):
            return _checked_documents(item)[0]
        if isinstance(item, str | os.PathLike):
            doc, _ = self._read_with_stop(item, None)
            return doc
        raise ValueError(f"A default base is a path or a `simple` document, not {type(item).__name__} (3.1).")

    def _simple_run(self, doc: SimpleManifest, stop: Exception | None, bases: DefaultBases | None = None) -> _CopyRun:
        """A `simple` result: `doc` with the link errors of this run stored on its elements (3.4, 4.10).

        The document is loaded as `check` loads it, without `strict_references`. So a reference that is not in
        the document is downgraded, and its element gets an `UnresolvedReference` warning, as the table outputs
        report it. The read errors are kept. A read that stopped does not remove the elements: the output keeps
        every element (4.10), so `stop` only marks the error that stopped the read.
        """
        loaded = load_document(
            self._rc,
            doc,
            check_names=None,
            strict_references=False,
            default_bases=bases,
        )
        out = with_implied(_with_errors(doc, loaded.log, loaded.collector.errors), loaded)
        return _CopyRun(out, _simple_reports(out, stop), stop)

    def _render(
        self,
        doc: SimpleManifest,
        fmt: str,
        bases: DefaultBases | None = None,
        *,
        source: bool,
        access: str,
        dataset: str | None,
        columns: Sequence[str] | None,
        order_by: str | None,
        format_names: bool,
        rename_duplicates: bool,
    ) -> _CopyRun:
        """Render `doc` from its nodes, with the writers that `spinta copy` uses (D14).

        The document is loaded as `check` loads it, with downgraded references
        (3.4). Names are not checked when they are renamed, as the CLI does.
        """
        loaded = load_document(
            self._rc,
            doc,
            check_names=False if (format_names or rename_duplicates) else None,
            strict_references=False,
            rename_duplicates=rename_duplicates,
            default_bases=bases,
        )
        stop = _load_stop(loaded)
        if stop is not None:
            # The nodes are only partly linked, so they cannot be written. The error that stopped the run is
            # returned, as the CLI raised it before (3.5).
            return _CopyRun(None, [(_as_error(stop), True)], stop)

        context, manifest = loaded.context, loaded.manifest
        if format_names:
            reformat_names(context, manifest)

        acc = get_enum_by_name(Access, access)
        if fmt in ("csv", "xlsx", "ascii"):
            rows = list(datasets_to_tabular(context, manifest, external=source, access=acc, order_by=order_by))
            if fmt == "ascii":
                cols = normalizes_columns(list(columns)) if columns else None
                output: Any = render_tabular_manifest_rows(rows, cols)
            elif fmt == "csv":
                output = _csv_bytes(rows)
            else:
                output = _xlsx_bytes(rows)
        elif fmt == "mermaid":
            output = write_mermaid_manifest(context, manifest, dataset, None)
        else:
            output = render_rdf_manifest(context, manifest, access=acc, dataset=dataset)
        return _CopyRun(output, [(manifest_error(e), False) for e in loaded.collector.errors], None)

    def check(
        self,
        manifest: Manifests,
        *,
        check_names: bool | None = None,
        strict_references: bool = True,
    ) -> SimpleManifest:
        """Link and check a `simple` document, without a backend (3.3).

        Returns the document, a new dict with the errors of each element set.
        The errors that belong to no element are on its `manifest` (4.3). The
        input is not changed. Each call loads the document into a new context,
        so calls do not see each other's nodes.
        A list is checked as one document: it is merged first (3.1).
        `check_names=None` keeps the configured value. `strict_references`
        makes an unresolved reference an error; otherwise it is a warning (3.4).
        """
        doc, _ = self._check(manifest, check_names=check_names, strict_references=strict_references)
        return doc

    def _check(
        self,
        manifest: Manifests,
        *,
        check_names: bool | None = None,
        strict_references: bool = True,
    ) -> tuple[SimpleManifest, bool]:
        """`check`, and whether the run raised an error (see `check_document`). The CLI uses the flag for its
        exit status (3.5)."""
        with self._lock:
            docs = _checked_documents(manifest)
            doc = docs[0] if len(docs) == 1 else merge_documents(docs, version=self.version)
            return check_document(
                self._rc,
                doc,
                check_names=check_names,
                strict_references=strict_references,
            )

    def errors(
        self,
        manifest: Manifests,
        code: str | Sequence[str] | None = None,
    ) -> list[ManifestError]:
        """The errors that a `simple` document stores (3.3, 4.10).

        The errors of every element come in document order (4.1). The errors
        that belong to no element are on its `manifest`. Nothing is linked or
        checked.
        """
        with self._lock:
            docs = _documents(manifest)
            for doc in docs:
                validate_document(doc)
            wanted = _codes(code)
            found: list[ManifestError] = []
            for doc in docs:
                found.extend(_stored_errors(doc))
            if wanted is not None:
                found = [e for e in found if e["code"] in wanted]
            return found

    def _read_with_stop(
        self,
        source: RawSource,
        format: str | None,
        *,
        rename_duplicates: bool = False,
    ) -> tuple[SimpleManifest, Exception | None]:
        """A raw source read to `simple`, and the error that stopped the read, if any (3.5).

        A table row that cannot be read is stored in the document, so the read
        goes on. The CLI stops at it, as it did before: the first error the
        reader raises is returned, with the same type and message.
        """
        # A path is opened by the reader. Bytes and open files are read here:
        # `text` for the text formats, `data` for XLSX.
        text: str | None = None
        data: bytes | None = None
        if isinstance(source, str | os.PathLike):
            name = os.fspath(source)
            fmt = format or _detect_format(name)
        elif isinstance(source, bytes | bytearray) or hasattr(source, "read"):
            if format is None:
                raise ValueError("`format` is required for bytes and open files (3.1).")
            name = "<memory>"
            fmt = format
            raw = source if isinstance(source, bytes | bytearray) else source.read()
            if isinstance(raw, str):
                text = raw
            elif fmt == "xlsx":
                data = bytes(raw)
            else:
                text = bytes(raw).decode("utf-8-sig")
        else:
            raise ValueError(f"Unsupported source {type(source).__name__}; expected a path, bytes or an open file.")

        _check_input_format(fmt)

        if fmt == "simple":
            if text is None:
                with open(name, encoding="utf-8") as f:
                    text = f.read()
            doc = _parse_json(text, name)
            check_format(doc)
            validate_document(doc)
            return doc, None

        doc = tabular_to_simple(
            _tabular_rows(fmt, name, text, data),
            type=fmt,
            version=self.version,
            source=name,
            rename_duplicates=rename_duplicates,
        )
        if not _stored_errors(doc):
            return doc, None
        return doc, _first_read_error(fmt, name, text, data, rename_duplicates=rename_duplicates)


def _package_config() -> RawConfig:
    """The package defaults, with logging off and an in-memory keymap (3.6).

    Only `spinta/config.py` is read. No config file, `.env` or environment
    variable, and no directory is created.
    """
    rc = RawConfig()
    rc.read([ConfigPath("spinta", "spinta.config:CONFIG")])
    return rc.fork(
        {
            "setup_logging": False,
            "keymaps.default": {"type": "sqlalchemy", "dsn": "sqlite://"},
        }
    )


def _is_document(manifest: Any) -> bool:
    return isinstance(manifest, dict | list | tuple)


def _documents(manifest: Any) -> list[SimpleManifest]:
    if isinstance(manifest, dict):
        return [manifest]
    if isinstance(manifest, list | tuple):
        return list(manifest)
    raise ValueError("Expected a simple manifest or a list of them (3.1).")


def _checked_documents(manifest: Any) -> list[SimpleManifest]:
    docs = _documents(manifest)
    if len(docs) == 0:
        raise ValueError("An empty list of manifests has nothing to check.")
    for doc in docs:
        validate_document(doc)
        check_format(doc)
    return docs


def _stored_errors(doc: SimpleManifest) -> list[ManifestError]:
    """The errors of every element of `doc`, in document order (4.10). A manifest holds the errors of its input."""
    found: list[ManifestError] = []
    for type_, elements in doc.items():
        if type_ != SPINTA_KEY:
            for element in elements.values():
                found.extend(element["errors"])
    return found


def _tabular_rows(fmt: str, name: str, text: str | None, data: bytes | None):
    if fmt == "xlsx":
        return _xlsx_rows(name, data, text)
    if fmt == "csv":
        return _csv_rows(name, text)
    return _ascii_rows(name, text)


def _first_read_error(
    fmt: str,
    name: str,
    text: str | None,
    data: bytes | None,
    *,
    rename_duplicates: bool,
) -> Exception | None:
    """The error that the reader raises for the first bad row, as it raised them before `log` (3.5)."""
    try:
        for _ in _read_tabular_manifest_rows(
            name, _tabular_rows(fmt, name, text, data), rename_duplicates=rename_duplicates
        ):
            pass
    except Exception as e:
        return e
    return None


def _load_stop(loaded: LoadedDocument) -> Exception | None:
    """The error that stopped a rendered run (3.5). Read errors come first, because rows are read first."""
    table = loaded.log.table
    if table.problems:
        problem = table.problems[0]
        return TabularManifestError(problem.message, line=problem.line)
    for element in table.elements:
        if element.errors:
            return TabularManifestError(element.errors[0][1])
    if loaded.collector.raised:
        return loaded.collector.raised[0]
    return None


def _csv_bytes(rows: list) -> bytes:
    """The table as `spinta copy -o FILE.csv` writes it: UTF-8, no BOM."""
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=DATASET)
    writer.writeheader()
    writer.writerows({c: row[c] for c in DATASET} for row in rows)
    return buffer.getvalue().encode("utf-8")


def _xlsx_bytes(rows: list) -> bytes:
    buffer = io.BytesIO()
    write_xlsx(buffer, ({c: row[c] for c in DATASET} for row in rows), DATASET)
    return buffer.getvalue()


def _codes(code: str | Sequence[str] | None) -> set[str] | None:
    if code is None:
        return None
    if isinstance(code, str):
        return {code}
    return set(code)


_RENDER_OPTIONS = ("source", "access", "dataset", "columns", "order_by", "format_names", "rename_duplicates")


def _copy_options(
    source: bool,
    access: str | Access,
    dataset: str | None,
    columns: Sequence[str] | None,
    order_by: str | None,
    format_names: bool,
    rename_duplicates: bool,
) -> dict[str, Any]:
    """The options of `copy`, by name. An `Access` is given by its name."""
    if isinstance(access, Access):
        access = access.name
    return {
        "source": source,
        "access": access,
        "dataset": dataset,
        "columns": columns,
        "order_by": order_by,
        "format_names": format_names,
        "rename_duplicates": rename_duplicates,
    }


def _copy_checked(format: str, **options: Any) -> dict[str, Any]:
    """The options of a copy to `format`, checked before anything is read or written (3.1)."""
    _check_output_format(format)
    checked = _copy_options(**options)
    _refuse_output_options(format, checked)
    return checked


def _refuse_output_options(format: str, options: dict[str, Any]) -> None:
    """An option that the output does not take raises, it is never ignored (3.1). `_OUTPUT_OPTIONS` says which."""
    for name, value in options.items():
        if value == _DEFAULTS[name] or name in _OUTPUT_OPTIONS[format]:
            continue
        if format == "simple" and name in _RENAMES:
            raise NotImplementedError(f"`{name}` is not implemented yet for `simple`.")
        if format == "simple":
            raise ValueError(f"`{name}` does not apply to a `simple` result (3.1).")
        raise ValueError(f"`{name}` does not apply to `{format}` output (3.1).")


def _as_error(error: Exception) -> ManifestError:
    """The `ManifestError` of an error that stopped a run (3.5). A row that cannot be read is a read error."""
    if isinstance(error, BaseError):
        return manifest_error(error)
    return read_error(type(error).__name__, str(error), index_of_line(getattr(error, "line", None)))


def _simple_reports(doc: SimpleManifest, stop: Exception | None) -> list[tuple[ManifestError, bool]]:
    """The stored errors of a `simple` result, and which one stopped the read (3.5, rule 4).

    The reader stops at the first row that cannot be read: that is the stored error with the lowest index.
    """
    errors = _stored_errors(doc)
    first = min(errors, key=lambda e: (e["index"] is None, e["index"] or 0), default=None) if stop else None
    return [(error, error is first) for error in errors]


def _report(run: _CopyRun, on_error: Callable[[ManifestError, bool], None] | None) -> Any:
    """Pass the errors of `run` to `on_error`. Without a handler, the error that stopped a table is raised."""
    if on_error is None:
        if run.output is None and run.stop is not None:
            raise run.stop
        return run.output
    for error, raised in run.reports:
        on_error(error, raised)
    return run.output


def _detect_format(path: str) -> str:
    if path.endswith(".simple.json"):
        return "simple"
    for suffix, fmt in ((".csv", "csv"), (".xlsx", "xlsx"), (".txt", "ascii")):
        if path.endswith(suffix):
            return fmt
    raise ValueError(f"Cannot detect the format of {path!r}; pass `format` (3.1).")


def _check_input_format(fmt: str) -> None:
    if fmt not in get_args(ReadFormat):
        raise ValueError(f"Unknown input format {fmt!r}.")
    if fmt not in _IMPLEMENTED_READ:
        raise NotImplementedError(f"Reading {fmt!r} is not implemented yet.")


def _check_output_format(fmt: str) -> None:
    if fmt not in get_args(WriteFormat):
        raise ValueError(f"Unknown output format {fmt!r}.")


def _parse_json(text: str, name: str) -> dict:
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise SimpleManifestError(f"{name}: {e}") from e


def _csv_rows(name: str, text: str | None):
    if text is None:
        with open(name, encoding="utf-8-sig", newline="") as f:
            text = f.read()
    return _rows_of(csv.reader(io.StringIO(text, newline="")))


def _rows_of(reader):
    for i, row in enumerate(reader, 1):
        yield str(i), row


def _ascii_rows(name: str, text: str | None):
    if text is None:
        with open(name, encoding="utf-8-sig") as f:
            text = f.read()
    return _read_ascii_tabular_manifest(io.StringIO(text))


def _xlsx_rows(name: str, data: bytes | None, text: str | None):
    file = io.BytesIO(data) if data is not None else None
    return _read_xlsx_manifest(name, file)
