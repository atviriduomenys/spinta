from typer import Argument, Exit, Option, echo
from typer import Context as TyperContext

from spinta import commands
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.store import prepare_manifest
from spinta.components import Context, Store
from spinta.core.config import KeyFormat
from spinta.core.context import configure_context
from spinta.core.enums import Mode
from spinta.exceptions import BaseError
from spinta.handlers import CLIErrorHandler
from spinta.lib import Spinta
from spinta.manifests.simple.types import MANIFEST_TYPE, SPINTA_KEY, ManifestError

# Formats that `Spinta.copy` reads (3.5). Other formats keep the current path.
_LIB_FORMATS = frozenset({"csv", "ascii", "xlsx", "simple"})


def config(
    ctx: TyperContext,
    name: list[str] | None = Argument(None),
    fmt: KeyFormat = KeyFormat.cfg,
):
    """Show current configuration values"""
    context = configure_context(ctx.obj)
    rc = context.get("rc")
    rc.dump(*name, fmt=fmt)


def check(
    ctx: TyperContext,
    manifests: list[str] | None = Argument(None, help=("Manifest files to load")),
    mode: Mode = Option("internal", help="Mode of backend operation"),
    check_names: bool = Option(None, help="To check dataset, model and property names"),
    check_query_conflicts: bool = Option(
        None,
        help="To check that resource source query keys do not clash with resource param query() keys",
    ),
    strict_references: bool = Option(
        False,
        "--strict-references/--no-strict-references",
        help="Unresolved references are errors, not warnings",
    ),
):
    """Check configuration and manifests"""
    paths = convert_str_to_manifest_path(manifests)
    counts: dict[str, int] = {}
    printed = False
    reported: set[tuple[str, str]] = set()

    if paths and all(path.type in _LIB_FORMATS for path in paths):
        # The manifest part is `Spinta.check` (D14): every error of the
        # document is printed. The exit status is the one of the current
        # check: errors that were collected do not fail the command, an error
        # that stopped the check does (3.5).
        spinta = Spinta._from_context(ctx.obj)
        # The files are read, not linked. The exit status depends on the errors of the read (3.5), and the link
        # errors that `copy` stores would be taken for read errors, and would change the report of the files.
        docs = [spinta._read_with_stop(path.path, path.type)[0] for path in paths]
        doc, failed = spinta._check(docs, check_names=check_names, strict_references=strict_references)
        found = _report(doc, paths, docs)
        printed = bool(found)
        reported = {(error["code"], error["message"]) for _, error in found}
        counts.update(_error_counts(found))
        if failed:
            _summary(counts)
            raise Exit(1)

    # Backend checks need the loaded store, so they stay on the current path.
    backend = _check_backends(ctx.obj, manifests, mode, check_names, check_query_conflicts, reported)
    for name, n in backend.items():
        counts[name] = counts.get(name, 0) + n

    if sum(counts.values()):
        _summary(counts)
    elif not printed:
        echo("OK")


def _check_backends(
    context: Context,
    manifests: list[str] | None,
    mode: Mode,
    check_names: bool | None,
    check_query_conflicts: bool | None,
    reported: set[tuple[str, str]],
) -> dict[str, int]:
    """Load the manifests and run the backend checks. Returns the error counts per file.

    The load finds the manifest errors again. `reported` are the ones `Spinta.check` printed,
    they are not printed or counted twice.
    """
    manifests = convert_str_to_manifest_path(manifests)
    context: Context = configure_context(
        context,
        manifests,
        mode=mode,
        check_names=check_names,
        check_query_conflicts=check_query_conflicts,
    )
    context.get("error_manager").handler = _NewErrors(reported)
    prepare_manifest(context, ensure_config_dir=True, full_load=True)
    manager = context.get("error_manager")

    store: Store = context.get("store")
    if store.manifest:
        for model in commands.get_models(context, store.manifest).values():
            if model.external and model.external.resource and model.external.resource.backend:
                commands.check(context, model, model.external.resource.backend)

    return {_file_name(name): n for name, n in manager.handler.get_counts().items()}


class _NewErrors(CLIErrorHandler):
    """Prints and counts the errors that are not in `reported` (see `_check_backends`)."""

    def __init__(self, reported: set[tuple[str, str]]) -> None:
        super().__init__()
        self.reported = reported

    def handle_error(self, error: BaseError, file: str | None = None, *, raised: bool = False) -> None:
        if (type(error).__name__, error.message) in self.reported:
            return
        super().handle_error(error, file=file)


def _file_name(name: str | None) -> str:
    return name if name else "(no file)"


def _report(doc, paths, docs) -> list[tuple[str, ManifestError]]:
    """Print every error and warning of a checked document, and return them with their file.

    An error belongs to the file of its element. The errors that name no
    element are on the `manifest` of their file, and they belong to that file
    when they were read from it. An error that this run found and that names
    no element belongs to the document, when several files were checked: the
    run cannot tell which file it comes from.
    """
    names = [str(path.path) for path in paths]
    owners: dict[tuple[str, str], str] = {}
    read: dict[str, list[ManifestError]] = {}
    for name, source in zip(names, docs):
        for type_, elements in source.items():
            if type_ != SPINTA_KEY:
                for path in elements:
                    owners.setdefault((type_, path), name)
        for path, manifest in source.get(MANIFEST_TYPE, {}).items():
            read.setdefault(path, []).extend(manifest["errors"])
    document = names[0] if len(names) == 1 else "(document)"

    found: list[tuple[str, ManifestError]] = []
    for type_, elements in doc.items():
        if type_ == SPINTA_KEY:
            continue
        for path, element in elements.items():
            for error in element["errors"]:
                owner = owners.get((type_, path), document)
                if type_ == MANIFEST_TYPE and not any(_same_error(error, e) for e in read.get(path, [])):
                    owner = document
                found.append((owner, error))

    for _, error in found:
        echo(_render(error))
    return found


def _same_error(error: ManifestError, other: ManifestError) -> bool:
    return (error["code"], error["message"], error["index"]) == (other["code"], other["message"], other["index"])


def _render(error: ManifestError) -> str:
    """A stored error as the CLI shows it: its message, its index and its context."""
    head = error["message"]
    if error["severity"] != "error":
        head = f"{error['severity']}: {head}"
    lines = [head]
    if error["index"] is not None:
        lines.append(f"  index: {error['index']}")
    if error["context"]:
        lines.append("  Context:")
        lines.extend(f"    {key}: {value}" for key, value in error["context"].items())
    return "\n".join(lines) + "\n"


def _error_counts(found: list[tuple[str, ManifestError]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for name, error in found:
        if error["severity"] == "error":
            counts[name] = counts.get(name, 0) + 1
    return counts


def _summary(counts: dict[str, int]) -> None:
    echo(f"Total errors: {sum(counts.values())}")
    for name, n in counts.items():
        if n:
            echo(f"- {name}: {n} error(s)")
