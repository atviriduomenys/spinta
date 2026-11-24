import enum
import json
from collections import Counter

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
from spinta.handlers import ErrorHandler
from spinta.lib import Spinta, _located
from spinta.manifests.simple.errors import manifest_error, read_error
from spinta.manifests.simple.types import ManifestError

# Formats that `Spinta.copy` reads (3.5). Other formats keep the current path.
_LIB_FORMATS = frozenset({"csv", "ascii", "xlsx", "simple"})


class CheckFormat(str, enum.Enum):
    text = "text"
    json = "json"


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
    codes: list[str] | None = Option(
        None,
        "-c",
        "--code",
        help="Show only errors with this code, example: `-c UnresolvedReference`. Can be given multiple times.",
    ),
    fmt: CheckFormat = Option(CheckFormat.text, "-f", "--format", help="Output format: text or json."),
):
    """Check configuration and manifests"""
    paths = convert_str_to_manifest_path(manifests)
    wanted = set(codes) if codes else None
    errors: list[ManifestError] = []
    failed = False

    if paths and all(path.type in _LIB_FORMATS for path in paths):
        # The manifest part is `Spinta.check` (D14), so every error of the documents is shown. The exit status is
        # the one of the current check: errors that were collected do not fail the command, an error that stopped
        # the check does (3.5).
        spinta = Spinta._from_context(ctx.obj)
        docs = [spinta._read_with_stop(path.path, path.type)[0] for path in paths]
        errors, failed = spinta._check_errors(
            docs,
            code=wanted,
            check_names=check_names,
            strict_references=strict_references,
        )

    if not failed:
        # Backend checks need the loaded store, so they stay on the current path. The manifest part found the
        # manifest errors already, so they are not found twice.
        reported = {_identity(error) for error in errors}
        errors += _check_backends(ctx.obj, manifests, mode, check_names, check_query_conflicts, wanted, reported)

    _show(errors, fmt)
    # An error of severity `error` fails the command, and so does an error that stopped the check (3.5).
    if failed or any(error["severity"] == "error" for error in errors):
        raise Exit(1)


def _check_backends(
    context: Context,
    manifests: list[str] | None,
    mode: Mode,
    check_names: bool | None,
    check_query_conflicts: bool | None,
    wanted: set[str] | None,
    reported: set[tuple],
) -> list[ManifestError]:
    """Load the manifests and run the backend checks. Returns the errors they find, with the codes `wanted`.

    The load finds the manifest errors again. `reported` are the ones the manifest part found (see `_identity`),
    they are not returned twice.
    """
    paths = convert_str_to_manifest_path(manifests)
    # The source of the errors, when there is one manifest file (see `_located`).
    source = paths[0].path if len(paths) == 1 else None
    context: Context = configure_context(
        context,
        manifests,
        mode=mode,
        check_names=check_names,
        check_query_conflicts=check_query_conflicts,
    )
    manager = context.get("error_manager")
    manager.handler = _NewErrors(reported, wanted)
    # Collect the errors of the load, as the manifest part does, so that they are reported and not only logged.
    manager.collect = True
    # `verbose=False`: the output is the errors only, so that `-f json` stays valid JSON.
    prepare_manifest(context, verbose=False, ensure_config_dir=True, full_load=True)

    store: Store = context.get("store")
    if store.manifest:
        for model in commands.get_models(context, store.manifest).values():
            if model.external and model.external.resource and model.external.resource.backend:
                commands.check(context, model, model.external.resource.backend)

    return [_located(error, source) for error in manager.handler.errors]


# The keys of an error's context that tell which element it is about. The same error is found by the manifest
# part and by the backend checks, and these keys are what they have in common.
_PLACE_KEYS = ("type", "dataset", "model", "property", "ref")


def _identity(error: ManifestError) -> tuple:
    """What tells an error apart from another one, to report it once: its code, message and place."""
    context = error["context"]
    return (error["code"], error["message"], tuple(context.get(key) for key in _PLACE_KEYS))


class _NewErrors(ErrorHandler):
    """Collects the errors that are not in `reported`, and that have a code in `wanted` (see `_check_backends`)."""

    def __init__(self, reported: set[tuple], wanted: set[str] | None) -> None:
        self.reported = reported
        self.wanted = wanted
        self.errors: list[ManifestError] = []

    def handle_error(self, error: BaseError, file: str | None = None, *, raised: bool = False) -> None:
        found = _as_error(error)
        if _identity(found) in self.reported:
            return
        if self.wanted is None or found["code"] in self.wanted:
            self.errors.append(found)

    def post_process(self) -> None:
        pass


def _as_error(error: Exception) -> ManifestError:
    """The `ManifestError` of an error that the backend checks raise. Other exceptions are read errors (4.10).

    Every key of the context is kept: a backend error does not name an element of a `simple` document, and its
    `entity` and `attribute` tell what it is about.
    """
    if isinstance(error, BaseError):
        found = manifest_error(error)
        found["context"] = {k: str(v) for k, v in error.context.items() if v is not None}
        return found
    return read_error(type(error).__name__, str(error), None)


def _show(errors: list[ManifestError], fmt: CheckFormat) -> None:
    """Print the errors as `fmt` says: `text` for people, `json` as `Spinta.check` returns them."""
    if fmt == CheckFormat.json:
        echo(json.dumps(errors, ensure_ascii=False, indent=2))
        return
    if not errors:
        echo("OK")
        return
    for error in errors:
        echo(_render(error))
        echo()
    counts = Counter(error["code"] for error in errors if error["severity"] == "error")
    if counts:
        echo("Summary:")
        for code, n in counts.items():
            echo(f"- {code}: {_count(n)}.")
        echo()
        echo(f"Total: {_count(sum(counts.values()))}.")


def _render(error: ManifestError) -> str:
    """A stored error as the CLI shows it: its severity, message, index, code and context."""
    lines = [f"{error['severity']}: {error['message']}"]
    if error["index"] is not None:
        lines.append(f"  index: {error['index']}")
    lines.append(f"  code: {error['code']}")
    if error["context"]:
        lines.append("  context:")
        lines.extend(f"    {key}: {value}" for key, value in error["context"].items())
    return "\n".join(lines)


def _count(n: int) -> str:
    return f"{n} error" if n == 1 else f"{n} errors"
