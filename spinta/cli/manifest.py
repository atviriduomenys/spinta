from collections.abc import Callable, Iterator
from pathlib import Path

from typer import Argument, Exit, Option, Typer, echo
from typer import Context as TyperContext

from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.store import load_manifest, load_store, manifest_banner
from spinta.components import Context
from spinta.core.context import configure_context
from spinta.core.enums import Access
from spinta.lib import ManifestError, Spinta, _copy_checked, _refuse_implied_with_base
from spinta.manifests.components import ManifestPath
from spinta.manifests.internal_sql.components import InternalSQLManifest
from spinta.manifests.internal_sql.helpers import write_internal_sql_manifest
from spinta.manifests.mermaid.helpers import write_mermaid_manifest
from spinta.manifests.rdf.writer import write_rdf_manifest
from spinta.manifests.simple.helpers import canonical_json
from spinta.manifests.tabular.components import ManifestColumn, ManifestRow
from spinta.manifests.tabular.helpers import (
    datasets_to_tabular,
    normalizes_columns,
    render_tabular_manifest_rows,
    write_tabular_manifest,
)
from spinta.naming.helpers import reformat_names
from spinta.utils.enums import get_enum_by_name

app = Typer()

# Input formats that `Spinta.copy` reads (3.5). Other formats keep the current path.
_LIB_INPUTS = frozenset({"csv", "ascii", "xlsx", "simple"})


@app.command(short_help="Copy manifest optionally transforming final copy")
def copy(
    ctx: TyperContext,
    source: bool = Option(True, help=("Do not copy external data source metadata")),
    # TODO: Change `str` to `Access`
    #       https://github.com/tiangolo/typer/issues/151
    access: str = Option("private", help=("Copy properties with at least specified access")),
    format_names: bool = Option(False, help=("Reformat model and property names.")),
    output: str | None = Option(None, "-o", "--output", help=("Output tabular manifest in a specified file")),
    dataset: str | None = Option(
        None,
        "-d",
        "--dataset",
        help=("Dataset name. Mermaid: main dataset. RDF Turtle: write only this dataset"),
    ),
    columns: str | None = Option(None, "-c", "--columns", help=("Comma separated list of columns")),
    order_by: str | None = Option(
        None, help=("Order by a specified column (currently only access column is supported)")
    ),
    rename_duplicates: bool = Option(False, help=("Rename duplicate model names by adding number suffix")),
    implied: bool = Option(
        False,
        "--implied",
        help=(
            "Table outputs (csv, xlsx, ascii) write the implied value of a title, description, eli, status, visibility "
            "or access cell that has no given value. Without it, a table has the given values only, so it reads back "
            "with them as given. Not for simple, mermaid or rdf outputs."
        ),
    ),
    base: list[str] = Option(
        None,
        "-b",
        "--base",
        help=(
            "Default base manifest. A model without a base is matched to the one model of the same name, without its "
            "dataset, in the default bases, and that model becomes its base. Can be given more than once."
        ),
    ),
    base_visibility: str | None = Option(
        None,
        "--base-visibility",
        help=(
            "Comma separated visibilities (public, package, protected, private) of the models that must match a "
            "default base. Other models get no error when they do not match. Default: all."
        ),
    ),
    include: list[str] = Option(
        None,
        "-i",
        "--include",
        help=(
            "Manifest whose elements the references of the source manifests need. Only the referenced elements are "
            "copied, and in `.simple.json` they have no `index`. Can be given more than once."
        ),
    ),
    manifests: list[str] = Argument(None, help=("Source manifest files to copy from")),
):
    """Copy models from CSV manifest files into another CSV manifest file"""
    context: Context = ctx.obj
    context = configure_context(context, manifests, check_names=not (format_names or rename_duplicates))
    errors = _CopyErrors(manifests[0] if manifests and len(manifests) == 1 else None)
    table = copy_manifest(
        context,
        source=source,
        access=access,
        format_names=format_names,
        output=output,
        columns=columns,
        order_by=order_by,
        rename_duplicates=rename_duplicates,
        manifests=manifests,
        dataset=dataset,
        base=base,
        base_visibility=_split_visibility(base_visibility),
        include=include,
        implied=implied,
        on_error=errors,
    )
    # The table is printed whenever it was written: an error that was collected is printed on stderr, and the
    # output is still produced (3.1, 3.5).
    if table is not None:
        echo(table)
    errors.summary()
    if errors.raised:
        raise Exit(code=1)


def copy_manifest(
    context: Context,
    source: bool = True,
    access: str = "private",
    format_names: bool = False,
    output: str | None = None,
    columns: str | None = None,
    order_by: str | None = None,
    rename_duplicates: bool = False,
    manifests: list[str] = None,
    output_type: str | None = None,
    dataset: str | None = None,
    base: list[str] | None = None,
    base_visibility: list[str] | None = None,
    include: list[str] | None = None,
    implied: bool = False,
    on_error: Callable[[ManifestError, bool], None] | None = None,
) -> str | None:
    """Copy models from CSV manifest files into another CSV manifest file

    Returns the table for stdout, when the copy goes through `Spinta.copy` and
    has no `output`. Without `on_error` the error that stops a table is raised.
    `base` are the default base manifests (`-b`), and `include` the manifests
    whose referenced elements are copied (`-i`), see `Spinta.copy`.
    """
    lib_format = _lib_output_format(output, output_type)
    lib_paths = _lib_input_paths(manifests)
    if lib_format and lib_paths:
        # The copy is `Spinta.copy` (D14): the same result as a call of the library.
        return _copy_with_lib(
            context,
            lib_paths,
            output,
            lib_format,
            source=source,
            access=access,
            dataset=dataset,
            columns=columns.split(",") if columns else None,
            order_by=order_by,
            format_names=format_names,
            rename_duplicates=rename_duplicates,
            base=base or [],
            base_visibility=base_visibility,
            include=include or [],
            implied=implied,
            on_error=on_error,
        )
    if implied:
        # The option is written by `Spinta.copy` only, as the table of its output (3.5, rule 5).
        raise NotImplementedError("`--implied` is not implemented yet for this input or output.")
    if base or base_visibility is not None or include:
        # The default bases and the includes are `Spinta.copy` options, so only its inputs and outputs take them.
        raise NotImplementedError("`--base` and `--include` are not implemented yet for this input or output.")

    # Only what `Spinta.copy` does not take yet, or does not take at all (3.5, rule 5), keeps this path.
    access = get_enum_by_name(Access, access)
    cols = normalizes_columns(columns.split(",")) if columns else None
    internal = False
    verbose = True
    if not output:
        verbose = False
    else:
        if output_type:
            if output_type == "internal_sql":
                internal = True
        else:
            internal = InternalSQLManifest.detect_from_path(output)

    if output and output.endswith(".mmd"):
        output_type = "mermaid"
    elif output and output.endswith(".ttl"):
        output_type = "rdf"

    if internal or output_type in ("mermaid", "rdf"):
        rows = _read_and_return_manifest(
            context,
            manifests,
            external=source,
            access=access,
            format_names=format_names,
            order_by=order_by,
            rename_duplicates=rename_duplicates,
            verbose=verbose,
            inherit=True,
        )
    else:
        rows = _read_and_return_rows(
            context,
            manifests,
            external=source,
            access=access,
            format_names=format_names,
            order_by=order_by,
            rename_duplicates=rename_duplicates,
            verbose=verbose,
            inherit=True,
        )
    if output:
        if output_type == "mermaid":
            write_mermaid_manifest(context, rows, dataset, output)
        elif output_type == "rdf":
            write_rdf_manifest(context, rows, output, access=access, dataset=dataset)
        elif internal:
            write_internal_sql_manifest(context, output, rows)
        else:
            write_tabular_manifest(context, output, rows)
    else:
        manager = context.get("error_manager")
        handler = manager.handler

        table = render_tabular_manifest_rows(rows, cols)

        if handler.get_counts():
            handler.post_process()
        else:
            echo(table)


def _split_visibility(value: str | None) -> list[str] | None:
    """`--base-visibility public,package` as a list. `None` when the option is not given."""
    if value is None:
        return None
    return [name.strip() for name in value.split(",")]


def _lib_output_format(output: str | None, output_type: str | None) -> str | None:
    """The `Spinta.copy` format of an output (3.5, rule 3), or `None` when the CLI keeps its own path.

    Without `-o` the table is printed, as `ascii`. The extension decides the
    format, as it always did: `.mmd`, `.ttl` and `.simple.json` first, `.txt`
    is the `ascii` table.
    """
    if output is None:
        return "ascii"
    if output_type == "internal_sql" or (not output_type and InternalSQLManifest.detect_from_path(output)):
        return None
    if output.endswith(".mmd"):
        return "mermaid"
    if output.endswith(".ttl"):
        return "rdf"
    if output.endswith(".simple.json"):
        return "simple"
    if output_type in ("mermaid", "rdf"):
        return output_type
    if output.endswith(".xlsx"):
        return "xlsx"
    if output.endswith(".csv"):
        return "csv"
    if output.endswith(".txt"):
        return "ascii"
    return None


def _lib_input_paths(manifests: list[str] | None) -> list[tuple[str, str]] | None:
    """The input files as `(path, format)` pairs, when `Spinta.copy` reads all of them (3.5), else `None`."""
    if not manifests:
        return None
    paths = convert_str_to_manifest_path(manifests)
    if all(path.type in _LIB_INPUTS and path.file is None for path in paths):
        return [(str(path.path), path.type) for path in paths]
    return None


def _copy_with_lib(
    context: Context,
    files: list[tuple[str, str]],
    output: str | None,
    fmt: str,
    *,
    source: bool,
    access: str,
    dataset: str | None,
    columns: list[str] | None,
    order_by: str | None,
    format_names: bool,
    rename_duplicates: bool,
    base: list[str],
    base_visibility: list[str] | None,
    include: list[str],
    implied: bool,
    on_error: Callable[[ManifestError, bool], None] | None,
) -> str | None:
    """`spinta copy` through `Spinta.copy` (3.5). The CLI parses the options and writes the result.

    The options are checked first, so an option that the output does not take raises before anything is loaded
    or printed (3.1). Returns the table for stdout, `None` when a file is written or nothing could be written.
    """
    options = _copy_checked(
        fmt,
        source=source,
        access=access,
        dataset=dataset,
        columns=columns,
        order_by=order_by,
        format_names=format_names,
        rename_duplicates=rename_duplicates,
        implied=implied,
    )
    _refuse_implied_with_base(options, base)
    # The config and the store are loaded as before, and with `-o` the manifest is named (verbose).
    store = load_store(context, verbose=bool(output))
    if output:
        echo(manifest_banner(store.manifest))

    spinta = Spinta._from_context(context)
    result = spinta._copy_files(
        files,
        fmt,
        base=base,
        base_visibility=base_visibility,
        include=_read_includes(spinta, include),
        on_error=on_error,
        **options,
    )
    if output and fmt == "simple":
        # The document keeps every element, so it is written even when a row could not be read (4.10).
        _write_output(output, canonical_json(result))
    elif output and result is not None:
        _write_output(output, result)
    return None if output else result


def _read_includes(spinta: Spinta, include: list[str]) -> list[dict]:
    """The `-i` manifests as `simple` documents (3.5, rule 2). Each file is read in its own format, as an input is.

    A row of an included file that cannot be read stays in its document, with its error. Only the elements that
    the references need are copied from it (3.2), so that error is reported only when its element is copied.
    """
    if not include:
        return []
    docs = []
    for path in convert_str_to_manifest_path(include):
        if path.file is not None or path.type not in _LIB_INPUTS:
            raise NotImplementedError(f"`--include` does not read {path.type!r} manifests yet.")
        doc, _ = spinta._read_with_stop(str(path.path), path.type)
        docs.append(doc)
    return docs


class _CopyErrors:
    """The CLI's error handler for `copy` (3.5, rule 4).

    Each error is printed to stderr and counted. `raised` is set when an error
    stopped the run; that is the exit status 1.
    """

    def __init__(self, file: str | None) -> None:
        self.file = file
        self.count = 0
        self.raised = False

    def __call__(self, error: ManifestError, raised: bool) -> None:
        if error["severity"] == "warning":
            # A warning is logged when it is found, and was never counted here. Keep it so.
            return
        self.count += 1
        self.raised = self.raised or raised
        echo(_error_text(error), err=True)

    def summary(self) -> None:
        if self.count:
            echo(f"Total errors: {self.count}", err=True)
            if self.file is not None:
                echo(f"- {self.file}: {self.count} error(s)", err=True)


def _error_text(error: ManifestError) -> str:
    """An error as `spinta copy` prints it: its message, then its context."""
    context = "".join(f"    {k}: {v}\n" for k, v in error["context"].items())
    return error["message"] + "\n" + ("  Context:\n" if context else "") + context


def _write_output(output: str, data: str | bytes) -> None:
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, bytes):
        path.write_bytes(data)
    else:
        path.write_text(data, encoding="utf-8")


def _read_and_return_manifest(
    context: Context,
    manifests: list[str | ManifestPath],
    *,
    external: bool = True,
    access: Access = Access.private,
    format_names: bool = False,
    order_by: ManifestColumn = None,
    rename_duplicates: bool = False,
    verbose: bool = True,
    check_config: bool = True,
    ensure_backends: bool = True,
    inherit: bool = False,
) -> Iterator[ManifestRow]:
    context = configure_context(context, manifests, ensure_backends=ensure_backends)
    store = load_manifest(
        context,
        rename_duplicates=rename_duplicates,
        load_internal=False,
        verbose=verbose,
        full_load=True,
        check_config=check_config,
        inherit=inherit,
    )

    if format_names:
        reformat_names(context, store.manifest)

    return store.manifest


def _read_and_return_rows(
    context: Context,
    manifests: list[str],
    *,
    external: bool = True,
    access: Access = Access.private,
    format_names: bool = False,
    order_by: ManifestColumn = None,
    rename_duplicates: bool = False,
    verbose: bool = True,
    inherit: bool = False,
) -> Iterator[ManifestRow]:
    context = configure_context(context, manifests)
    store = load_manifest(
        context,
        rename_duplicates=rename_duplicates,
        load_internal=False,
        verbose=verbose,
        full_load=True,
        inherit=inherit,
    )
    if format_names:
        reformat_names(context, store.manifest)

    yield from datasets_to_tabular(
        context,
        store.manifest,
        external=external,
        access=access,
        order_by=order_by,
    )
