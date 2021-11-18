from typing import Iterator
from typing import List
from typing import Optional

from typer import Argument
from typer import Context as TyperContext
from typer import Option
from typer import Typer
from typer import echo

from spinta.cli.helpers.store import load_manifest
from spinta.components import Context
from spinta.core.context import configure_context
from spinta.core.enums import Access
from spinta.manifests.tabular.components import ManifestColumn
from spinta.manifests.tabular.components import ManifestRow
from spinta.manifests.tabular.helpers import datasets_to_tabular
from spinta.manifests.tabular.helpers import normalizes_columns
from spinta.manifests.tabular.helpers import render_tabular_manifest_rows
from spinta.manifests.tabular.helpers import write_tabular_manifest
from spinta.naming.helpers import reformat_names
from spinta.utils.enums import get_enum_by_name

app = Typer()


@app.command(short_help="Copy manifest optionally transforming final copy")
def copy(
    ctx: TyperContext,
    source: bool = Option(True, help=(
        "Do not copy external data source metadata"
    )),
    # TODO: Change `str` to `Access`
    #       https://github.com/tiangolo/typer/issues/151
    access: str = Option('private', help=(
        "Copy properties with at least specified access"
    )),
    format_names: bool = Option(False, help=(
        "Reformat model and property names."
    )),
    output: Optional[str] = Option(None, '-o', '--output', help=(
        "Output tabular manifest in a specified file"
    )),
    columns: Optional[str] = Option(None, '-c', '--columns', help=(
        "Comma separated list of columns"
    )),
    order_by: Optional[str] = Option(None, help=(
        "Order by a specified column (currently only access column is supported)"
    )),
    rename_duplicates: bool = Option(False, help=(
        "Rename duplicate model names by adding number suffix"
    )),
    manifests: List[str] = Argument(None, help=(
        "Source manifest files to copy from"
    )),
):
    """Copy models from CSV manifest files into another CSV manifest file"""
    context: Context = ctx.obj
    access = get_enum_by_name(Access, access)
    cols = normalizes_columns(columns.split(',')) if columns else None

    verbose = True
    if not output:
        verbose = False

    rows = _read_csv_files(
        context,
        manifests,
        external=source,
        access=access,
        format_names=format_names,
        order_by=order_by,
        rename_duplicates=rename_duplicates,
        verbose=verbose,
    )

    if output:
        write_tabular_manifest(output, rows)
    else:
        echo(render_tabular_manifest_rows(rows, cols))


def _read_csv_files(
    context: Context,
    manifests: List[str],
    *,
    external: bool = True,
    access: Access = Access.private,
    format_names: bool = False,
    order_by: ManifestColumn = None,
    rename_duplicates: bool = False,
    verbose: bool = True,
) -> Iterator[ManifestRow]:
    context = configure_context(context, manifests)
    store = load_manifest(
        context,
        rename_duplicates=rename_duplicates,
        load_internal=False,
        verbose=verbose,
    )

    if format_names:
        reformat_names(context, store.manifest)

    yield from datasets_to_tabular(
        store.manifest,
        external=external,
        access=access,
        order_by=order_by,
    )

