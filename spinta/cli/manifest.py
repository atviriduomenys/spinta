import pathlib
from typing import Iterator
from typing import List
from typing import Optional

from typer import Argument
from typer import Context as TyperContext
from typer import Option
from typer import Typer
from typer import echo

from spinta import commands
from spinta.components import Context
from spinta.core.enums import Access
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.tabular.components import ManifestColumn
from spinta.manifests.tabular.components import ManifestRow
from spinta.manifests.tabular.helpers import datasets_to_tabular
from spinta.manifests.tabular.helpers import normalizes_columns
from spinta.manifests.tabular.helpers import read_tabular_manifest
from spinta.manifests.tabular.helpers import render_tabular_manifest_rows
from spinta.manifests.tabular.helpers import write_tabular_manifest
from spinta.naming.helpers import reformat_names
from spinta.utils.enums import get_enum_by_name
from spinta.utils.imports import importstr

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
    output: Optional[pathlib.Path] = Option(None, '-o', '--output', help=(
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
    files: List[pathlib.Path] = Argument(None, help=(
        "Source manifest files to copy from"
    )),
):
    """Copy models from CSV manifest files into another CSV manifest file"""
    context: Context = ctx.obj
    access = get_enum_by_name(Access, access)
    cols = normalizes_columns(columns.split(',')) if columns else None

    rows = _read_csv_files(
        context,
        files,
        external=source,
        access=access,
        format_names=format_names,
        order_by=order_by,
        rename_duplicates=rename_duplicates,
    )

    if output:
        with output.open('w') as f:
            write_tabular_manifest(f, rows)
    else:
        echo(render_tabular_manifest_rows(rows, cols))


def _read_csv_files(
    context: Context,
    files: List[pathlib.Path],
    *,
    external: bool = True,
    access: Access = Access.private,
    format_names: bool = False,
    order_by: ManifestColumn = None,
    rename_duplicates: bool = False,
) -> Iterator[ManifestRow]:
    rc = context.get('rc')
    for path in files:
        with context:
            context.set('rc', rc.fork({
                'manifest': 'copy',
                'manifests': {
                    'copy': {
                        'type': 'tabular',
                        'path': path,
                        'keymap': 'default',
                        'backend': None,
                    },
                },
                'keymaps': {
                    'default': {
                        'type': 'sqlalchemy',
                        'dsn': 'sqlite:///:memory:',
                    }
                }
            }))

            config = context.get('config')
            commands.load(context, config)
            commands.check(context, config)

            Store = rc.get('components', 'core', 'store', cast=importstr, required=True)
            store = Store()
            context.set('store', store)
            commands.load(context, store)

            schemas = read_tabular_manifest(
                path,
                rename_duplicates=rename_duplicates,
            )
            load_manifest_nodes(context, store.manifest, schemas)

            commands.link(context, store.manifest)
            commands.check(context, store.manifest)

            if format_names:
                reformat_names(context, store.manifest)

            yield from datasets_to_tabular(
                store.manifest,
                external=external,
                access=access,
                order_by=order_by,
            )

