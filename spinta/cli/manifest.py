import pathlib
from typing import Iterator
from typing import List

from typer import Argument
from typer import Context as TyperContext
from typer import Option
from typer import Typer

from spinta import commands
from spinta.components import Context
from spinta.core.enums import Access
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.tabular.helpers import datasets_to_tabular
from spinta.manifests.tabular.helpers import read_tabular_manifest
from spinta.manifests.tabular.helpers import write_tabular_manifest
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
    files: List[pathlib.Path] = Argument(None, help=(
        "Source manifest files to copy from"
    )),
    dest: pathlib.Path = Argument(None, help=(
        "Target manifest file to save a copy to"
    )),
):
    """Copy models from CSV manifest files into another CSV manifest file"""
    context: Context = ctx.obj
    access = get_enum_by_name(Access, access)
    rows = _read_csv_files(context, files, external=source, access=access)
    with dest.open('w') as f:
        write_tabular_manifest(f, rows)


def _read_csv_files(
    context: Context,
    files: List[pathlib.Path],
    *,
    external: bool = True,
    access: Access = Access.private,
) -> Iterator[dict]:
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

            schemas = read_tabular_manifest(path)
            load_manifest_nodes(context, store.manifest, schemas)

            commands.link(context, store.manifest)
            commands.check(context, store.manifest)

            yield from datasets_to_tabular(store.manifest, external=external, access=access)
