from typing import List, Iterator

import pathlib

import click

from spinta import commands
from spinta.utils.imports import importstr
from spinta.utils.enums import get_enum_by_name
from spinta.core.enums import Access
from spinta.core.context import create_context
from spinta.components import Context
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.tabular.helpers import datasets_to_tabular
from spinta.manifests.tabular.helpers import write_tabular_manifest
from spinta.manifests.tabular.helpers import read_tabular_manifest


@click.command()
@click.option('--option', '-o', multiple=True, help=(
    "Set configuration option, example: `-o option.name=value`."
))
@click.option('--external/--no-external', is_flag=True, default=True, help=(
    "Do not copy external data source metadata."
))
@click.option('--access', default='private', help=(
    "Copy properties with at least specified access."
))
@click.argument('files', nargs=-1, required=True)
@click.pass_context
def main(ctx, option, external, access, files):
    """Copy models from CSV manifest files into another CSV manifest file."""

    context = ctx.obj or create_context('cli', args=option)

    files = [pathlib.Path(f) for f in files]
    files, dest = files[:-1], files[-1]

    access = get_enum_by_name(Access, access)

    rows = _read_csv_files(context, files, external=external, access=access)
    write_tabular_manifest(dest, rows)


def _read_csv_files(
    context: Context,
    files: List[str],
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
