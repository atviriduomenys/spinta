import pathlib
from typing import TextIO

import click

from spinta import commands
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.store import prepare_manifest
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.manifests.tabular.helpers import datasets_to_tabular
from spinta.manifests.tabular.helpers import write_tabular_manifest


def _save_manifest(manifest: Manifest, dest: TextIO):
    # TODO: Currently saving is hardcoded to tabular manifest type, but it
    #       should be possible to save or probably freeze to any manifest type.
    rows = datasets_to_tabular(manifest)
    write_tabular_manifest(dest, rows)


@click.command(help='Update manifest from data source schema.')
@click.option('--auth', '-a', help="Authorize as client.")
@click.pass_context
def inspect(
    ctx: click.Context,
    auth: str,
):
    context: Context = ctx.obj
    context: Context = context.fork('inspect')

    # Load manifest
    store = prepare_manifest(context)
    manifest = store.manifest
    with context:
        require_auth(context, auth)

        commands.inspect(context, manifest)

        output = manifest.path
        with pathlib.Path(output).open('w') as f:
            _save_manifest(manifest, f)
