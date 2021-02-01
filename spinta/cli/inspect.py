import pathlib
from typing import Optional
from typing import TextIO
from typing import Tuple

from typer import Argument
from typer import Context as TyperContext
from typer import Option
from typer import echo

from spinta import commands
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.store import load_store
from spinta.cli.helpers.store import prepare_manifest
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.components import Manifest
from spinta.manifests.tabular.helpers import datasets_to_tabular
from spinta.manifests.tabular.helpers import load_ascii_tabular_manifest
from spinta.manifests.tabular.helpers import render_tabular_manifest
from spinta.manifests.tabular.helpers import write_tabular_manifest


def _save_manifest(manifest: Manifest, dest: TextIO):
    # TODO: Currently saving is hardcoded to tabular manifest type, but it
    #       should be possible to save or probably freeze to any manifest type.
    rows = datasets_to_tabular(manifest)
    write_tabular_manifest(dest, rows)


def inspect(
    ctx: TyperContext,
    manifest: Optional[pathlib.Path] = Argument(None, help="Path to manifest."),
    resource: Optional[Tuple[str, str]] = Option((None, None), '-r', '--resource', help=(
        "Resource type and source URI (-r sql sqlite:////tmp/db.sqlite)"
    )),
    formula: str = Option('', '-f', '--formula', help=(
        "Formula if needed, to prepare resource for reading"
    )),
    output: Optional[pathlib.Path] = Option(None, '-o', '--output', help=(
        "Output tabular manifest in a specified file"
    )),
    auth: Optional[str] = Option(None, '-a', '--auth', help=(
        "Authorize as a client"
    )),
):
    """Update manifest schema from an external data source"""
    context: Context = ctx.obj
    context = context.fork('inspect')

    if any(resource):
        config = {
            'backends.null': {
                'type': 'memory',
            },
            'manifests.inspect': {
                'type': 'tabular',
                'backend': 'null',
                'keymap': '',
                'mode': 'external',
                'path': None,
            },
            'manifest': 'inspect',
        }

        # Add given manifest file to configuration
        rc: RawConfig = context.get('rc')
        context.set('rc', rc.fork(config))
        store = load_store(context)
        resource_type, resource_source = resource
        load_ascii_tabular_manifest(context, store.manifest, f'''
        d | r               | type            | source            | prepare
        dataset             |                 |                   |
          | {resource_type} | {resource_type} | {resource_source} | {formula}
        ''', strip=True)
        commands.check(context, store.manifest)
        commands.prepare(context, store.manifest)

    elif manifest:
        config = {
            'backends': [],
            'manifest': 'inspect',
            'manifests.inspect': {
                'type': 'tabular',
                'backend': '',
                'keymap': '',
                'mode': 'internal',
                'path': manifest,
            },
        }

        # Add given manifest file to configuration
        rc: RawConfig = context.get('rc')
        context.set('rc', rc.fork(config))

        # Load manifest
        store = prepare_manifest(context)

    else:
        # Load manifest
        store = prepare_manifest(context)

    manifest = store.manifest

    with context:
        require_auth(context, auth)
        commands.inspect(context, manifest)

    if output:
        with pathlib.Path(output).open('w') as f:
            _save_manifest(manifest, f)
    else:
        echo(render_tabular_manifest(manifest))
