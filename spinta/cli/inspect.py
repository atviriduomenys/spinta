from typing import Optional
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
from spinta.manifests.tabular.helpers import load_ascii_tabular_manifest
from spinta.manifests.tabular.helpers import render_tabular_manifest
from spinta.manifests.tabular.helpers import write_tabular_manifest


def inspect(
    ctx: TyperContext,
    manifest: Optional[str] = Argument(None, help="Path to manifest."),
    resource: Optional[Tuple[str, str]] = Option((None, None), '-r', '--resource', help=(
        "Resource type and source URI (-r sql sqlite:////tmp/db.sqlite)"
    )),
    formula: str = Option('', '-f', '--formula', help=(
        "Formula if needed, to prepare resource for reading"
    )),
    output: Optional[str] = Option(None, '-o', '--output', help=(
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
            'keymaps.inspect': {
                'type': 'sqlalchemy',
                'dsn': 'sqlite:///keymaps.db',
            },
            'manifests.inspect': {
                'type': 'tabular',
                'backend': 'null',
                'keymap': 'inspect',
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
                'keymap': 'inspect',
                'mode': 'internal',
                'path': manifest,
            },
            'keymaps.inspect': {
                'type': 'sqlalchemy',
                'dsn': 'sqlite:///keymaps.db',
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
        write_tabular_manifest(output, manifest)
    else:
        echo(render_tabular_manifest(manifest))
