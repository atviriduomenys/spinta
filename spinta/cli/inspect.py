from typing import Optional
from typing import Tuple

from typer import Argument
from typer import Context as TyperContext
from typer import Option
from typer import echo

from spinta import commands
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.store import prepare_manifest
from spinta.components import Mode
from spinta.core.context import configure_context
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import init_manifest
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.tabular.helpers import render_tabular_manifest
from spinta.manifests.tabular.helpers import write_tabular_manifest
from spinta.core.config import parse_resource_args


def inspect(
    ctx: TyperContext,
    manifest: Optional[str] = Argument(None, help="Path to manifest."),
    resource: Optional[Tuple[str, str]] = Option((None, None), '-r', '--resource', help=(
        "Resource type and source URI (-r sql sqlite:////tmp/db.sqlite)"
    )),
    formula: str = Option('', '-f', '--formula', help=(
        "Formula if needed, to prepare resource for reading"
    )),
    backend: Optional[str] = Option(None, '-b', '--backend', help=(
        "Backend connection string"
    )),
    output: Optional[str] = Option(None, '-o', '--output', help=(
        "Output tabular manifest in a specified file"
    )),
    auth: Optional[str] = Option(None, '-a', '--auth', help=(
        "Authorize as a client"
    )),
):
    """Update manifest schema from an external data source"""
    resources = parse_resource_args(*resource, formula)
    context = configure_context(
        ctx.obj,
        [manifest] if manifest else None,
        mode=Mode.external,
        backend=backend,
        resources=resources,
    )
    store = prepare_manifest(context, ensure_config_dir=True)

    manifest = Manifest()
    with context:
        require_auth(context, auth)
        init_manifest(context, manifest, 'inspect')
        manifest.keymap = store.manifest.keymap
        schemas = commands.inspect(
            context,
            store.manifest.backend,
            store.manifest,
            None,
        )
        load_manifest_nodes(context, manifest, schemas)
        commands.link(context, manifest)

    if output:
        write_tabular_manifest(output, manifest)
    else:
        echo(render_tabular_manifest(manifest))
