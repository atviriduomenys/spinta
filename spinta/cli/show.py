from typing import List
from typing import Optional

from typer import Argument
from typer import Context as TyperContext
from typer import Option
from typer import echo

from spinta.cli.helpers.store import prepare_manifest
from spinta.components import Mode
from spinta.core.context import configure_context
from spinta.manifests.tabular.helpers import render_tabular_manifest


def show(
    ctx: TyperContext,
    manifests: Optional[List[str]] = Argument(None, help=(
        "Manifest files to load"
    )),
    mode: Mode = Option('internal', help="Mode of backend operation"),
):
    """Show manifest as ascii table"""
    context = configure_context(ctx.obj, manifests, mode=mode)
    store = prepare_manifest(context, verbose=False)
    manifest = store.manifest
    echo(render_tabular_manifest(manifest))

