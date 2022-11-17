from typing import List
from typing import Optional

from typer import Argument
from typer import Context as TyperContext
from typer import Option
from typer import echo

from spinta.cli.helpers.store import prepare_manifest
from spinta.components import Mode
from spinta.core.config import KeyFormat
from spinta.core.context import configure_context


def config(
    ctx: TyperContext,
    name: Optional[List[str]] = Argument(None),
    fmt: KeyFormat = KeyFormat.cfg,
):
    """Show current configuration values"""
    context = configure_context(ctx.obj)
    rc = context.get('rc')
    rc.dump(*name, fmt=fmt)


def check(
    ctx: TyperContext,
    manifests: Optional[List[str]] = Argument(None, help=(
        "Manifest files to load"
    )),
    mode: Mode = Option('internal', help="Mode of backend operation"),
    names: Optional[str] = Option(None, '--names', help="To check dataset, model and property names"),
):
    """Check configuration and manifests"""
    context = configure_context(ctx.obj, manifests, mode=mode, names=names)
    prepare_manifest(context, ensure_config_dir=True)
    echo("OK")
