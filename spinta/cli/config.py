from typing import List
from typing import Optional

from typer import Argument
from typer import Context as TyperContext
from typer import echo

from spinta.cli.helpers.store import prepare_manifest
from spinta.core.config import KeyFormat


def config(
    ctx: TyperContext,
    name: Optional[List[str]] = Argument(None),
    fmt: KeyFormat = KeyFormat.cfg,
):
    """Show current configuration values"""
    context = ctx.obj
    rc = context.get('rc')
    rc.dump(*name, fmt=fmt)


def check(
    ctx: TyperContext,
):
    """Check configuration and manifests"""
    context = ctx.obj
    prepare_manifest(context)
    echo("OK")
