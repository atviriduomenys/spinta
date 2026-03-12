from typing import List, Optional

from typer import Argument, Option, echo
from typer import Context as TyperContext

from spinta import commands
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.store import prepare_manifest
from spinta.components import Context, Store
from spinta.core.config import KeyFormat
from spinta.core.context import configure_context
from spinta.core.enums import Mode


def config(
    ctx: TyperContext,
    name: Optional[List[str]] = Argument(None),
    fmt: KeyFormat = KeyFormat.cfg,
):
    """Show current configuration values"""
    context = configure_context(ctx.obj)
    rc = context.get("rc")
    rc.dump(*name, fmt=fmt)


def check(
    ctx: TyperContext,
    manifests: Optional[List[str]] = Argument(None, help=("Manifest files to load")),
    mode: Mode = Option("internal", help="Mode of backend operation"),
    check_names: bool = Option(None, help="To check dataset, model and property names"),
):
    """Check configuration and manifests"""
    manifests = convert_str_to_manifest_path(manifests)
    context: Context = configure_context(ctx.obj, manifests, mode=mode, check_names=check_names)
    prepare_manifest(context, ensure_config_dir=True, full_load=True)
    manager = context.get("error_manager")

    store: Store = context.get("store")
    if store.manifest:
        for model in commands.get_models(context, store.manifest).values():
            if model.external and model.external.resource and model.external.resource.backend:
                commands.check(context, model, model.external.resource.backend)

    handler = manager.handler

    if handler.get_counts():
        handler.post_process()
    else:
        echo("OK")
