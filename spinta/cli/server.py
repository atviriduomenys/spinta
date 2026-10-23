import logging

from typer import Argument, Exit, Option, echo
from typer import Context as TyperContext

from spinta import commands
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.store import load_store, prepare_manifest
from spinta.core.context import configure_context
from spinta.core.enums import Mode

log = logging.getLogger(__name__)


def run(
    ctx: TyperContext,
    manifests: list[str] | None = Argument(None, help=("Manifest files to load")),
    mode: Mode = Option("internal", help="Mode of backend operation"),
    host: str = Option("127.0.0.1", help="Run server on given host"),
    port: int = Option(8000, help="Run server on given port"),
    backend: str | None = Option(None, "-b", "--backend", help=("Backend connection string")),
    backend_type: str | None = Option(None, "-t", "--backend_type", help=("Backend type")),
):
    """Run development server"""
    import os

    import uvicorn

    import spinta.api

    os.environ["AUTHLIB_INSECURE_TRANSPORT"] = "1"

    manifests = convert_str_to_manifest_path(manifests)
    context = configure_context(ctx.obj, manifests, mode=mode, backend_type=backend_type, backend=backend)
    prepare_manifest(context, ensure_config_dir=True)
    app = spinta.api.init(context)

    echo("Spinta has started!")
    uvicorn.run(app, host=host, port=port)


def wait(
    ctx: TyperContext,
    seconds: int | None = Argument(None),
):
    """Wait while all backends are up"""
    context = ctx.obj
    store = load_store(context)
    if not commands.wait(context, store, seconds=seconds, verbose=True):
        raise Exit(code=1)
