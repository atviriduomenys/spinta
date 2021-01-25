import logging
from typing import Optional

from typer import Argument
from typer import Context as TyperContext
from typer import Exit
from typer import echo

from spinta import commands
from spinta.cli.helpers.store import load_store
from spinta.cli.helpers.store import prepare_store
from spinta.components import Context

log = logging.getLogger(__name__)


def run(
    ctx: TyperContext,
    host: str = '127.0.0.1',
    port: int = 8000,
):
    """Run development server"""
    import uvicorn
    import spinta.api

    context: Context = ctx.obj
    prepare_store(context)
    app = spinta.api.init(context)

    echo("Spinta has started!")
    uvicorn.run(app, host=host, port=port)


def wait(
    ctx: TyperContext,
    seconds: Optional[int] = Argument(None),
):
    """Wait while all backends are up"""
    context = ctx.obj
    store = load_store(context)
    if not commands.wait(context, store, seconds=seconds):
        raise Exit(code=1)
