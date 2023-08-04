from typer import Argument
from typer import Context as TyperContext
from typer import Option
from typer import Typer

from spinta import commands
from spinta.cli.helpers.store import load_store
from spinta.core.context import configure_context
from spinta.datasets.keymaps.helpers import load_keymap_from_url

keymap = Typer()


@keymap.command('where', short_help="Show configured keymap location")
def keymap_where(
    ctx: TyperContext,
):
    """Get locaton of a keymap."""
    context = configure_context(ctx.obj)
    store = load_store(context)
    print(store.manifest.keymap.dsn)


@keymap.command('encode', short_help="Get global identifier")
def keymap_encode(
    ctx: TyperContext,
    model: str = Argument(None, help=(
        "Model name to get the identifier for"
    )),
    key: str = Argument(None, help=(
        "Model name to get the identifier for"
    )),
    url: str = Option('', '-i', '--url', help=(
        "Keymap database location (URL)"
    )),
):
    """Get global identifier from keymap database for a given local identifier.

    If local identifier does not exist in keymap database, a new entry will be
    created.
    """
    context = configure_context(ctx.obj)
    store = load_store(context)
    if url:
        keymap = load_keymap_from_url(context, url)
    else:
        keymap = store.manifest.keymap
    commands.prepare(context, keymap)
    with keymap:
        print(keymap.encode(model, key, create=False))


@keymap.command('decode', short_help="Get local identifier")
def keymap_encode(
    ctx: TyperContext,
    model: str = Argument(None, help=(
        "Model name to get the identifier for"
    )),
    key: str = Argument(None, help=(
        "Model name to get the identifier for"
    )),
    url: str = Option('', '-i', '--url', help=(
        "Keymap database location (URL)"
    )),
):
    """Get local identifier from keymap database for a given global identifier."""
    context = configure_context(ctx.obj)
    store = load_store(context)
    if url:
        keymap = load_keymap_from_url(context, url)
    else:
        keymap = store.manifest.keymap
    commands.prepare(context, keymap)
    with keymap:
        print(keymap.decode(model, key))
