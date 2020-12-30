import asyncio

import click

from spinta import commands
from spinta.cli import main
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.store import load_store
from spinta.cli.helpers.store import prepare_manifest


@main.command()
@click.pass_context
def bootstrap(ctx):
    context = ctx.obj
    store = prepare_manifest(context)
    with context:
        require_auth(context)
        commands.bootstrap(context, store.manifest)


@main.command()
@click.pass_context
def sync(ctx):
    context = ctx.obj
    store = load_store(context)
    commands.load(context, store.internal, into=store.manifest)
    with context:
        require_auth(context)
        coro = commands.sync(context, store.manifest)
        asyncio.run(coro)
    click.echo("Done.")


@main.command()
@click.pass_context
def migrate(ctx):
    context = ctx.obj
    store = prepare_manifest(context)
    with context:
        require_auth(context)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(commands.migrate(context, store.manifest))


@main.command()
@click.pass_context
def freeze(ctx):
    context = ctx.obj
    # Load just store, manifests will be loaded by freeze command.
    store = load_store(context)
    with context:
        require_auth(context)
        commands.freeze(context, store.manifest)
    click.echo("Done.")
