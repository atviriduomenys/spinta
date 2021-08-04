import asyncio
from typing import List
from typing import Optional

import click
from typer import Argument
from typer import Context as TyperContext

from spinta import commands
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.store import load_store
from spinta.cli.helpers.store import prepare_manifest
from spinta.core.context import configure_context


def bootstrap(
    ctx: TyperContext,
    manifests: Optional[List[str]] = Argument(None, help=(
        "Manifest files to load"
    )),
):
    """Initialize backends

    This will create tables and sync manifest to backends.
    """
    context = configure_context(ctx.obj, manifests)
    store = prepare_manifest(context, ensure_config_dir=True)

    with context:
        require_auth(context)
        commands.bootstrap(context, store.manifest)


def sync(ctx: TyperContext):
    """Sync source manifests into main manifest

    Single main manifest can be populated from multiple different backends.
    """
    context = ctx.obj
    store = load_store(context)
    commands.load(context, store.internal, into=store.manifest)
    with context:
        require_auth(context)
        coro = commands.sync(context, store.manifest)
        asyncio.run(coro)
    click.echo("Done.")


def migrate(
    ctx: TyperContext,
    manifests: Optional[List[str]] = Argument(None, help=(
        "Manifest files to load"
    )),
):
    """Migrate schema change to backends"""
    context = configure_context(ctx.obj, manifests)
    store = prepare_manifest(context, ensure_config_dir=True)
    with context:
        require_auth(context)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(commands.migrate(context, store.manifest))


def freeze(ctx: TyperContext):
    """Detect schema changes and create new version

    This will read current manifest structure, compare it with a previous
    freezed version and will generate new migration version if current and last
    versions do not match.
    """
    context = ctx.obj
    # Load just store, manifests will be loaded by freeze command.
    store = load_store(context)
    with context:
        require_auth(context)
        commands.freeze(context, store.manifest)
    click.echo("Done.")
