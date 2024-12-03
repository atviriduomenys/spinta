import asyncio
from typing import List
from typing import Optional

import click
from typer import Argument
from typer import Context as TyperContext
from typer import Option

from spinta import commands
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.errors import cli_error
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.migrate import MigrateRename, MigrateMeta
from spinta.cli.helpers.store import load_store
from spinta.cli.helpers.store import prepare_manifest
from spinta.core.context import configure_context
from spinta.manifests.commands.manifest import has_dataset


def bootstrap(
    ctx: TyperContext,
    manifests: Optional[List[str]] = Argument(None, help=(
        "Manifest files to load"
    )),
):
    """Initialize backends

    This will create tables and sync manifest to backends.
    """
    manifests = convert_str_to_manifest_path(manifests)
    context = configure_context(ctx.obj, manifests)
    store = prepare_manifest(context, ensure_config_dir=True, full_load=True)

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
    plan: bool = Option(False, '-p', '--plan', help=(
        "If added, prints SQL code instead of executing it"
    ), is_flag=True),
    rename: str = Option(None, '-r', '--rename', help=(
        "JSON file, that maps manifest node renaming (models, properties)"
    )),
    autocommit: bool = Option(False, '-a', '--autocommit', help=(
        "If added, migrate will do atomic transactions, meaning it will automatically commit after each action (use it at your own risk)"
    )),
    datasets: Optional[List[str]] = Option(None, '-d', '--datasets', help=(
        "List of datasets to migrate"
    )),
):
    """Migrate schema change to backends"""
    manifests = convert_str_to_manifest_path(manifests)
    context = configure_context(ctx.obj, manifests)
    store = prepare_manifest(context, ensure_config_dir=True)
    manifest = store.manifest

    if datasets:
        invalid_datasets = [dataset for dataset in datasets if not has_dataset(context, manifest, dataset)]
        if invalid_datasets:
            cli_error(f"Invalid dataset(s) provided: {', '.join(invalid_datasets)}")

    migrate_meta = MigrateMeta(
        plan=plan,
        autocommit=autocommit,
        rename=MigrateRename(
            rename_src=rename
        ),
        datasets=datasets
    )
    commands.migrate(context, manifest, migrate_meta)


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



