from pathlib import Path

import click

from spinta import commands
from spinta.auth import create_client_file
from spinta.auth import gen_auth_server_keys
from spinta.components import Context
from spinta.components import Store
from spinta.core.config import DEFAULT_CONFIG_PATH


def _ensure_config_dir(path: Path, *, verbose: bool = True):
    """Create default config_path directory if it does not exits"""
    if path.exists() or path != DEFAULT_CONFIG_PATH:
        return

    if verbose:
        click.echo(f"Initializing default config dir: {path}")

    path.mkdir(parents=True)
    (path / 'clients').mkdir(parents=True)
    gen_auth_server_keys(path)
    create_client_file(path / 'clients', 'default', secret=None, scopes=[
        'spinta_getall',
        'spinta_getone',
        'spinta_search',
        'spinta_changes',
    ])


def load_store(
    context: Context,
    *,
    verbose: bool = True,
    ensure_config_dir: bool = False,
) -> Store:
    config = context.get('config')
    commands.load(context, config)
    if ensure_config_dir:
        _ensure_config_dir(config.config_path, verbose=verbose)
    commands.check(context, config)
    store = context.get('store')
    commands.load(context, store)
    return store


def prepare_manifest(
    context: Context,
    *,
    verbose: bool = True,
    ensure_config_dir: bool = False
) -> Store:
    store = load_store(
        context,
        verbose=verbose,
        ensure_config_dir=ensure_config_dir,
    )
    if verbose:
        click.echo(f"Loading manifest {store.manifest.name}...")
    commands.load(context, store.manifest)
    commands.link(context, store.manifest)
    commands.check(context, store.manifest)
    commands.wait(context, store)
    commands.prepare(context, store.manifest)
    return store
