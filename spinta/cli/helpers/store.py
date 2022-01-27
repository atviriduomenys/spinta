from pathlib import Path

import click

from spinta import commands
from spinta.auth import auth_server_keys_exists
from spinta.auth import client_exists
from spinta.auth import create_client_file
from spinta.auth import gen_auth_server_keys
from spinta.components import Config
from spinta.components import Context
from spinta.components import Store
from spinta.core.config import DEFAULT_CONFIG_PATH


def _ensure_config_dir(
    path: Path,
    default_auth_client: str,
    *,
    verbose: bool = True,
):
    """Create default config_path directory if it does not exits"""

    # If a non default config directory is given, it should exist.
    if not path.exists() and path != DEFAULT_CONFIG_PATH:
        raise Exception(f"Config dir {path} does not exist!")

    # Ensure clients directory.
    clients_path = (path / 'clients')
    clients_path.mkdir(parents=True, exist_ok=True)

    # Ensure auth server keys.
    if not auth_server_keys_exists(path):
        if verbose:
            click.echo(f"Initializing auth server keys: {path}")
        gen_auth_server_keys(path)

    # Ensure default client.
    if not client_exists(clients_path, default_auth_client):
        if verbose:
            click.echo(f"Initializing default auth client: {path}")
        create_client_file(
            clients_path,
            default_auth_client,
            secret=None,
            scopes=[
                'spinta_getall',
                'spinta_getone',
                'spinta_search',
                'spinta_changes',
            ],
        )


def load_config(
    context: Context,
    *,
    verbose: bool = True,
    ensure_config_dir: bool = False,
) -> Config:
    config = context.get('config')
    commands.load(context, config)
    if ensure_config_dir:
        _ensure_config_dir(
            config.config_path,
            config.default_auth_client,
            verbose=verbose,
        )
    commands.check(context, config)
    return config


def load_store(
    context: Context,
    *,
    verbose: bool = True,
    ensure_config_dir: bool = False,
) -> Store:
    load_config(
        context,
        verbose=verbose,
        ensure_config_dir=ensure_config_dir,
    )
    store = context.get('store')
    commands.load(context, store)
    return store


def load_manifest(
    context: Context,
    *,
    store: Store = None,
    verbose: bool = True,
    ensure_config_dir: bool = False,
    rename_duplicates: bool = False,
    load_internal: bool = True,
) -> Store:
    if store is None:
        store = load_store(
            context,
            verbose=verbose,
            ensure_config_dir=ensure_config_dir,
        )
    if verbose:
        if store.manifest.path:
            click.echo(
                f"Loading {type(store.manifest).__name__} "
                f"manifest {store.manifest.name} "
                f"({store.manifest.path})..."
            )
        else:
            click.echo(
                f"Loading {type(store.manifest).__name__} "
                f"manifest {store.manifest.name}"
            )
    commands.load(
        context, store.manifest,
        rename_duplicates=rename_duplicates,
        load_internal=load_internal,
    )
    commands.link(context, store.manifest)
    commands.check(context, store.manifest)
    return store


def prepare_manifest(
    context: Context,
    *,
    verbose: bool = True,
    ensure_config_dir: bool = False,
) -> Store:
    store = load_manifest(
        context,
        verbose=verbose,
        ensure_config_dir=ensure_config_dir,
    )
    commands.wait(context, store)
    commands.prepare(context, store.manifest)
    return store
