import uuid
from pathlib import Path

import click

from spinta import commands
from spinta.auth import auth_server_keys_exists, client_name_exists, get_clients_path, ensure_client_folders_exist
from spinta.auth import create_client_file
from spinta.auth import gen_auth_server_keys
from spinta.backends.helpers import validate_and_return_transaction, validate_and_return_begin
from spinta.cli.helpers.upgrade.clients import requires_client_migration
from spinta.components import Config
from spinta.components import Context
from spinta.components import Store
from spinta.core.config import DEFAULT_CONFIG_PATH
from spinta.exceptions import ClientsMigrationRequired
from spinta.manifests.components import Manifest


def _ensure_config_dir(
    config: Config,
    path: Path,
    default_auth_client: str,
    *,
    verbose: bool = True,
):
    """Create default config_path directory if it does not exits"""

    # If a non default config directory is given, it should exist.
    if not path.exists() and path != DEFAULT_CONFIG_PATH:
        raise Exception(f"Config dir {path} does not exist!")


    clients_path = get_clients_path(config)
    # Check if client migrations are needed
    if requires_client_migration(clients_path):
        raise ClientsMigrationRequired()

    # Ensure all files/folders exist for clients operations
    ensure_client_folders_exist(clients_path)

    # Ensure auth server keys.
    if not auth_server_keys_exists(path):
        if verbose:
            click.echo(f"Initializing auth server keys: {path}")
        gen_auth_server_keys(path)

    # Ensure default client.
    if default_auth_client and not client_name_exists(clients_path, default_auth_client):
        if verbose:
            click.echo(f"Initializing default auth client: {path}")
        create_client_file(
            clients_path,
            default_auth_client,
            str(uuid.uuid4()),
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
            config,
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
    full_load: bool = False
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
        full_load=full_load
    )
    commands.link(context, store.manifest)
    commands.check(context, store.manifest)
    return store


def prepare_manifest(
    context: Context,
    *,
    verbose: bool = True,
    ensure_config_dir: bool = False,
    full_load: bool = False
) -> Store:
    store = load_manifest(
        context,
        verbose=verbose,
        ensure_config_dir=ensure_config_dir,
        full_load=full_load
    )
    commands.wait(context, store)
    commands.prepare(context, store.manifest)
    return store


def attach_backends(context: Context, store: Store, manifest: Manifest) -> None:
    context.attach('transaction', validate_and_return_transaction, context, manifest.backend)
    backends = set()
    for backend in store.backends.values():
        backends.add(backend.name)
        context.attach(f'transaction.{backend.name}', validate_and_return_begin, context, backend)
    for backend in manifest.backends.values():
        backends.add(backend.name)
        context.attach(f'transaction.{backend.name}', validate_and_return_begin, context, backend)
    for dataset_ in commands.get_datasets(context, manifest).values():
        for resource in dataset_.resources.values():
            if resource.backend and resource.backend.name not in backends:
                backends.add(resource.backend.name)
                context.attach(f'transaction.{resource.backend.name}', validate_and_return_begin, context, resource.backend)


def attach_keymaps(context: Context, store: Store) -> None:
    for keymap in store.keymaps.values():
        context.attach(f'keymap.{keymap.name}', lambda: keymap)
