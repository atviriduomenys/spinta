import logging
import pathlib
from typing import List, Optional

import requests
from typer import Argument, Option, Typer, echo, Exit
from typer import Context as TyperContext

from spinta import commands, exceptions
from spinta.auth import get_client_id_from_name, get_clients_path
from spinta.backends.helpers import validate_and_return_transaction, validate_and_return_begin
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.data import ensure_data_dir

from spinta.cli.helpers.errors import ErrorCounter
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.push.components import State
from spinta.cli.helpers.push.state import init_push_state
from spinta.cli.helpers.push.sync import sync_push_state
from spinta.cli.helpers.push.utils import extract_dependant_nodes, update_page_values_for_models

from spinta.cli.helpers.store import prepare_manifest
from spinta.client import get_access_token, get_client_credentials
from spinta.components import Action, Context, Store, Config, Mode
from spinta.core.context import configure_context
from spinta.datasets.keymaps.sync import sync_keymap
from spinta.manifests.components import Manifest
from spinta.types.namespace import sort_models_by_ref_and_base


log = logging.getLogger(__name__)

keymap = Typer()

@keymap.command('sync', short_help="Sync keymap from external data source")
def keymap_sync(
    ctx: TyperContext,
    manifests: Optional[List[str]] = Argument(None, help=(
        "Source manifest files to copy from"
    )),
    input_source: str = Option(None, '-i', '--input', help=(
        "Input source file"
    )),
    credentials: str = Option(None, '--credentials', help=(
        "Credentials file, defaults to {config_path}/credentials.cfg"
    )),
    dataset: str = Option(None, '-d', '--dataset', help=(
        "Sync only specified dataset"
    )),
    dry_run: bool = Option(False, '--dry-run', help=(
        "Do not write data to keymap, but do everything else, that does not change anything."
    )),
    max_error_count: int = Option(50, '--max-errors', help=(
            "If errors exceed given number, push command will be stopped."
        )),
    mode: Mode = Option('external', help=(
        "Mode of backend operation, default: external"
    )),
):
    """Sync keymap from external data source"""
    if not input_source:
        echo("Input source is required.")
        raise Exit(code=1)

    manifests = convert_str_to_manifest_path(manifests)
    context = configure_context(ctx.obj, manifests, mode=mode)
    store = prepare_manifest(context, full_load=True)
    config: Config = context.get('config')

    if credentials:
        credsfile = pathlib.Path(credentials)
        if not credsfile.exists():
            echo(f"Credentials file {credsfile} does not exist.")
            raise Exit(code=1)
    else:
        credsfile = config.credentials_file

    creds = get_client_credentials(credsfile, input_source)

    manifest = store.manifest
    if dataset and not commands.has_dataset(context, manifest, dataset):
        echo(str(exceptions.NodeNotFound(manifest, type='dataset', name=dataset)))
        raise Exit(code=1)

    ns = commands.get_namespace(context, manifest, '')

    echo(f"Get access token from {creds.server}")
    token = get_access_token(creds)

    client = requests.Session()
    client.headers['Content-Type'] = 'application/json'
    client.headers['Authorization'] = f'Bearer {token}'

    with context:
        auth_client = get_client_id_from_name(get_clients_path(config), config.default_auth_client)
        require_auth(context, auth_client)

        _attach_backends(context, store, manifest)
        _attach_keymaps(context, store)
        error_counter = ErrorCounter(max_count=max_error_count)

        models = commands.traverse_ns_models(context, ns, manifest, Action.SEARCH, dataset_=dataset, source_check=True)
        models = sort_models_by_ref_and_base(list(models))

        # Synchronize keymaps
        with manifest.keymap as km:
            dependant_models = extract_dependant_nodes(context, models, False)
            sync_keymap(
                context=context,
                keymap=km,
                client=client,
                server=creds.server,
                models=dependant_models,
                error_counter=error_counter,
                no_progress_bar=False,
                reset_cid=True,
#               dry_run=dry_run
            )

        if error_counter.has_errors():
            raise Exit(code=1)


def _attach_backends(context: Context, store: Store, manifest: Manifest) -> None:
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


def _attach_keymaps(context: Context, store: Store) -> None:
    for keymap in store.keymaps.values():
        context.attach(f'keymap.{keymap.name}', lambda: keymap)
