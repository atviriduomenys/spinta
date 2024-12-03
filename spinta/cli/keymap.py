import logging
import pathlib
from typing import List, Optional

import requests
from typer import Argument, Option, Typer, echo, Exit
from typer import Context as TyperContext

from spinta import commands, exceptions
from spinta.auth import get_client_id_from_name, get_clients_path
from spinta.cli.helpers.auth import require_auth

from spinta.cli.helpers.errors import ErrorCounter, cli_error
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.push.utils import extract_dependant_nodes

from spinta.cli.helpers.store import prepare_manifest, attach_backends, attach_keymaps
from spinta.client import get_access_token, get_client_credentials
from spinta.components import Action, Config, Mode
from spinta.core.context import configure_context
from spinta.datasets.keymaps.sync import sync_keymap
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
        "Run keymap sync in dry-run mode, no changes will be made."
    )),
    max_error_count: int = Option(50, '--max-errors', help=(
        "If errors exceed given number, keymap command will be stopped."
    )),
    mode: Mode = Option('external', help=(
        "Mode of backend operation, default: external"
    )),
    no_progress_bar: bool = Option(False, '--no-progress-bar', help=(
        "Skip counting total rows to improve performance."
    )),
    read_timeout: float = Option(300, '--read-timeout', help=(
        "Timeout for reading a response, default: 5 minutes (300s). The value is in seconds."
    )),
    connect_timeout: float = Option(5, '--connect-timeout', help=(
        "Timeout for connecting, default: 5 seconds."
    )),
):
    """Sync keymap from external data source"""
    if not input_source:
        cli_error(
            "Input source is required."
        )

    manifests = convert_str_to_manifest_path(manifests)
    context = configure_context(ctx.obj, manifests, mode=mode)
    store = prepare_manifest(context, full_load=True)
    config: Config = context.get('config')

    if credentials:
        credsfile = pathlib.Path(credentials)
        if not credsfile.exists():
            cli_error(
                f"Credentials file {credsfile} does not exist."
            )
    else:
        credsfile = config.credentials_file

    creds = get_client_credentials(credsfile, input_source)

    manifest = store.manifest
    if dataset and not commands.has_dataset(context, manifest, dataset):
        cli_error(
            str(exceptions.NodeNotFound(manifest, type='dataset', name=dataset))
        )

    ns = commands.get_namespace(context, manifest, '')

    echo(f"Get access token from {creds.server}")
    token = get_access_token(creds)

    client = requests.Session()
    client.headers['Content-Type'] = 'application/json'
    client.headers['Authorization'] = f'Bearer {token}'

    with context:
        auth_client = get_client_id_from_name(get_clients_path(config), config.default_auth_client)
        require_auth(context, auth_client)

        attach_backends(context, store, manifest)
        attach_keymaps(context, store)
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
                no_progress_bar=no_progress_bar,
                reset_cid=True,
                dry_run=dry_run,
                timeout=(connect_timeout, read_timeout)
            )

        if error_counter.has_errors():
            raise Exit(code=1)
