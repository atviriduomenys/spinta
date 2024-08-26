import logging
import pathlib
from typing import List
from typing import Optional

import requests
from typer import Argument
from typer import Context as TyperContext
from typer import Exit
from typer import Option
from typer import echo

from spinta import exceptions, commands
from spinta.auth import get_client_id_from_name, get_clients_path
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.data import ensure_data_dir
from spinta.cli.helpers.errors import ErrorCounter
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.push.components import State
from spinta.cli.helpers.push.write import push as push_
from spinta.cli.helpers.push.utils import extract_dependant_nodes, update_page_values_for_models
from spinta.cli.helpers.push.read import read_rows
from spinta.cli.helpers.push.state import init_push_state
from spinta.cli.helpers.push.sync import sync_push_state
from spinta.cli.helpers.store import prepare_manifest
from spinta.client import get_access_token
from spinta.client import get_client_credentials
from spinta.components import Action, Context, Store
from spinta.components import Config
from spinta.components import Mode
from spinta.core.context import configure_context
from spinta.datasets.keymaps.sync import sync_keymap
from spinta.manifests.components import Manifest
from spinta.types.namespace import sort_models_by_ref_and_base
from spinta.utils.units import tobytes
from spinta.utils.units import toseconds

log = logging.getLogger(__name__)


def push(
    ctx: TyperContext,
    manifests: Optional[List[str]] = Argument(None, help=(
        "Source manifest files to copy from"
    )),
    output: Optional[str] = Option(None, '-o', '--output', help=(
        "Output data to a given location, by default outputs to stdout"
    )),
    credentials: str = Option(None, '--credentials', help=(
        "Credentials file, defaults to {config_path}/credentials.cfg"
    )),
    dataset: str = Option(None, '-d', '--dataset', help=(
        "Push only specified dataset"
    )),
    auth: str = Option(None, '-a', '--auth', help=(
        "Authorize as a client, defaults to {default_auth_client}"
    )),
    limit: int = Option(None, help=(
        "Limit number of rows read from each model"
    )),
    chunk_size: str = Option('1m', help=(
        "Push data in chunks (1b, 1k, 2m, ...), default: 1m"
    )),
    stop_time: str = Option(None, help=(
        "Stop pushing after given time (1s, 1m, 2h, ...), by default does not "
        "stops until all data is pushed"
    )),
    stop_row: int = Option(None, help=(
        "Stop after pushing n rows, by default does not stop until all data "
        "is pushed"
    )),
    state: pathlib.Path = Option(None, help=(
        "Save push state into a file, by default state is saved to "
        "{data_path}/push/{remote}.db SQLite database file"
    )),
    mode: Mode = Option('external', help=(
        "Mode of backend operation, default: external"
    )),
    dry_run: bool = Option(False, '--dry-run', help=(
        "Read data to be pushed, but do not push or write data to the "
        "destination."
    )),
    stop_on_error: bool = Option(False, '--stop-on-error', help=(
        "Exit immediately on first error."
    )),
    no_progress_bar: bool = Option(False, '--no-progress-bar', help=(
        "Skip counting total rows to improve performance."
    )),
    retry_count: int = Option(5, '--retries', help=(
        "Repeat push until this count if there are errors."
    )),
    max_error_count: int = Option(50, '--max-errors', help=(
        "If errors exceed given number, push command will be stopped."
    )),
    incremental: bool = Option(False, '-i', '--incremental', help=(
        "Do an incremental push, only pushing objects from last page."
    )),
    page: Optional[List[str]] = Option(None, '--page', help=(
        "Page value from which rows will be pushed."
    )),
    page_model: str = Option(None, '--model', help=(
        "Model of the page value."
    )),
    synchronize: bool = Option(False, '--sync', help=(
        "Synchronize push state and keymap, in {data_path}/push/{remote}.db and {data_path}/keymap.db"
    )),
):
    """Push data to external data store"""
    synchronize_keymap = synchronize
    synchronize_state = synchronize

    if chunk_size:
        chunk_size = tobytes(chunk_size)

    if stop_time:
        stop_time = toseconds(stop_time)

    manifests = convert_str_to_manifest_path(manifests)
    context = configure_context(ctx.obj, manifests, mode=mode)
    store = prepare_manifest(context, full_load=True)
    config: Config = context.get('config')

    if credentials:
        credsfile = pathlib.Path(credentials)
        if not credsfile.exists():
            echo(f"Credentials file {credsfile} does not exit.")
            raise Exit(code=1)
    else:
        credsfile = config.credentials_file
    # TODO: Read client credentials only if a Spinta URL is given.
    creds = get_client_credentials(credsfile, output)

    if not state:
        ensure_data_dir(config.data_path / 'push')
        state = config.data_path / 'push' / f'{creds.remote}.db'

    state = f'sqlite+spinta:///{state}'

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
        auth_client = auth or config.default_auth_client
        auth_client = get_client_id_from_name(get_clients_path(config), auth_client)
        require_auth(context, auth_client)

        _attach_backends(context, store, manifest)
        _attach_keymaps(context, store)
        error_counter = ErrorCounter(max_count=max_error_count)

        models = commands.traverse_ns_models(context, ns, manifest, Action.SEARCH, dataset_=dataset, source_check=True)
        models = sort_models_by_ref_and_base(list(models))

        if state:
            state = State(*init_push_state(state, models))
            context.attach('push.state.conn', state.engine.begin)

        # Synchronize keymaps
        with manifest.keymap as km:
            first_time = km.first_time_sync()
            if first_time:
                synchronize_keymap = True
            dependant_models = extract_dependant_nodes(context, models, not synchronize_keymap)
            sync_keymap(
                context=context,
                keymap=km,
                client=client,
                server=creds.server,
                models=dependant_models,
                error_counter=error_counter,
                no_progress_bar=no_progress_bar,
                reset_cid=synchronize_keymap
            )

        # Synchronize push state
        if synchronize_state:
            sync_push_state(
                context=context,
                models=models,
                client=client,
                server=creds.server,
                error_counter=error_counter,
                no_progress_bar=no_progress_bar,
                metadata=state.metadata
            )

        update_page_values_for_models(context, state.metadata, models, incremental, page_model, page)

        rows = read_rows(
            context,
            client,
            creds.server,
            models,
            state,
            limit,
            stop_on_error=stop_on_error,
            retry_count=retry_count,
            no_progress_bar=no_progress_bar,
            error_counter=error_counter,
        )

        push_(
            context,
            client,
            creds.server,
            models,
            rows,
            state=state,
            stop_time=stop_time,
            stop_row=stop_row,
            chunk_size=chunk_size,
            dry_run=dry_run,
            stop_on_error=stop_on_error,
            error_counter=error_counter,
        )

        if error_counter.has_errors():
            raise Exit(code=1)


def _attach_backends(context: Context, store: Store, manifest: Manifest) -> None:
    context.attach('transaction', manifest.backend.transaction)
    backends = set()
    for backend in store.backends.values():
        backends.add(backend.name)
        context.attach(f'transaction.{backend.name}', backend.begin)
    for backend in manifest.backends.values():
        backends.add(backend.name)
        context.attach(f'transaction.{backend.name}', backend.begin)
    for dataset_ in commands.get_datasets(context, manifest).values():
        for resource in dataset_.resources.values():
            if resource.backend and resource.backend.name not in backends:
                backends.add(resource.backend.name)
                context.attach(f'transaction.{resource.backend.name}', resource.backend.begin)


def _attach_keymaps(context: Context, store: Store) -> None:
    for keymap in store.keymaps.values():
        context.attach(f'keymap.{keymap.name}', lambda: keymap)
