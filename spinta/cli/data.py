import asyncio
import itertools
import json
import pathlib
from typing import Optional, List

import requests
from typer import Argument
from typer import Context as TyperContext
from typer import Option
from typer import echo

from spinta import commands
from spinta.backends import Backend
from spinta.backends.helpers import validate_and_return_transaction
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.data import process_stream, count_rows
from spinta.cli.helpers.errors import cli_error, ErrorCounter
from spinta.cli.helpers.export.components import CounterManager
from spinta.cli.helpers.export.helpers import validate_and_return_shallow_backend, validate_and_return_formatter, \
    export_data
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.push.utils import extract_dependant_nodes
from spinta.cli.helpers.store import prepare_manifest, attach_backends, attach_keymaps
from spinta.client import get_client_credentials, get_access_token
from spinta.commands.write import write
from spinta.components import Mode, Config, Action
from spinta.core.context import configure_context
from spinta.datasets.keymaps.sync import sync_keymap
from spinta.exceptions import NodeNotFound
from spinta.formats.components import Format
from spinta.types.namespace import sort_models_by_ref_and_base


def import_(
    ctx: TyperContext,
    source: str,
    auth: Optional[str] = Option(None, '-a', '--auth', help=(
        "Authorize as a client"
    )),
    limit: Optional[int] = Option(None, help=(
        "Limit number of rows read from source."
    )),
):
    """Import data from a file"""
    context = ctx.obj
    store = prepare_manifest(context)
    manifest = store.manifest
    root = commands.get_namespace(context, manifest, '')

    with context:
        require_auth(context, auth)
        context.attach('transaction', validate_and_return_transaction, context, manifest.backend, write=True)
        with open(source) as f:
            stream = (json.loads(line.strip()) for line in f)
            stream = itertools.islice(stream, limit) if limit else stream
            stream = write(context, root, stream, changed=True)
            coro = process_stream(source, stream)
            asyncio.get_event_loop().run_until_complete(coro)


def export_(
    ctx: TyperContext,
    manifests: Optional[List[str]] = Argument(None, help=(
        "Source manifest files to copy from"
    )),
    backend: Optional[str] = Option(None, '-b', '--backend', help=(
        "Backend format that will import the exported data ('postgresql', 'sql', ...)"
    )),
    fmt: Optional[str] = Option(None, '-f', '--format', help=(
        "Output format ('csv', 'html, 'json', ...)"
    )),
    output: Optional[str] = Option(None, '-o', '--output', help=(
        "Output path"
    )),
    mode: Mode = Option('external', help=(
        "Mode of backend operation, default: external"
    )),
    dataset: str = Option(None, '-d', '--dataset', help=(
        "Extract only specified dataset"
    )),
    no_progress_bar: bool = Option(False, '--no-progress-bar', help=(
        "Skip counting total rows to improve performance."
    )),
    credentials: str = Option(None, '--credentials', help=(
        "Credentials file, defaults to {config_path}/credentials.cfg."
    )),
    input_source: str = Option(None, '-i', '--input', help=(
        "Input source used for synchronizing keymap (credentials entry), if not given, it will skip synchronization step."
    )),
    ignore_sync_cid: bool = Option(False, '--sync-full', help=(
        "Ignores sync keymap optimization and fully synchronizes data (does not start from last synchronized row)."
    )),
    max_error_count: int = Option(50, '--max-errors', help=(
        "If errors exceed given number, export command will be stopped."
    )),
    read_timeout: float = Option(300, '--read-timeout', help=(
        "Timeout for reading a response, default: 5 minutes (300s). The value is in seconds."
    )),
    connect_timeout: float = Option(5, '--connect-timeout', help=(
        "Timeout for connecting, default: 5 seconds."
    )),
):
    manifests = convert_str_to_manifest_path(manifests)
    context = configure_context(ctx.obj, manifests, mode=mode)
    store = prepare_manifest(context, full_load=True)
    config: Config = context.get('config')

    if backend and fmt:
        cli_error(
            "Export can only output to one type (either `--backend` or `--format`), but not both."
        )

    if not backend and not fmt:
        cli_error(
            "Export must be given an output format (either `--backend` or `--format`)."
        )

    if backend:
        backend: Backend = validate_and_return_shallow_backend(context, backend)

    if fmt:
        fmt: Format = validate_and_return_formatter(context, fmt)

    manifest = store.manifest
    if dataset and not commands.has_dataset(context, manifest, dataset):
        cli_error(
            str(NodeNotFound(manifest, type='dataset', name=dataset))
        )

    if not output:
        cli_error(
            "Output argument is required (`--output`)."
        )
    commands.validate_export_output(context, fmt or backend, output)

    with context:
        require_auth(context)
        error_counter = ErrorCounter(max_count=max_error_count)

        attach_backends(context, store, manifest)
        attach_keymaps(context, store)
        ns = commands.get_namespace(context, manifest, '')
        models = commands.traverse_ns_models(context, ns, manifest, Action.SEARCH, dataset_=dataset, source_check=True)
        models = sort_models_by_ref_and_base(list(models))

        # Synchronize only if input_source is given
        if input_source:
            if credentials:
                credsfile = pathlib.Path(credentials)
                if not credsfile.exists():
                    cli_error(
                        f"Credentials file {credsfile} does not exit."
                    )
            else:
                credsfile = config.credentials_file
            creds = get_client_credentials(credsfile, input_source)
            echo(f"Get access token from {creds.server}")
            token = get_access_token(creds)

            client = requests.Session()
            client.headers['Content-Type'] = 'application/json'
            client.headers['Authorization'] = f'Bearer {token}'

            # Synchronize keymaps
            with manifest.keymap as km:
                dependant_models = extract_dependant_nodes(context, models, True)
                sync_keymap(
                    context=context,
                    keymap=km,
                    client=client,
                    server=creds.server,
                    models=dependant_models,
                    error_counter=error_counter,
                    no_progress_bar=no_progress_bar,
                    reset_cid=ignore_sync_cid,
                    timeout=(connect_timeout, read_timeout)
                )
        else:
            dependant_models = extract_dependant_nodes(context, models, True)
            if dependant_models:
                cli_error((
                    f"Detected some models, that might require synchronization step: {', '.join([model.model_type() for model in dependant_models])}\n"
                    "Add --input argument to run export with incremental synchronization."
                ))
            echo("Input source not given, skipping synchronization step.")

        counts = count_rows(
            context,
            models,
            no_progress_bar=no_progress_bar
        )

        with CounterManager(
            enabled=not no_progress_bar,
            totals=counts
        ) as counter:
            asyncio.get_event_loop().run_until_complete(
                export_data(
                    context,
                    models,
                    fmt or backend,
                    output,
                    counter,
                )
            )
