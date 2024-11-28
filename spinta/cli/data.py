import asyncio
import itertools
import json
from typing import Optional, List

from typer import Argument
from typer import Context as TyperContext
from typer import Exit
from typer import Option
from typer import echo

from spinta import commands
from spinta.backends import Backend
from spinta.backends.helpers import validate_and_return_transaction
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.data import process_stream, count_rows
from spinta.cli.helpers.export.components import CounterManager
from spinta.cli.helpers.export.helpers import validate_and_return_shallow_backend, validate_and_return_formatter, \
    export_data
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.store import prepare_manifest, attach_backends, attach_keymaps
from spinta.commands.write import write
from spinta.components import Mode, Config, Action
from spinta.core.context import configure_context
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
    synchronize: bool = Option(False, '--sync', help=(
        "Synchronize keymap, in {data_path}/keymap.db"
    )),
    dataset: str = Option(None, '-d', '--dataset', help=(
        "Extract only specified dataset"
    )),
    no_progress_bar: bool = Option(False, '--no-progress-bar', help=(
        "Skip counting total rows to improve performance."
    )),
):
    manifests = convert_str_to_manifest_path(manifests)
    context = configure_context(ctx.obj, manifests, mode=mode)
    store = prepare_manifest(context, full_load=True)
    config: Config = context.get('config')

    if backend and fmt:
        echo("Export can only output to one type (either `--backend` or `--format`), but not both.")
        raise Exit(code=1)

    if not backend and not fmt:
        echo("Export must be given an output format (either `--backend` or `--format`).")
        raise Exit(code=1)

    if backend:
        backend: Backend = validate_and_return_shallow_backend(context, backend)

    if fmt:
        fmt: Format = validate_and_return_formatter(context, fmt)

    manifest = store.manifest
    if dataset and not commands.has_dataset(context, manifest, dataset):
        echo(str(NodeNotFound(manifest, type='dataset', name=dataset)))
        raise Exit(code=1)

    if not output:
        echo("Output argument is required (`--output`).")
        raise Exit(code=1)

    with context:
        require_auth(context)

        attach_backends(context, store, manifest)
        attach_keymaps(context, store)
        ns = commands.get_namespace(context, manifest, '')
        models = commands.traverse_ns_models(context, ns, manifest, Action.SEARCH, dataset_=dataset, source_check=True)
        models = sort_models_by_ref_and_base(list(models))

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


        # Synchronize keymaps
        # with manifest.keymap as km:
        #     first_time = km.first_time_sync()
        #     if first_time:
        #         synchronize_keymap = True
        #     dependant_models = extract_dependant_nodes(context, models, not synchronize_keymap)
        #     sync_keymap(
        #         context=context,
        #         keymap=km,
        #         client=client,
        #         server=creds.server,
        #         models=dependant_models,
        #         error_counter=error_counter,
        #         no_progress_bar=no_progress_bar,
        #         reset_cid=synchronize_keymap
        #     )
