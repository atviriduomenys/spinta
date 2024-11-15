import asyncio
import itertools
import json
from typing import Optional, List, Iterator

from typer import Context as TyperContext
from typer import Option
from typer import echo
from typer import Exit
from typer import Argument

from spinta import commands
from spinta.backends import Backend
from spinta.backends.helpers import validate_and_return_transaction
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.export.helpers import validate_and_return_shallow_backend, validate_and_return_formatter
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.store import prepare_manifest
from spinta.cli.helpers.data import process_stream
from spinta.cli.push import _attach_keymaps
from spinta.commands.write import write
from spinta.components import Mode, Config, Action, Model, Context, pagination_enabled
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
        "Authorize as a client"
    )),
    fmt: Optional[str] = Option(None, '-f', '--format', help=(
        "Authorize as a client"
    )),
    output: Optional[str] = Option(None, '-o', '--output', help=(
        "Authorize as a client"
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
):
    manifests = convert_str_to_manifest_path(manifests)
    context = configure_context(ctx.obj, manifests, mode=mode)
    store = prepare_manifest(context, full_load=True)
    config: Config = context.get('config')

    if backend and fmt:
        raise Exception("CAN ONLY HAVE ONE")

    if not backend and not fmt:
        raise Exception("NEED TO GIVE EITHER BACKEND OR FMT")

    if backend:
        backend: Backend = validate_and_return_shallow_backend(context, backend)

    if fmt:
        fmt: Format = validate_and_return_formatter(context, fmt)

    manifest = store.manifest
    if dataset and not commands.has_dataset(context, manifest, dataset):
        echo(str(NodeNotFound(manifest, type='dataset', name=dataset)))
        raise Exit(code=1)

    with context:
        require_auth(context)

        _attach_keymaps(context, store)
        ns = commands.get_namespace(context, manifest, '')
        models = commands.traverse_ns_models(context, ns, manifest, Action.SEARCH, dataset_=dataset, source_check=True)
        models = sort_models_by_ref_and_base(list(models))
        for model in models:
            print(model)

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


def read_rows(
    context: Context,
    model: Model
) -> Iterator[dict]:
    if pagination_enabled(model):
        rows = read_with_page()

def read_with_page(
    context: Context,
    model: Model
):
    pass
