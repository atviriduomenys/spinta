import asyncio
import pathlib
from typing import List
from typing import Optional

from typer import Context as TyperContext
from typer import Option

from spinta import commands
from spinta import exceptions
from spinta.backends.helpers import validate_and_return_transaction
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.data import process_stream
from spinta.cli.helpers.errors import cli_error
from spinta.cli.helpers.store import prepare_manifest
from spinta.commands.write import write
from spinta.components import Context
from spinta.components import Model
from spinta.datasets.components import Dataset
from spinta.manifests.components import Manifest


def _get_dataset_models(context: Context, manifest: Manifest, dataset: Dataset):
    for model in commands.get_models(context, manifest).values():
        if model.external and model.external.dataset and model.external.dataset.name == dataset.name:
            yield model


def _pull_models(context: Context, models: List[Model]):
    for model in models:
        external = model.external
        backend = external.resource.backend
        rows = commands.getall(context, external, backend)
        yield from rows


def pull(
    ctx: TyperContext,
    dataset: str,
    model: Optional[List[str]] = Option(None, '-m', '--model', help=(
        "Pull only specified models."
    )),
    push: bool = Option(False, help="Write pulled data to database"),
    export: str = Option(None, '-e', '--export', help=(
        "Export pulled data to a file or stdout. For stdout use "
        "'stdout:<fmt>', where <fmt> can be 'csv' or other supported format."
    )),
):
    """Pull data from an external data source."""
    context = ctx.obj
    store = prepare_manifest(context)
    manifest = store.manifest
    if commands.has_namespace(context, manifest, dataset):
        dataset = commands.get_dataset(context, manifest, dataset)
    else:
        cli_error(
            str(exceptions.NodeNotFound(manifest, type='dataset', name=dataset))
        )

    if model:
        models = []
        for model in model:
            if not commands.has_model(context, manifest, model):
                cli_error(
                    str(exceptions.NodeNotFound(manifest, type='model', name=model))
                )
            models.append(commands.get_model(context, manifest, model))
    else:
        models = _get_dataset_models(context, manifest, dataset)

    try:
        with context:
            require_auth(context)
            backend = store.backends['default']
            context.attach('transaction', validate_and_return_transaction, context, backend, write=push)

            path = None
            exporter = None

            stream = _pull_models(context, models)
            if push:
                root = commands.get_namespace(context, manifest, '')
                stream = write(context, root, stream, changed=True)

            if export is None and push is False:
                export = 'stdout'

            if export:
                if export == 'stdout':
                    fmt = 'ascii'
                elif export.startswith('stdout:'):
                    fmt = export.split(':', 1)[1]
                else:
                    path = pathlib.Path(export)
                    fmt = export.suffix.strip('.')

                config = context.get('config')

                if fmt not in config.exporters:
                    cli_error(
                        f"unknown export file type {fmt!r}"
                    )

                exporter = config.exporters[fmt]

            asyncio.get_event_loop().run_until_complete(
                process_stream(dataset.name, stream, exporter, path)
            )

    except exceptions.BaseError as e:
        cli_error(
            str(e)
        )
