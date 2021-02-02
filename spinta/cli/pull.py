import asyncio
import pathlib
from typing import List
from typing import Optional

from typer import Context as TyperContext
from typer import Exit
from typer import Option
from typer import echo

from spinta import commands
from spinta import exceptions
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.data import process_stream
from spinta.cli.helpers.store import prepare_manifest
from spinta.commands.write import write
from spinta.components import Context
from spinta.components import Model
from spinta.datasets.components import Dataset
from spinta.manifests.components import Manifest


def _get_dataset_models(manifest: Manifest, dataset: Dataset):
    for model in manifest.models.values():
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
    if dataset in manifest.objects['dataset']:
        dataset = manifest.objects['dataset'][dataset]
    else:
        echo(str(exceptions.NodeNotFound(manifest, type='dataset', name=dataset)))
        raise Exit(code=1)

    if model:
        models = []
        for model in model:
            if model not in manifest.models:
                echo(str(exceptions.NodeNotFound(manifest, type='model', name=model)))
                raise Exit(code=1)
            models.append(manifest.models[model])
    else:
        models = _get_dataset_models(manifest, dataset)

    try:
        with context:
            require_auth(context)
            backend = store.backends['default']
            context.attach('transaction', backend.transaction, write=push)

            path = None
            exporter = None

            stream = _pull_models(context, models)
            if push:
                root = manifest.objects['ns']['']
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
                    echo(f"unknown export file type {fmt!r}")
                    raise Exit(code=1)

                exporter = config.exporters[fmt]

            asyncio.get_event_loop().run_until_complete(
                process_stream(dataset.name, stream, exporter, path)
            )

    except exceptions.BaseError as e:
        echo(str(e))
        raise Exit(code=1)
