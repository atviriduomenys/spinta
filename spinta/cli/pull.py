import asyncio
import pathlib
import types
from typing import List
from typing import Optional

import click
import tqdm

from spinta import commands
from spinta import exceptions
from spinta.cli import main
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.store import prepare_store
from spinta.commands.formats import Format
from spinta.commands.write import write
from spinta.components import Context
from spinta.components import DataStream
from spinta.components import Model
from spinta.datasets.components import Dataset
from spinta.manifests.components import Manifest
from spinta.utils.aiotools import alist


@main.command(help='Pull data from an external dataset.')
@click.argument('dataset')
@click.option('--model', '-m', multiple=True, help="Pull only specified models.")
@click.option('--push', is_flag=True, default=False, help="Write pulled data to database.")
@click.option('--export', '-e', help="Export pulled data to a file or stdout. For stdout use 'stdout:<fmt>', where <fmt> can be 'csv' or other supported format.")
@click.pass_context
def pull(ctx, dataset, model, push, export):
    context = ctx.obj
    store = prepare_store(context)
    manifest = store.manifest
    if dataset in manifest.objects['dataset']:
        dataset = manifest.objects['dataset'][dataset]
    else:
        raise click.ClickException(str(exceptions.NodeNotFound(manifest, type='dataset', name=dataset)))

    if model:
        models = []
        for model in model:
            if model not in manifest.models:
                raise click.ClickException(str(exceptions.NodeNotFound(manifest, type='model', name=model)))
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
                    raise click.UsageError(f"unknown export file type {fmt!r}")

                exporter = config.exporters[fmt]

            asyncio.get_event_loop().run_until_complete(
                _process_stream(dataset.name, stream, exporter, path)
            )

    except exceptions.BaseError as e:
        print()
        raise click.ClickException(str(e))


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


async def _process_stream(
    source: str,
    stream: DataStream,
    exporter: Optional[Format] = None,
    path: Optional[pathlib.Path] = None,
) -> None:
    if exporter:
        # TODO: Probably exporters should support async generators.
        if isinstance(stream, types.AsyncGeneratorType):
            stream = await alist(stream)
        chunks = exporter(stream)
        if path is None:
            for chunk in chunks:
                print(chunk, end='')
        else:
            with path.open('wb') as f:
                for chunk in chunks:
                    f.write(chunk)
    else:
        with tqdm.tqdm(desc=source, ascii=True) as pbar:
            async for _ in stream:
                pbar.update(1)
