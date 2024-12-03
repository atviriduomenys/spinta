from typing import Iterable, AsyncIterator, List, Any

from spinta import commands
from spinta.backends import Backend
from spinta.cli.helpers.data import read_model_data
from spinta.cli.helpers.errors import cli_error
from spinta.cli.helpers.export.components import CounterManager
from spinta.commands.read import get_page
from spinta.components import Context, Model, pagination_enabled, DataItem, Action
from spinta.formats.components import Format
from spinta.ufuncs.basequerybuilder.components import QueryParams


def validate_and_return_shallow_backend(context: Context, type_: str) -> Backend:
    """
    Validates and returns shallow backend.
    Shallow backend, is not loaded and is only used for type checking
    """
    config = context.get("config")
    backends = config.components['backends']
    if type_ not in backends:
        cli_error(
            f"Unknown output backend {type_!r}, from available: {list(backends.keys())}"
        )

    backend = config.components['backends'][type_]()
    backend.type = type_
    return backend


def validate_and_return_formatter(context: Context, type_: str) -> Format:
    config = context.get("config")
    exporters = config.exporters

    if type_ not in exporters:
        cli_error(
            f"Unknown output format {type_!r}, from available: {list(exporters.keys())}"
        )

    fmt = config.exporters[type_]
    return fmt


def get_data(context: Context, model: Model):
    params = QueryParams()
    params.push = True

    if pagination_enabled(model):
        page = commands.create_page(model.page)
        rows = get_page(
            context,
            model,
            model.backend,
            page,
            [],
            params=params
        )
    else:
        rows = read_model_data(
            context,
            model,
            params=params
        )
    yield from rows


def create_data_items(model: Model, backend: Backend, data: Iterable):
    for item in data:
        yield DataItem(
            model=model,
            backend=backend,
            action=Action.INSERT,
            payload=item
        )


async def prepare_data_without_checks(
    context: Context,
    dstream: AsyncIterator[DataItem],
):
    async for data in dstream:
        data.payload = commands.rename_metadata(context, data.payload)
        data.given = commands.load(context, data.model, data.payload)
        yield data


async def export_data(
    context: Context,
    models: List[Model],
    fmt: Any,
    output: str,
    counter: CounterManager,
):
    txn = commands.gen_object_id(context, fmt)
    for model in models:
        data = get_data(context, model)
        await commands.export_data(
            context,
            model,
            fmt,
            data=data,
            path=output,
            counter=counter,
            txn=txn
        )
        counter.close_model(model)
