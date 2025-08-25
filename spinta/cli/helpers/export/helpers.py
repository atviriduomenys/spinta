from collections.abc import Generator
from typing import Iterable, AsyncIterator, List, Any

from typer import echo

from spinta import commands
from spinta.backends import Backend
from spinta.cli.helpers.data import read_model_data
from spinta.cli.helpers.errors import cli_error
from spinta.cli.helpers.export.components import CounterManager
from spinta.commands import build_data_patch_for_export
from spinta.components import Context, Model, pagination_enabled, DataItem
from spinta.core.enums import Access, Action
from spinta.formats.components import Format
from spinta.ufuncs.querybuilder.components import QueryParams
from spinta.utils.schema import NA


def validate_and_return_shallow_backend(context: Context, type_: str) -> Backend:
    """
    Validates and returns shallow backend.
    Shallow backend, is not loaded and is only used for type checking
    """
    config = context.get("config")
    backends = config.components["backends"]
    if type_ not in backends:
        cli_error(f"Unknown output backend {type_!r}, from available: {list(backends.keys())}")

    backend = config.components["backends"][type_]()
    backend.type = type_
    return backend


def validate_and_return_formatter(context: Context, type_: str) -> Format:
    config = context.get("config")
    exporters = config.exporters

    if type_ not in exporters:
        cli_error(f"Unknown output format {type_!r}, from available: {list(exporters.keys())}")

    fmt = config.exporters[type_]
    return fmt


def get_data(context: Context, model: Model, access: Access):
    params = QueryParams()
    params.push = True

    page = None
    if pagination_enabled(model):
        page = commands.create_page(model.page)

    select_expr = None
    # Potentially we can also filter out all data with query
    # for now we only support `postgresql` output, which uses `prepare_export_patch`, that also does access filtering
    # in case we cannot do this with other outputs, we can also apply filtering with data fetch, but need to
    # make sure it all works together and all required data is fetched (for that reason currently we only use
    # `prepare_export_patch` to filter access data).

    # select_expr = Expr('select', Expr('bind', '_id'), *[
    #     Expr('bind', key) for key, prop in model.flatprops.items()
    #     if prop.access >= access and prop.name not in RESERVED_PROPERTY_NAMES
    # ])

    rows = read_model_data(context, model, params=params, page=page, query=select_expr)
    yield from rows


def create_data_items(model: Model, backend: Backend, data: Iterable):
    for item in data:
        yield DataItem(model=model, backend=backend, action=Action.INSERT, payload=item)


async def prepare_data_without_checks(
    context: Context,
    dstream: AsyncIterator[DataItem],
):
    async for data in dstream:
        data.payload = commands.rename_metadata(context, data.payload)
        data.given = commands.load(context, data.model, data.payload)
        yield data


async def prepare_export_patch(
    context: Context,
    dstream: AsyncIterator[DataItem],
    access: Access,
) -> AsyncIterator[DataItem]:
    # Truncated patch generation, fit for export command (fewer checks)
    # Main reason we use patch, is because `prepare_data_for_write` converts given data to backend specific data
    # It uses DataItem.patch to do it, so we need to generate one that also applies `export` command requirements.

    async for data in dstream:
        data.patch = build_data_patch_for_export(context, data.model or data.prop, given=data.given, access=access)
        if data.patch is NA:
            data.patch = {}

        if "_id" in data.given:
            data.patch["_id"] = data.given["_id"]
        else:
            data.patch["_id"] = commands.gen_object_id(context, data.model.backend, data.model)

        data.patch["_revision"] = commands.gen_object_id(context, data.model.backend, data.model)
        yield data


async def export_data(
    context: Context, models: List[Model], fmt: Any, output: str, counter: CounterManager, access: Access
):
    txn = commands.gen_object_id(context, fmt)
    for model in models:
        data = get_data(context, model, access)
        await commands.export_data(context, model, fmt, data=data, path=output, counter=counter, txn=txn, access=access)
        counter.close_model(model)


def filter_models_by_access_verbose(models: Generator[Model], access: Access) -> Generator[Model]:
    for model in models:
        if model.access < access:
            echo(
                f"Skipping '{model.model_type()}' model, access level ('{model.access.name}') is lower than required ('{access.name}')"
            )
            continue
        yield model
