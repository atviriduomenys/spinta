import datetime
import pathlib
from contextlib import AsyncExitStack
from typing import Iterator

import aiofiles

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.cli.helpers.errors import cli_error
from spinta.cli.helpers.export.backends.postgresql.components import PostgresqlExportMetadata
from spinta.cli.helpers.export.backends.postgresql.helpers import split_data, generate_file_paths, \
    generate_export_metadata, prepare_changelog, generate_csv_headers, generate_table_data, extract_headers
from spinta.cli.helpers.export.components import CounterManager
from spinta.cli.helpers.export.helpers import create_data_items, prepare_data_without_checks
from spinta.commands.write import prepare_patch, prepare_data_for_write
from spinta.components import Context, Model, UrlParams, Action, DataSubItem
from spinta.types.datatype import Array
from spinta.types.geometry.components import Geometry
from spinta.utils.aiotools import aiter, aenumerate, anext, achain
from spinta.utils.data import take


@commands.export_data.register(Context, Model, PostgreSQL)
async def export_data(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    data: Iterator,
    path: str,
    counter: CounterManager,
    txn: str,
    **kwargs
):
    params = UrlParams()
    params.action = Action.INSERT
    dstream = aiter(create_data_items(model, backend, data))
    dstream = prepare_data_without_checks(context, dstream)
    dstream = prepare_patch(context, dstream)
    dstream = prepare_data_for_write(context, dstream, params)

    data_headers, dstream = await extract_headers(context, model, backend, dstream, txn=txn)

    file_mapping, property_mapping = generate_file_paths(model, pathlib.Path(path))
    base_key = model.model_type()
    changes_key = f'{model.model_type()}.changes'

    async with AsyncExitStack() as stack:
        files = {
            key: await stack.enter_async_context(aiofiles.open(path, 'w+', newline=''))
            for key, path in file_mapping.items()
        }

        base_export_metadata = generate_export_metadata(
            model,
            base_key,
            files[base_key],
            fieldnames=data_headers,
            counter=counter
        )

        changes_export_metadata = generate_export_metadata(
            model,
            changes_key,
            files[changes_key],
            fieldnames=['_id', '_revision', '_txn', '_rid', 'datetime', 'action', 'data'],
            counter=counter
        )

        export_metadata: dict[str, PostgresqlExportMetadata] = {
            base_key: base_export_metadata,
            changes_key: changes_export_metadata,
            **{
                key: generate_export_metadata(
                    model,
                    key,
                    files[key],
                    fieldnames=generate_csv_headers(context, prop.dtype, backend),
                    counter=counter
                ) for key, prop in property_mapping.items() if key not in [base_key, changes_key]
            }
        }

        for meta in export_metadata.values():
            await meta.writeheader()

        async for i, item in aenumerate(dstream):
            created_time = datetime.datetime.now()
            data = commands.before_export(
                context,
                model,
                backend,
                data=item,
                txn=txn,
                created=created_time
            )
            normal_data, table_data = split_data(data)
            change_data = prepare_changelog(item.patch, i + 1, txn, created_time)
            await base_export_metadata.writerow(normal_data, counter)
            await changes_export_metadata.writerow(change_data, counter)

            for table in table_data:
                await export_metadata[table.name].write_table_data(table, normal_data, counter)

            counter.update_total(1)


@commands.before_export.register(Context, Model, PostgreSQL)
def before_export(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    data: DataSubItem,
    txn: str = "",
    created: datetime.datetime = datetime.datetime.now()
):
    patch = take(['_id'], data.patch)
    patch['_revision'] = take('_revision', data.patch, data.saved)
    patch['_txn'] = txn
    patch['_created'] = created
    patch['_updated'] = None
    for prop in take(model.properties).values():
        if not prop.dtype.inherited:
            prop_data = data[prop.name]
            value = commands.before_export(
                context,
                prop.dtype,
                backend,
                data=prop_data,
            )
            patch.update(value)
    return patch


@commands.before_export.register(Context, Geometry, PostgreSQL)
def before_export(
    context: Context,
    dtype: Geometry,
    backend: PostgreSQL,
    *,
    data: DataSubItem
):
    value = data.patch
    if (
        dtype.srid is not None and
        value and
        "SRID" not in value
    ):
        value = f"SRID={dtype.srid};{value}"
    return take({dtype.prop.place: value})


@commands.before_export.register(Context, Array, PostgreSQL)
def before_export(
    context: Context,
    dtype: Array,
    backend: PostgreSQL,
    *,
    data: DataSubItem,
):
    if dtype.prop.list:
        return {}
    else:
        key = dtype.prop.place
        array_data = take({key: data.patch})
        table_data = generate_table_data(dtype, array_data[key])
        return {
            **array_data,
            table_data.name: table_data
        }


@commands.validate_export_output.register(Context, PostgreSQL, str)
def validate_export_output(context: Context, backend: PostgreSQL, output: str):
    commands.validate_export_output(
        context,
        backend,
        pathlib.Path(output)
    )


@commands.validate_export_output.register(Context, PostgreSQL, pathlib.Path)
def validate_export_output(context: Context, backend: PostgreSQL, output: pathlib.Path):
    if not output.exists():
        cli_error(
            f"{str(output)!r} directory does not exist."
        )
