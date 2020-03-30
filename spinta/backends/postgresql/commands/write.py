from typing import AsyncIterator, List, Union, Tuple

import cgi
import itertools

from starlette.requests import Request

from spinta import commands
from spinta.utils.schema import NA
from spinta.utils.aiotools import aiter
from spinta.utils.data import take
from spinta.renderer import render
from spinta.components import Context, Action, Property, UrlParams
from spinta.types.datatype import DataType, Object, File, Array
from spinta.components import Context, Action, Model, Property, DataItem, DataSubItem
from spinta.commands.write import prepare_patch, simple_response, validate_data
from spinta.backends.components import Backend
from spinta.backends.postgresql.files import DatabaseFile
from spinta.backends.postgresql.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.sqlalchemy import utcnow


def _update_lists_table(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    action: Action,
    pk: str,
    patch: dict,
) -> None:
    transaction = context.get('transaction')
    connection = transaction.connection
    rows = _get_lists_data(model, patch)
    sort_key = lambda x: x[0].place  # noqa
    rows = sorted(rows, key=sort_key)
    for place, rows in itertools.groupby(rows, key=sort_key):
        prop = model.flatprops[place]
        table = backend.get_table(prop, TableType.LIST)
        if action != Action.INSERT:
            connection.execute(table.delete().where(table.c._rid == pk))
        rows = [
            {
                '_txn': transaction.id,
                '_rid': pk,
                **{
                    _get_list_column_name(place, k): v
                    for k, v in row.items()
                }
            }
            for prop, row in rows
        ]
        connection.execute(table.insert(), rows)


def _get_list_column_name(place, name):
    if place == name:
        return place.split('.')[-1]
    else:
        return name[len(place) + 1:]


def _get_lists_data(
    dtype: Union[Model, DataType],
    value: object,
) -> List[dict]:
    data, lists = _separate_lists_and_data(dtype, value)
    if isinstance(dtype, DataType) and data is not NA:
        yield dtype.prop, data
    for prop, vals in lists:
        for v in vals:
            yield from _get_lists_data(prop.dtype, v)


def _separate_lists_and_data(
    dtype: Union[Model, DataType],
    value: object,
) -> Tuple[dict, List[Tuple[Property, list]]]:
    if isinstance(dtype, (Model, Object)):
        data = {}
        lists = []
        for k, v in (value or {}).items():
            prop = dtype.properties[k]
            v, more = _separate_lists_and_data(prop.dtype, v)
            if v is not NA:
                data.update(v)
            if more:
                lists += more
        return data or NA, lists
    elif isinstance(dtype, Array):
        if value:
            return NA, [(dtype.items, value)]
        else:
            return NA, []
    else:
        return {dtype.prop.place: value}, []


@commands.push.register()
async def push(
    context: Context,
    request: Request,
    dtype: File,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    if params.propref:
        return await push[type(context), Request, DataType, type(backend)](
            context, request, dtype, backend,
            action=action,
            params=params,
        )

    prop = dtype.prop

    # XXX: This command should just prepare AsyncIterator[DataItem] and call
    #      push_stream or something like that. Now I think this command does
    #      too much work.

    commands.authorize(context, action, prop)

    data = DataItem(
        prop.model,
        prop,
        propref=False,
        backend=backend,
        action=action
    )

    if action == Action.DELETE:
        data.given = {
            prop.name: {
                '_id': None,
                '_content_type': None,
                '_content': None,
            }
        }
    else:
        data.given = {
            prop.name: {
                '_content_type': request.headers.get('content-type'),
                '_content': await request.body(),
            }
        }
        if 'Content-Disposition' in request.headers:
            _, cdisp = cgi.parse_header(request.headers['Content-Disposition'])
            if 'filename' in cdisp:
                data.given[prop.name]['_id'] = cdisp['filename']

    if 'Revision' in request.headers:
        data.given['_revision'] = request.headers['Revision']

    commands.simple_data_check(context, data, data.prop, data.model.backend)

    data.saved = commands.getone(context, prop, dtype, prop.model.backend, id_=params.pk)

    dstream = aiter([data])
    dstream = validate_data(context, dstream)
    dstream = prepare_patch(context, dstream)

    if action in (Action.UPDATE, Action.PATCH, Action.DELETE):
        dstream = commands.update(context, prop, dtype, prop.model.backend, dstream=dstream)
        dstream = commands.create_changelog_entry(
            context, prop.model, prop.model.backend, dstream=dstream,
        )

    elif action == Action.DELETE:
        dstream = commands.delete(context, prop, dtype, prop.model.backend, dstream=dstream)
        dstream = commands.create_changelog_entry(
            context, prop.model, prop.model.backend, dstream=dstream,
        )

    else:
        raise Exception(f"Unknown action {action!r}.")

    status_code, response = await simple_response(context, dstream)
    return render(context, request, prop, params, response, status_code=status_code)


@commands.insert.register()
async def insert(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.get_table(model)
    async for data in dstream:
        patch = commands.before_write(context, model, backend, data=data)

        # TODO: Refactor this to insert batches with single query.
        qry = table.insert().values(
            _id=patch['_id'],
            _revision=patch['_revision'],
            _txn=transaction.id,
            _created=utcnow(),
        )
        connection.execute(qry, patch)

        commands.after_write(context, model, backend, data=data)

        yield data


@commands.update.register()
async def update(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: dict,
    stop_on_error: bool = True,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.get_table(model)

    async for data in dstream:
        if not data.patch:
            yield data
            continue

        pk = data.saved['_id']
        patch = commands.before_write(context, model, backend, data=data)
        result = connection.execute(
            table.update().
            where(table.c._id == pk).
            where(table.c._revision == data.saved['_revision']).
            values(patch)
        )

        if result.rowcount == 0:
            raise Exception(f"Update failed, {model} with {pk} not found.")
        elif result.rowcount > 1:
            raise Exception(
                f"Update failed, {model} with {pk} has found and update "
                f"{result.rowcount} rows."
            )

        commands.after_write(context, model, backend, data=data)

        yield data


@commands.delete.register()
async def delete(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.get_table(model)
    async for data in dstream:
        commands.before_write(context, model, backend, data=data)
        connection.execute(
            table.delete().
            where(table.c._id == data.saved['_id'])
        )
        commands.after_write(context, model, backend, data=data)
        yield data


@commands.before_write.register()
def before_write(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    data: DataSubItem,
) -> dict:
    patch = take(['_id'], data.patch)
    patch['_revision'] = take('_revision', data.patch, data.saved)
    patch['_txn'] = context.get('transaction').id
    patch['_created'] = utcnow()
    for prop in take(model.properties).values():
        value = commands.before_write(
            context,
            prop.dtype,
            backend,
            data=data[prop.name],
        )
        patch.update(value)
    return patch


@commands.after_write.register()
def after_write(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    data: DataSubItem,
) -> dict:
    for key in take(data.patch or {}):
        prop = model.properties[key]
        commands.after_write(context, prop.dtype, backend, data=data[key])


@commands.before_write.register()
def before_write(  # noqa
    context: Context,
    dtype: Array,
    backend: PostgreSQL,
    *,
    data: DataSubItem,
):
    if data.saved and data.patch is not NA:
        prop = dtype.prop
        table = backend.get_table(prop, TableType.LIST)
        transaction = context.get('transaction')
        connection = transaction.connection
        connection.execute(
            table.delete().
            where(table.c._rid == data.root.saved['_id'])
        )

    if dtype.prop.list:
        return {}
    else:
        return take({dtype.prop.place: data.patch})


@commands.after_write.register()
def after_write(  # noqa
    context: Context,
    dtype: Array,
    backend: PostgreSQL,
    *,
    data: DataSubItem,
):
    if data.patch:
        prop = dtype.prop
        table = backend.get_table(prop, TableType.LIST)
        transaction = context.get('transaction')
        connection = transaction.connection
        rid = take('_id', data.root.patch, data.root.saved)
        rows = [
            {
                _get_list_column_name(prop.place, k): v
                for k, v in commands.before_write(
                    context,
                    dtype.items.dtype,
                    backend,
                    data=d,
                ).items()
            }
            for d in data.iter(patch=True)
        ]
        qry = table.insert().values({
            '_txn': transaction.id,
            '_rid': rid,
        })
        connection.execute(qry, rows)

        for d in data.iter(patch=True):
            commands.after_write(context, dtype.items.dtype, backend, data=d)


@commands.before_write.register()
def before_write(  # noqa
    context: Context,
    dtype: File,
    backend: PostgreSQL,
    *,
    data: DataSubItem,
):
    content = take('_content', data.patch)
    if isinstance(content, bytes):
        transaction = context.get('transaction')
        connection = transaction.connection
        prop = dtype.prop
        table = backend.get_table(prop, TableType.FILE)
        with DatabaseFile(connection, table, mode='w') as f:
            f.write(data.patch['_content'])
            data.patch['_size'] = f.size
            data.patch['_blocks'] = f.blocks
            data.patch['_bsize'] = f.bsize

    return commands.before_write[type(context), File, Backend](
        context,
        dtype,
        backend,
        data=data,
    )
