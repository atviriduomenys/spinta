from typing import AsyncIterator

from sqlalchemy import exc
from spinta import commands, exceptions
from spinta.commands import create_exception
from spinta.types.datatype import Denorm
from spinta.utils.data import take
from spinta.components import Context, Model, DataItem, DataSubItem
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.sqlalchemy import utcnow


@commands.insert.register(Context, Model, PostgreSQL)
async def insert(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
):
    transaction = context.get('transaction')
    config = context.get('config')
    connection = transaction.connection
    table = backend.get_table(model)

    # Need to set specific amount of max errors, to prevent memory problems
    max_error_count = config.max_error_count_on_insert
    error_list = []
    savepoint_transaction_start = connection.begin_nested()
    rollback_full = False
    async for data in dstream:
        try:
            savepoint = connection.begin_nested()
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
            savepoint.commit()
        except exc.DatabaseError as error:
            rollback_full = True
            savepoint.rollback()
            exception = create_exception(data, error)
            error_list.append(exception)
            data.error = exception
            if len(error_list) >= max_error_count or stop_on_error:
                yield data
                savepoint_transaction_start.rollback()
                raise exceptions.MultipleErrors(error_list)
        yield data

    if rollback_full:
        savepoint_transaction_start.rollback()
        raise exceptions.MultipleErrors(error_list)
    savepoint_transaction_start.commit()


@commands.update.register(Context, Model, PostgreSQL)
async def update(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: dict,
    stop_on_error: bool = True,
):
    transaction = context.get('transaction')
    config = context.get('config')
    connection = transaction.connection
    table = backend.get_table(model)

    # Need to set specific amount of max errors, to prevent memory problems
    max_error_count = config.max_error_count_on_insert
    error_list = []
    savepoint_transaction_start = connection.begin_nested()
    rollback_full = False
    async for data in dstream:
        if not data.patch:
            yield data
            continue
        try:
            savepoint = connection.begin_nested()
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
            savepoint.commit()
        except exc.DatabaseError as error:
            rollback_full = True
            savepoint.rollback()
            exception = create_exception(data, error)
            error_list.append(exception)
            data.error = exception

            if len(error_list) >= max_error_count:
                yield data
                savepoint_transaction_start.rollback()
                raise exceptions.MultipleErrors(error_list)
        yield data

    if rollback_full:
        savepoint_transaction_start.rollback()
        raise exceptions.MultipleErrors(error_list)
    savepoint_transaction_start.commit()


@commands.delete.register(Context, Model, PostgreSQL)
async def delete(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
):
    transaction = context.get('transaction')
    config = context.get('config')
    connection = transaction.connection
    table = backend.get_table(model)

    # Need to set specific amount of max errors, to prevent memory problems
    max_error_count = config.max_error_count_on_insert
    error_list = []
    savepoint_transaction_start = connection.begin_nested()
    rollback_full = False
    async for data in dstream:
        try:
            savepoint = connection.begin_nested()
            commands.before_write(context, model, backend, data=data)
            connection.execute(
                table.delete().
                where(table.c._id == data.saved['_id'])
            )
            commands.after_write(context, model, backend, data=data)
            savepoint.commit()
        except exc.DatabaseError as error:
            rollback_full = True
            savepoint.rollback()
            exception = create_exception(data, error)
            error_list.append(exception)
            data.error = exception

            if len(error_list) >= max_error_count:
                yield data
                savepoint_transaction_start.rollback()
                raise exceptions.MultipleErrors(error_list)
        yield data

    if rollback_full:
        savepoint_transaction_start.rollback()
        raise exceptions.MultipleErrors(error_list)
    savepoint_transaction_start.commit()


@commands.before_write.register(Context, Model, PostgreSQL)
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
        if not prop.dtype.inherited:
            prop_data = data[prop.name]
            value = commands.before_write(
                context,
                prop.dtype,
                backend,
                data=prop_data,
            )
            patch.update(value)
    return patch


@commands.after_write.register(Context, Model, PostgreSQL)
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
